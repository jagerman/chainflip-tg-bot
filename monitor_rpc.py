#!/usr/bin/env python3
# Copyright (C) 2026 Jason Rhinelander
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

"""
Chainflip RPC endpoint monitor.

Periodically polls block heights on a set of internal/external RPC endpoints
for btc/eth/arb/sol/dot/hub/tron, compares against public ground-truth RPCs,
and sends Telegram alerts on state transitions. Send-only — does not poll
Telegram for updates, so it can safely share a bot token with other bots.

Severity model: at each poll we observe max_h (the highest height reported by
any of our endpoints or the ground-truth RPC). For each monitored endpoint we
remember the earliest max_h it has not yet caught up to (target_height) and
when we observed it (target_time). If the endpoint reaches that target the
state clears. Severity scales with `now - target_time` regardless of chain
block time — i.e. "how long until we caught up to a height we observed
elsewhere", not "how many blocks behind".

Usage:
  ./monitor_rpc.py [config_file]
"""

import asyncio
import logging
import sys
import time
import tomllib
from pathlib import Path

import aiohttp
from telegram import Bot
from telegram.constants import ParseMode

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logging.getLogger('httpx').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ── Endpoints ────────────────────────────────────────────────────────────────

# rpc kind per chain: chooses the height-fetcher implementation. Tron uses
# eth_blockNumber on its EVM-compatible jsonrpc endpoint so we hit Chainflip's
# cached path (10s TTL upstream, request de-dup) instead of /wallet/getnowblock.
CHAIN_RPC = {
    'btc':  'btc_rpc',
    'eth':  'evm',
    'arb':  'evm',
    'sol':  'solana',
    'dot':  'substrate',
    'hub':  'substrate',
    'tron': 'evm',
}

# Endpoints to monitor — populated from the [endpoints.<chain>] sections of the
# TOML config at startup. The URLs are deployment-specific (private VPN IPs,
# internal DNS names) so we don't commit them.
ENDPOINTS = {}

# Public ground-truth services per chain. Pre-populated with defaults; users
# may override via [ground_truth] in the TOML. The RPC kind for each chain's
# ground truth is fixed by the URL we're talking to, so we keep the kind
# resolution alongside the URL.
GROUND_TRUTH = {
    'btc':  ('btc_blockstream', 'https://blockstream.info/api/blocks/tip/height'),
    'eth':  ('evm',             'https://ethereum-rpc.publicnode.com'),
    'arb':  ('evm',             'https://arb1.arbitrum.io/rpc'),
    'sol':  ('solana',          'https://api.mainnet-beta.solana.com'),
    'dot':  ('substrate',       'https://rpc.polkadot.io'),
    'hub':  ('substrate',       'https://polkadot-asset-hub-rpc.polkadot.io'),
    'tron': ('evm',             'https://api.trongrid.io/jsonrpc'),
}

# Time-behind thresholds (seconds). Uniform across chains; overridable via TOML.
DEFAULT_THRESHOLDS = {
    'warning':  30,
    'alert':    120,
    'critical': 300,
}

EMOJI = {}
SEVERITY_RANK = {'ok': 0, 'warning': 1, 'alert': 2, 'critical': 3}

def e(s):
    return EMOJI[s]

def severity_for_time(seconds, thresholds):
    if seconds >= thresholds['critical']:
        return 'critical'
    if seconds >= thresholds['alert']:
        return 'alert'
    if seconds >= thresholds['warning']:
        return 'warning'
    return 'ok'

def fmt_secs(seconds):
    if seconds < 60:
        return f'{int(seconds)}s'
    if seconds < 3600:
        return f'{int(seconds // 60)}m{int(seconds % 60):02d}s'
    return f'{int(seconds // 3600)}h{int((seconds % 3600) // 60):02d}m'

def fmt_error(ex):
    """Render an exception as a concise one-line summary.
    aiohttp's str() already includes the underlying cause (DNS error, refused
    connection, etc.) for connection errors, so we mostly just use it directly."""
    if isinstance(ex, aiohttp.ClientResponseError):
        return f'HTTP {ex.status} {ex.message}'
    msg = str(ex).splitlines()[0].strip() if str(ex) else ''
    return f'{type(ex).__name__}: {msg}' if msg else type(ex).__name__

# ── RPC height fetchers ──────────────────────────────────────────────────────

async def _post_json(client, url, payload):
    async with client.post(url, json=payload) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)

async def fetch_height(client, kind, url):
    """Return the current block/slot height for the given endpoint, or raise."""
    if kind == 'btc_rpc':
        data = await _post_json(client, url, {'jsonrpc': '1.0', 'id': 1,
                                              'method': 'getblockcount', 'params': []})
        return int(data['result'])

    if kind == 'btc_blockstream':
        async with client.get(url) as resp:
            resp.raise_for_status()
            text = await resp.text()
        return int(text.strip())

    if kind == 'evm':
        data = await _post_json(client, url, {'jsonrpc': '2.0', 'id': 1,
                                              'method': 'eth_blockNumber', 'params': []})
        return int(data['result'], 16)

    if kind == 'solana':
        data = await _post_json(client, url, {'jsonrpc': '2.0', 'id': 1,
                                              'method': 'getSlot', 'params': []})
        return int(data['result'])

    if kind == 'substrate':
        data = await _post_json(client, url, {'jsonrpc': '2.0', 'id': 1,
                                              'method': 'chain_getHeader', 'params': []})
        return int(data['result']['number'], 16)

    raise ValueError(f'unknown rpc kind: {kind}')

# ── Per-endpoint evaluation ──────────────────────────────────────────────────

def evaluate_endpoint(state, chain, name, result, max_h, now,
                      thresholds, reminder_interval, min_consecutive_failures):
    """Update in-memory state for one endpoint; return alert_msg or None."""
    key = (chain, name)
    s = state.get(key)
    first_seen = s is None
    if first_seen:
        s = {
            'last_height': 0, 'target_height': None, 'target_time': None,
            'severity': 'ok', 'last_reminder': 0,
            'consecutive_failures': 0,
        }
        state[key] = s

    if isinstance(result, Exception):
        s['consecutive_failures'] += 1
        # Tolerate transient request failures (e.g. one-off 502s, brief upstream
        # blips). Only escalate to critical once we've seen `min_consecutive_failures`
        # in a row. The warning log in poll_chain still records every failure.
        if s['consecutive_failures'] < min_consecutive_failures:
            return None
        new_severity = 'critical'
        reason = f'unreachable ({s["consecutive_failures"]} polls) — {fmt_error(result)}'
    else:
        s['consecutive_failures'] = 0
        s['last_height'] = result

        # Clear the target if this endpoint has caught up to it.
        if s['target_height'] is not None and result >= s['target_height']:
            s['target_height'] = None
            s['target_time']   = None

        # If we're now below the chain max and have no target, start one.
        if s['target_height'] is None and max_h is not None and result < max_h:
            s['target_height'] = max_h
            s['target_time']   = now

        if s['target_time'] is not None:
            time_behind  = now - s['target_time']
            new_severity = severity_for_time(time_behind, thresholds)
            reason = (f'height {result}, target {s["target_height"]} '
                      f'({fmt_secs(time_behind)} behind)')
        else:
            new_severity = 'ok'
            reason = f'height {result}'

    prev_severity = s['severity']
    s['severity'] = new_severity

    if new_severity != prev_severity:
        if SEVERITY_RANK[new_severity] > SEVERITY_RANK[prev_severity]:
            msg = f'{e(new_severity)} <b>{chain}/{name}</b> {new_severity.upper()}: {reason}'
        else:
            msg = f'{e(new_severity)} <b>{chain}/{name}</b> recovered: {reason}'
        if new_severity != 'ok':
            s['last_reminder'] = now
        return msg
    if new_severity == 'critical' and (now - s['last_reminder']) >= reminder_interval:
        s['last_reminder'] = now
        return f'{e("critical")} <b>{chain}/{name}</b> still CRITICAL (reminder): {reason}'
    return None

# ── Poll loop ────────────────────────────────────────────────────────────────

async def poll_chain(client, state, chain, thresholds, reminder_interval, min_consecutive_failures):
    """Poll one chain's endpoints and ground truth. Returns list of alert strings."""
    now = time.time()
    endpoints = ENDPOINTS[chain]
    kind = CHAIN_RPC[chain]
    gt_kind, gt_url = GROUND_TRUTH[chain]

    tasks = {name: asyncio.create_task(fetch_height(client, kind, url))
             for name, url in endpoints.items()}
    tasks['__gt__'] = asyncio.create_task(fetch_height(client, gt_kind, gt_url))

    results = {}
    for name, task in tasks.items():
        try:
            results[name] = await task
        except Exception as ex:
            results[name] = ex
            label = 'ground_truth' if name == '__gt__' else name
            log.warning(f'{chain}/{label}: {fmt_error(ex)}')

    heights = [v for v in results.values() if isinstance(v, int)]
    max_h = max(heights) if heights else None
    if max_h is None:
        log.warning(f'{chain}: every endpoint and ground truth failed')

    alerts = []
    for name in endpoints:
        msg = evaluate_endpoint(
            state, chain, name, results[name], max_h, now, thresholds, reminder_interval,
            min_consecutive_failures,
        )
        if msg:
            alerts.append(msg)
    return alerts

async def chain_loop(chain, interval, client, state, thresholds, reminder_interval,
                    min_consecutive_failures, send):
    """Per-chain forever-loop: poll → send alerts → sleep."""
    log.info(f'{chain}: polling every {interval}s')
    while True:
        try:
            alerts = await poll_chain(client, state, chain, thresholds, reminder_interval,
                                       min_consecutive_failures)
        except Exception as ex:
            log.error(f'{chain}: poll error: {ex}', exc_info=True)
            alerts = []
        for msg in alerts:
            await send(msg)
        await asyncio.sleep(interval)

# ── Telegram sender ──────────────────────────────────────────────────────────

async def sender_task(queue, bot, chat_id):
    """Drain the alert queue, sending each to Telegram with retry+backoff.

    Outbound network blips on this host shouldn't lose alerts; queued messages
    will sit here until the connection comes back. Gives up after ~7 minutes
    of failed attempts per message so a persistent failure can't pile up
    forever, but that's far longer than any normal blip.
    """
    while True:
        msg = await queue.get()
        delay = 2
        for attempt in range(1, 11):
            try:
                await bot.send_message(
                    chat_id=chat_id, text=msg,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                )
                break
            except Exception as ex:
                if attempt == 10:
                    log.error(f'Telegram send failed, giving up after {attempt} attempts: {ex}')
                    break
                log.warning(f'Telegram send failed (attempt {attempt}), retrying in {delay}s: {ex}')
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

# ── Main ─────────────────────────────────────────────────────────────────────

def load_config(config_path):
    """Load the TOML config. Populates the module-level ENDPOINTS/GROUND_TRUTH
    dicts (callers can then import them) and returns the parsed config dict."""
    with open(config_path, 'rb') as f:
        config = tomllib.load(f)

    ENDPOINTS.clear()
    for chain, urls in config.get('endpoints', {}).items():
        if chain not in CHAIN_RPC:
            log.warning(f'config: ignoring unknown chain {chain!r}')
            continue
        ENDPOINTS[chain] = dict(urls)

    for chain, url in config.get('ground_truth', {}).items():
        if chain not in GROUND_TRUTH:
            log.warning(f'config: ignoring ground_truth for unknown chain {chain!r}')
            continue
        existing_kind, _ = GROUND_TRUTH[chain]
        GROUND_TRUTH[chain] = (existing_kind, url)

    return config

async def main_async(config_path):
    config = load_config(config_path)

    if not ENDPOINTS:
        print('Error: no [endpoints.<chain>] sections in config. Nothing to monitor.',
              file=sys.stderr)
        sys.exit(1)

    bot_token         = config['telegram']['bot_token']
    chat_id           = config['telegram']['chat_id']
    default_interval  = config['monitoring'].get('poll_interval_seconds', 60)
    reminder_interval = config['monitoring'].get('reminder_interval_seconds', 3600)
    request_timeout   = config['monitoring'].get('request_timeout_seconds', 10)
    min_consecutive_failures = config['monitoring'].get('min_consecutive_failures', 2)

    intervals = {c: default_interval for c in ENDPOINTS}
    intervals.update(config.get('poll_intervals', {}))

    thresholds = dict(DEFAULT_THRESHOLDS)
    thresholds.update(config.get('thresholds', {}))
    # Sanity: warning <= alert <= critical
    if not (thresholds['warning'] <= thresholds['alert'] <= thresholds['critical']):
        log.warning(f'thresholds out of order: {thresholds}')

    emoji_cfg = config.get('emoji', {})
    EMOJI['ok']       = emoji_cfg.get('ok',       '🟢')
    EMOJI['warning']  = emoji_cfg.get('warning',  '🟡')
    EMOJI['alert']    = emoji_cfg.get('alert',    '🟠')
    EMOJI['critical'] = emoji_cfg.get('critical', '🔴')

    state = {}  # (chain, endpoint) -> {last_height, target_height, target_time, severity, last_reminder}
    bot = Bot(token=bot_token)
    alert_queue: asyncio.Queue[str] = asyncio.Queue()

    async def send(msg):
        # Polling tasks just enqueue — the sender_task below handles delivery
        # with retry+backoff so a transient outbound-network hiccup on this
        # host doesn't silently drop the alert it produced.
        await alert_queue.put(msg)

    timeout = aiohttp.ClientTimeout(total=request_timeout)
    async with aiohttp.ClientSession(timeout=timeout) as client:
        log.info(f'RPC monitor started (thresholds {thresholds}).')
        sender = asyncio.create_task(sender_task(alert_queue, bot, chat_id))
        loops = [
            asyncio.create_task(
                chain_loop(chain, intervals[chain], client, state, thresholds,
                           reminder_interval, min_consecutive_failures, send)
            )
            for chain in ENDPOINTS
        ]
        await asyncio.gather(sender, *loops)

def main():
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_suffix('.toml')
    if not config_path.exists():
        print(f'Error: config file not found: {config_path}', file=sys.stderr)
        sys.exit(1)
    asyncio.run(main_async(config_path))

if __name__ == '__main__':
    main()

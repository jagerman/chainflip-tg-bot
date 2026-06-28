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
and sends Telegram alerts on state transitions. Also exposes a `/status`
command that reports the current state of every monitored endpoint.

Needs its own Telegram bot token (separate from monitor_validators.py) since
both bots poll for updates and Telegram allows only one consumer of
getUpdates per token.

Severity model: at each poll we observe max_h (the highest height reported by
any of our endpoints or the ground-truth RPC) and append it to a per-chain
ring buffer of (time, max_h) checkpoints. For each monitored endpoint we
identify the oldest checkpoint the endpoint has not yet reached — its age is
the "time behind" used to determine severity. The endpoint is considered
fully recovered ("ok") only when every checkpoint older than warning_threshold
has been met, i.e. the endpoint is current with the chain head as observed
warning_threshold seconds ago (not just with the chain head from whenever the
last warning happened to fire).

Usage:
  ./monitor_rpc.py [config_file]
"""

import asyncio
import logging
import sys
import time
import tomllib
from collections import deque
from pathlib import Path

import aiohttp
from telegram import BotCommand, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

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

def evaluate_endpoint(state, chain, name, result, history, now,
                      thresholds, reminder_interval, min_consecutive_failures):
    """Update in-memory state for one endpoint; return (msg, is_recovery) or None.

    `history` is the chain's ring buffer of (time, max_h) checkpoints. The
    endpoint's "time behind" is the age of the oldest checkpoint it hasn't
    reached — so the chain head has to advance forward of the endpoint for
    the *threshold duration* before we trigger, and the endpoint has to
    catch up to a checkpoint at least *threshold* seconds old to recover.

    is_recovery is True when severity transitions downward (toward ok).
    The chain_loop uses this to coalesce simultaneous recoveries into one
    message and append a remaining-issues summary."""
    key = (chain, name)
    s = state.get(key)
    if s is None:
        s = {
            'severity': 'ok', 'last_reminder': 0, 'consecutive_failures': 0,
            'last_height': None, 'last_height_time': None,
            'time_behind': 0, 'target_height': None,
            'last_error': None,
        }
        state[key] = s

    if isinstance(result, Exception):
        s['consecutive_failures'] += 1
        s['last_error'] = fmt_error(result)
        # Tolerate transient request failures (e.g. one-off 502s, brief upstream
        # blips). Only escalate to critical once we've seen `min_consecutive_failures`
        # in a row. The warning log in poll_chain still records every failure.
        if s['consecutive_failures'] < min_consecutive_failures:
            return None
        new_severity = 'critical'
        reason = f'unreachable ({s["consecutive_failures"]} polls) — {s["last_error"]}'
    else:
        s['consecutive_failures'] = 0
        s['last_error'] = None
        s['last_height'] = result
        s['last_height_time'] = now

        # Walk the chain history oldest → newest and find the first checkpoint
        # this endpoint hasn't reached. Its age is our "time behind".
        target_time, target_height = None, None
        for t, mh in history:
            if result < mh:
                target_time, target_height = t, mh
                break

        if target_time is not None:
            time_behind  = now - target_time
            new_severity = severity_for_time(time_behind, thresholds)
            reason = (f'height {result}, target {target_height} '
                      f'from {fmt_secs(time_behind)} ago')
            s['time_behind']   = time_behind
            s['target_height'] = target_height
        else:
            new_severity = 'ok'
            reason = f'height {result}'
            s['time_behind']   = 0
            s['target_height'] = None

    prev_severity = s['severity']
    s['severity'] = new_severity

    if new_severity != prev_severity:
        if SEVERITY_RANK[new_severity] > SEVERITY_RANK[prev_severity]:
            msg = f'{e(new_severity)} <b>{chain}/{name}</b> {new_severity.upper()}: {reason}'
            is_recovery = False
        else:
            msg = f'{e(new_severity)} <b>{chain}/{name}</b> recovered: {reason}'
            is_recovery = True
        if new_severity != 'ok':
            s['last_reminder'] = now
        return msg, is_recovery
    if new_severity == 'critical' and (now - s['last_reminder']) >= reminder_interval:
        s['last_reminder'] = now
        return f'{e("critical")} <b>{chain}/{name}</b> still CRITICAL (reminder): {reason}', False
    return None

# ── Poll loop ────────────────────────────────────────────────────────────────

async def poll_chain(client, state, history, chain, thresholds, reminder_interval, min_consecutive_failures):
    """Poll one chain's endpoints and ground truth. Returns list of alert strings.

    `history` is the per-chain deque of (time, max_h) checkpoints; we append
    this poll's observation and prune anything older than the buffer horizon."""
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
    else:
        history.append((now, max_h))
        # Keep history twice as long as the slowest severity threshold so an
        # endpoint that drifts up into critical and back down still has
        # checkpoints to compare against.
        horizon = now - 2 * thresholds['critical']
        while history and history[0][0] < horizon:
            history.popleft()

    alerts = []  # list of (msg, is_recovery)
    for name in endpoints:
        result = evaluate_endpoint(
            state, chain, name, results[name], history, now, thresholds, reminder_interval,
            min_consecutive_failures,
        )
        if result is not None:
            alerts.append(result)
    return alerts

def _remaining_issues(state):
    """Sorted list of 'chain/name' for every endpoint not currently in ok severity."""
    return sorted(
        f'{chain}/{name}'
        for (chain, name), s in state.items()
        if s.get('severity') and s['severity'] != 'ok'
    )

async def chain_loop(chain, interval, client, state, thresholds, reminder_interval,
                    min_consecutive_failures, send):
    """Per-chain forever-loop: poll → send alerts → sleep.

    Recoveries from the same poll are coalesced into a single message with a
    remaining-issues tail (count + names, or 'all clear'). Escalations and
    reminders go out as separate messages."""
    log.info(f'{chain}: polling every {interval}s')
    history = deque()
    while True:
        try:
            alerts = await poll_chain(client, state, history, chain, thresholds, reminder_interval,
                                       min_consecutive_failures)
        except Exception as ex:
            log.error(f'{chain}: poll error: {ex}', exc_info=True)
            alerts = []

        recoveries = [m for m, is_rec in alerts if is_rec]
        others     = [m for m, is_rec in alerts if not is_rec]

        if recoveries:
            remaining = _remaining_issues(state)
            body = '\n'.join(recoveries)
            if not remaining:
                tail = '<i>(all clear — every monitored endpoint is healthy)</i>'
            else:
                shown = remaining[:6]
                names = ', '.join(shown)
                if len(remaining) > len(shown):
                    names += f', +{len(remaining) - len(shown)} more'
                tail = f'<i>({len(remaining)} endpoint{"s" if len(remaining) != 1 else ""} still affected: {names})</i>'
            await send(body + '\n\n' + tail)

        for msg in others:
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

# ── Telegram command handlers ────────────────────────────────────────────────

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reply with a snapshot of every monitored endpoint's current state."""
    state = context.bot_data['state']
    now = time.time()

    if not state:
        await update.message.reply_text(
            '⏳ No data yet — bot just started, first polls in progress.',
            parse_mode=ParseMode.HTML,
        )
        return

    # Group by chain in the canonical ENDPOINTS order.
    lines = []
    overall = 'ok'
    for chain in ENDPOINTS:
        chain_entries = [(name, s) for (c, name), s in state.items() if c == chain]
        if not chain_entries:
            continue
        chain_severity = 'ok'
        for _, s in chain_entries:
            sev = s.get('severity') or 'ok'
            if SEVERITY_RANK[sev] > SEVERITY_RANK[chain_severity]:
                chain_severity = sev
        if SEVERITY_RANK[chain_severity] > SEVERITY_RANK[overall]:
            overall = chain_severity
        lines.append(f'{e(chain_severity)} <b>{chain}</b>')

        # Endpoints in ENDPOINTS' configured order
        for name in ENDPOINTS[chain]:
            s = state.get((chain, name))
            if not s:
                lines.append(f'    ⚪ {name}: <i>not yet polled</i>')
                continue
            sev = s.get('severity') or 'ok'
            h = s.get('last_height')
            age = now - s['last_height_time'] if s.get('last_height_time') else None
            if s.get('last_error') and sev != 'ok':
                lines.append(f'    {e(sev)} {name}: <i>{s["last_error"]}</i>')
            elif sev == 'ok':
                tail = f' ({fmt_secs(age)} ago)' if age and age > 5 else ''
                lines.append(f'    {e(sev)} {name}: {h}{tail}')
            else:
                tb = s.get('time_behind') or 0
                tgt = s.get('target_height')
                tgt_str = f', target {tgt}' if tgt else ''
                lines.append(f'    {e(sev)} {name}: {h}{tgt_str} ({fmt_secs(tb)} behind)')

    body = '\n'.join(lines) if lines else '<i>(no endpoints configured)</i>'
    head = f'{e(overall)} <b>Chainflip RPC Status</b> — {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(now))}'
    await update.message.reply_text(
        head + '\n\n' + body,
        parse_mode=ParseMode.HTML, disable_web_page_preview=True,
    )

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_suffix('.toml')
    if not config_path.exists():
        print(f'Error: config file not found: {config_path}', file=sys.stderr)
        sys.exit(1)

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
    if not (thresholds['warning'] <= thresholds['alert'] <= thresholds['critical']):
        log.warning(f'thresholds out of order: {thresholds}')

    emoji_cfg = config.get('emoji', {})
    EMOJI['ok']       = emoji_cfg.get('ok',       '🟢')
    EMOJI['warning']  = emoji_cfg.get('warning',  '🟡')
    EMOJI['alert']    = emoji_cfg.get('alert',    '🟠')
    EMOJI['critical'] = emoji_cfg.get('critical', '🔴')

    state = {}  # (chain, endpoint) -> per-endpoint dict (see evaluate_endpoint)

    app = Application.builder().token(bot_token).build()
    app.bot_data['state']   = state
    app.bot_data['chat_id'] = chat_id

    app.add_handler(CommandHandler('status', cmd_status))

    async def post_init(app):
        await app.bot.set_my_commands([
            BotCommand('status', 'Show current status of all monitored RPC endpoints'),
        ])

        timeout = aiohttp.ClientTimeout(total=request_timeout)
        client = aiohttp.ClientSession(timeout=timeout)
        alert_queue: asyncio.Queue[str] = asyncio.Queue()

        async def send(msg):
            # Polling tasks just enqueue — sender_task below handles delivery
            # with retry+backoff so a transient outbound network hiccup on
            # this host doesn't silently drop the alert.
            await alert_queue.put(msg)

        app.bot_data['client'] = client
        app.bot_data['monitor_tasks'] = [
            asyncio.create_task(sender_task(alert_queue, app.bot, chat_id))
        ] + [
            asyncio.create_task(
                chain_loop(chain, intervals[chain], client, state, thresholds,
                           reminder_interval, min_consecutive_failures, send)
            )
            for chain in ENDPOINTS
        ]
        log.info(f'RPC monitor started (thresholds {thresholds}).')

    async def post_shutdown(app):
        for task in app.bot_data.get('monitor_tasks', []):
            task.cancel()
        client = app.bot_data.get('client')
        if client is not None:
            await client.close()

    app.post_init = post_init
    app.post_shutdown = post_shutdown

    app.run_polling()

if __name__ == '__main__':
    main()

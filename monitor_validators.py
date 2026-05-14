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
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Chainflip validator monitoring bot.

Usage:
  ./monitor_validators.py [config_file]

Config file defaults to monitor_validators.toml in the same directory.

Telegram commands:
  /start                  - Welcome message
  /register <operator>    - Add an operator to your watch list
  /unregister             - Remove an operator from your watch list
  /status                 - Show current status of all monitored operators
"""

import asyncio
import logging
import sqlite3
import sys
import time
import tomllib
from pathlib import Path
from eth_utils import is_address, to_checksum_address
from substrateinterface import SubstrateInterface
from scalecodec.utils.ss58 import ss58_encode
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters
from telegram.constants import ParseMode

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

HEARTBEAT_BLOCK_INTERVAL = 150

# Reputation thresholds
REP_WARNING  = 2500
REP_ALERT    = 1000
REP_CRITICAL = 0

# Consecutive-drop requirements per level
DROPS_WARNING  = 4
DROPS_ALERT    = 2
DROPS_CRITICAL = 1

SCAN_VALIDATOR = 'https://scan.chainflip.io/validators'
SCAN_OPERATOR  = 'https://scan.chainflip.io/operators'

# Chainflip SS58 prefix; ETH-derived accounts are 12 zero bytes + 20-byte address.
CHAINFLIP_SS58_PREFIX = 2112

# Severity levels in ascending order — populated from config at startup
EMOJI = {}  # ok / warning / alert / critical -> str

SEVERITY_RANK = {'ok': 0, 'warning': 1, 'alert': 2, 'critical': 3}

def worst(*severities):
    return max(severities, key=lambda s: SEVERITY_RANK[s])

def e(severity):
    """Return the emoji for a severity level."""
    return EMOJI[severity]

def relative_time(timestamp):
    """Format a unix timestamp as a relative time string."""
    diff = int(time.time() - timestamp)
    if diff < 0:
        return 'just now'
    if diff < 60:
        return f'{diff}s ago'
    if diff < 3600:
        return f'{diff // 60}m {diff % 60}s ago'
    if diff < 86400:
        return f'{diff // 3600}h {(diff % 3600) // 60}m ago'
    return f'{diff // 86400}d {(diff % 86400) // 3600}h ago'

def short_addr(addr):
    """Abbreviate a Chainflip address to a consistent short form."""
    return f'{addr[:6]}…{addr[-4:]}'

def parse_eth_addr(s):
    """Parse a 0x… ETH address string into 20 raw bytes; return None if invalid."""
    s = s.strip()
    if not is_address(s):
        return None
    return bytes.fromhex(s[2:].lower())

def eth_to_ss58(raw):
    """Convert 20 raw ETH address bytes to the on-chain Chainflip SS58 (cF…) form."""
    return ss58_encode(b'\x00' * 12 + raw, ss58_format=CHAINFLIP_SS58_PREFIX)

def wallet_display(raw):
    """EIP-55 checksummed 0x… display form for 20 raw bytes."""
    return to_checksum_address(raw)

def wallet_short(raw):
    c = wallet_display(raw)
    return f'{c[:6]}…{c[-4:]}'

def op_link(operator, vanity_map=None):
    """Return operator display with a clickable abbreviated address link."""
    name = (vanity_map or {}).get(operator)
    short = short_addr(operator)
    link = f'<a href="{SCAN_OPERATOR}/{operator}">{short}</a>'
    if name:
        return f'<b>{name}</b> ({link})'
    return link

# ── Database ─────────────────────────────────────────────────────────────────

def db_connect(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            chat_id        INTEGER,
            operator       TEXT,
            last_severity  TEXT DEFAULT NULL,
            PRIMARY KEY (chat_id, operator)
        );

        CREATE TABLE IF NOT EXISTS validator_state (
            chat_id           INTEGER,
            operator          TEXT,
            validator         TEXT,
            reputation        INTEGER  DEFAULT 0,
            consecutive_drops INTEGER  DEFAULT 0,
            alert_offline     INTEGER  DEFAULT 0,
            alert_warning     INTEGER  DEFAULT 0,
            alert_alert       INTEGER  DEFAULT 0,
            alert_critical    INTEGER  DEFAULT 0,
            last_reminder     REAL     DEFAULT 0,
            PRIMARY KEY (chat_id, operator, validator)
        );

        CREATE TABLE IF NOT EXISTS operator_state (
            chat_id           INTEGER,
            operator          TEXT,
            alert_alert_agg    INTEGER DEFAULT 0,
            alert_critical_agg INTEGER DEFAULT 0,
            last_reminder      REAL    DEFAULT 0,
            PRIMARY KEY (chat_id, operator)
        );

        CREATE TABLE IF NOT EXISTS wallets (
            chat_id  INTEGER,
            wallet   BLOB,     -- 20 raw bytes of ETH address
            label    TEXT,
            PRIMARY KEY (chat_id, wallet)
        );
    ''')
    conn.commit()
    return conn

# ── Chain queries ─────────────────────────────────────────────────────────────

def fetch_chain_data(api):
    current_block = api.get_block_number(api.get_chain_head())
    block_timestamp = api.query('Timestamp', 'Now').value / 1000  # ms -> seconds

    vanity_map = dict(api.query('AccountRoles', 'VanityNames').value or [])

    all_reputations = {
        str(k): (v.value or {}).get('reputation_points', 0)
        for k, v in api.query_map('Reputation', 'Reputations')
    }
    all_heartbeats = {
        str(k): v.value
        for k, v in api.query_map('Reputation', 'LastHeartbeat')
    }

    authorities = set(api.query('Validator', 'CurrentAuthorities').value or [])
    active_bidders = set(api.query('Validator', 'ActiveBidder').value or [])

    return current_block, block_timestamp, vanity_map, all_reputations, all_heartbeats, authorities, active_bidders

FLIP_DECIMALS = 10**18

def format_flip(amount):
    """Format a raw FLIP amount with 3 significant digits and k/M suffix."""
    val = amount / FLIP_DECIMALS
    if val >= 1_000_000:
        v, suffix = val / 1_000_000, 'M'
    elif val >= 1_000:
        v, suffix = val / 1_000, 'k'
    else:
        v, suffix = val, ''
    if v >= 100:
        return f'{v:.0f}{suffix}'
    if v >= 10:
        return f'{v:.1f}{suffix}'
    if v >= 1:
        return f'{v:.2f}{suffix}'
    return f'{v:.3f}{suffix}'

def get_operator_financials(api, operator, validators):
    """Fetch balance, total validator stake, and delegation info for an operator."""
    # Operator balance
    op_bal = api.query('Flip', 'Account', [operator]).value
    operator_balance = op_bal.get('balance', 0)

    # Total stake across managed validators
    total_stake = 0
    for val in validators:
        vbal = api.query('Flip', 'Account', [val]).value
        total_stake += vbal.get('bond', 0)

    # Delegations into this operator
    num_delegators = 0
    total_delegation = 0
    for _, v in api.query_map('Validator', 'DelegationChoice'):
        del_operator, max_bid = v.value
        if del_operator == operator:
            num_delegators += 1
            total_delegation += max_bid

    return {
        'operator_balance': operator_balance,
        'total_stake': total_stake,
        'num_delegators': num_delegators,
        'total_delegation': total_delegation,
    }

def get_managed_validators(api, operator):
    result = api.query('Validator', 'ManagedValidators', [operator])
    return list(result.value or [])

def get_wallet_delegations(api, wallets, operators):
    """
    For each wallet (20-byte raw), look up its delegation choice and on-chain balance.
    Returns {operator_ss58: [(wallet_raw, label, current, upcoming, reward), ...]} for
    only those wallets whose chosen operator is in `operators`.
    """
    if not wallets or not operators:
        return {}

    current_epoch = api.query('Validator', 'CurrentEpoch').value
    operators = set(operators)

    snapshots = {}  # operator -> {delegator_ss58: amount}
    out = {op: [] for op in operators}

    for raw, label in wallets:
        ss58 = eth_to_ss58(raw)
        choice = api.query('Validator', 'DelegationChoice', [ss58]).value
        if not choice:
            continue
        chosen_op, max_bid = choice
        if chosen_op not in operators:
            continue

        if chosen_op not in snapshots:
            snap = api.query('Validator', 'DelegationSnapshots', [current_epoch, chosen_op]).value
            snapshots[chosen_op] = dict(snap['delegators']) if snap else {}

        current = snapshots[chosen_op].get(ss58, 0)
        acc = api.query('Flip', 'Account', [ss58]).value or {}
        balance = acc.get('balance', 0)
        bond    = acc.get('bond', 0)
        reward  = max(balance - bond, 0)

        out[chosen_op].append((raw, label, current, max_bid, reward))

    return out

def is_online(validator, current_block, all_heartbeats):
    last_hb = all_heartbeats.get(validator)
    if last_hb is None:
        return False
    return (current_block - last_hb) < HEARTBEAT_BLOCK_INTERVAL

# ── Alert logic ───────────────────────────────────────────────────────────────

def compute_validator_severity(state_row):
    """Determine severity for a validator from its stored alert state."""
    if state_row['alert_critical'] or state_row['alert_offline']:
        return 'critical'
    if state_row['alert_alert']:
        return 'alert'
    if state_row['alert_warning']:
        return 'warning'
    return 'ok'

def process_validator(conn, chat_id, operator, validator, online, reputation, now, reminder_interval):
    """
    Update state for one validator, returning a list of (severity, message) alert tuples.
    """
    row = conn.execute(
        'SELECT * FROM validator_state WHERE chat_id=? AND operator=? AND validator=?',
        (chat_id, operator, validator)
    ).fetchone()

    if row is None:
        conn.execute(
            'INSERT INTO validator_state (chat_id, operator, validator, reputation) VALUES (?,?,?,?)',
            (chat_id, operator, validator, reputation)
        )
        conn.commit()
        return []  # No history yet — skip alerting on first poll

    prev_rep      = row['reputation']
    cons_drops    = row['consecutive_drops']
    was_offline   = bool(row['alert_offline'])
    was_warning   = bool(row['alert_warning'])
    was_alert     = bool(row['alert_alert'])
    was_critical  = bool(row['alert_critical'])
    last_reminder = row['last_reminder']

    alerts = []

    # ── Consecutive drops ───────────────────────────────────────────────────
    if reputation < prev_rep:
        cons_drops += 1
    elif reputation > prev_rep:
        cons_drops = 0
    # If equal, keep cons_drops unchanged

    # ── Compute new alert states ────────────────────────────────────────────

    new_offline = not online

    # Reputation alerts: triggered by consecutive drops + threshold.
    # Stay triggered until rep climbs back above the threshold.
    new_warning = (
        (was_warning  and reputation < REP_WARNING) or
        (cons_drops >= DROPS_WARNING  and reputation < REP_WARNING)
    )
    new_alert = (
        (was_alert    and reputation < REP_ALERT) or
        (cons_drops >= DROPS_ALERT    and reputation < REP_ALERT)
    )
    new_critical = (
        (was_critical and reputation < REP_CRITICAL) or
        (cons_drops >= DROPS_CRITICAL and reputation < REP_CRITICAL)
    )

    # ── Generate transition messages ────────────────────────────────────────

    if new_offline and not was_offline:
        alerts.append(('critical', f'{e("critical")} <b>OFFLINE</b>'))
    elif not new_offline and was_offline:
        alerts.append(('ok', f'{e("ok")} Back <b>ONLINE</b>'))

    if new_critical and not was_critical:
        alerts.append(('critical', f'{e("critical")} Reputation CRITICAL: {reputation} (dropping below {REP_CRITICAL})'))
    elif not new_critical and was_critical:
        alerts.append(('ok', f'{e("ok")} Reputation recovered above {REP_CRITICAL}: {reputation}'))

    if new_alert and not was_alert:
        alerts.append(('alert', f'{e("alert")} Reputation ALERT: {reputation} (dropping below {REP_ALERT})'))
    elif not new_alert and was_alert:
        alerts.append(('ok', f'{e("ok")} Reputation recovered above {REP_ALERT}: {reputation}'))

    if new_warning and not was_warning:
        alerts.append(('warning', f'{e("warning")} Reputation WARNING: {reputation} (dropping below {REP_WARNING})'))
    elif not new_warning and was_warning:
        alerts.append(('ok', f'{e("ok")} Reputation recovered above {REP_WARNING}: {reputation}'))

    # ── Reminders for persistent CRITICAL conditions ────────────────────────
    is_critical_condition = new_offline or new_critical
    if is_critical_condition and not alerts and (now - last_reminder) >= reminder_interval:
        if new_offline:
            alerts.append(('critical', f'{e("critical")} Still <b>OFFLINE</b> (reminder)'))
        if new_critical:
            alerts.append(('critical', f'{e("critical")} Reputation still CRITICAL: {reputation} (reminder)'))

    if alerts:
        last_reminder = now

    conn.execute('''
        UPDATE validator_state
        SET reputation=?, consecutive_drops=?,
            alert_offline=?, alert_warning=?, alert_alert=?, alert_critical=?,
            last_reminder=?
        WHERE chat_id=? AND operator=? AND validator=?
    ''', (
        reputation, cons_drops,
        int(new_offline), int(new_warning), int(new_alert), int(new_critical),
        last_reminder,
        chat_id, operator, validator
    ))
    conn.commit()

    return alerts

def process_operator_aggregates(conn, chat_id, operator, validator_reps, now, reminder_interval):
    """
    Check aggregate reputation conditions across all validators for an operator.
    Returns list of (severity, message) tuples.
    """
    total = len(validator_reps)
    if total == 0:
        return []

    below_warning  = sum(1 for r in validator_reps if r < REP_WARNING)
    below_alert    = sum(1 for r in validator_reps if r < REP_ALERT)

    new_alert_agg    = below_warning > total / 2
    new_critical_agg = below_alert   > total / 2

    row = conn.execute(
        'SELECT * FROM operator_state WHERE chat_id=? AND operator=?',
        (chat_id, operator)
    ).fetchone()

    if row is None:
        conn.execute(
            'INSERT INTO operator_state (chat_id, operator, alert_alert_agg, alert_critical_agg) VALUES (?,?,?,?)',
            (chat_id, operator, int(new_alert_agg), int(new_critical_agg))
        )
        conn.commit()
        return []

    was_alert_agg    = bool(row['alert_alert_agg'])
    was_critical_agg = bool(row['alert_critical_agg'])
    last_reminder    = row['last_reminder']

    alerts = []

    if new_critical_agg and not was_critical_agg:
        alerts.append(('critical', f'{e("critical")} <b>Aggregate CRITICAL</b>: {below_alert}/{total} validators below {REP_ALERT}'))
    elif not new_critical_agg and was_critical_agg:
        alerts.append(('ok', f'{e("ok")} Aggregate recovered: fewer than half below {REP_ALERT}'))

    if new_alert_agg and not was_alert_agg:
        alerts.append(('alert', f'{e("alert")} <b>Aggregate ALERT</b>: {below_warning}/{total} validators below {REP_WARNING}'))
    elif not new_alert_agg and was_alert_agg:
        alerts.append(('ok', f'{e("ok")} Aggregate recovered: fewer than half below {REP_WARNING}'))

    # Reminder for persistent critical aggregate
    if new_critical_agg and not alerts and (now - last_reminder) >= reminder_interval:
        alerts.append(('critical', f'{e("critical")} Still aggregate CRITICAL: {below_alert}/{total} below {REP_ALERT} (reminder)'))

    if alerts:
        last_reminder = now

    conn.execute('''
        UPDATE operator_state
        SET alert_alert_agg=?, alert_critical_agg=?, last_reminder=?
        WHERE chat_id=? AND operator=?
    ''', (int(new_alert_agg), int(new_critical_agg), last_reminder, chat_id, operator))
    conn.commit()

    return alerts

# ── Status message builder ────────────────────────────────────────────────────

def format_wallet_line(raw, label, current, upcoming, reward):
    """Format one wallet delegation sub-line."""
    name = label or wallet_short(raw)
    cur_s = format_flip(current)
    parts = [f'{cur_s} FLIP delegated']
    if upcoming != current:
        parts[-1] = f'{cur_s} → {format_flip(upcoming)} FLIP delegated'
    if reward > 0:
        parts.append(f'+{format_flip(reward)} claimable')
    return f'        💼 {name}: {", ".join(parts)}'

def build_status_message_for_user(conn, chat_id, api_data, operator_validators, vanity_map, operator_financials, wallet_delegations=None):
    wallet_delegations = wallet_delegations or {}
    current_block, block_timestamp, _, all_reputations, all_heartbeats, authorities, active_bidders = api_data

    if not operator_validators:
        return 'ok', f'{e("ok")} You have no operators registered. Use /register &lt;operator&gt; to add one.'

    all_operator_severities = []
    sections = []

    for operator, validators in operator_validators.items():
        val_lines = []
        val_severities = []

        for validator in sorted(validators, key=lambda v: vanity_map.get(v, v)):
            name = vanity_map.get(validator, short_addr(validator))
            rep = all_reputations.get(validator, 0)
            online = is_online(validator, current_block, all_heartbeats)

            row = conn.execute(
                'SELECT * FROM validator_state WHERE chat_id=? AND operator=? AND validator=?',
                (chat_id, operator, validator)
            ).fetchone()

            if row:
                severity = compute_validator_severity(row)
            else:
                severity = 'ok' if online else 'critical'

            val_severities.append(severity)
            link = f'<a href="{SCAN_VALIDATOR}/{validator}">{name}</a>'
            role = ''
            if validator not in authorities:
                role = '🌱 ' if validator in active_bidders else '💤 '
            val_lines.append(f'    {e(severity)} {role}{link} (rep: {rep})')

        op_severity = worst(*val_severities) if val_severities else 'ok'

        op_row = conn.execute(
            'SELECT alert_alert_agg, alert_critical_agg FROM operator_state WHERE chat_id=? AND operator=?',
            (chat_id, operator)
        ).fetchone()
        if op_row:
            if op_row['alert_critical_agg']:
                op_severity = worst(op_severity, 'critical')
            elif op_row['alert_alert_agg']:
                op_severity = worst(op_severity, 'alert')

        all_operator_severities.append(op_severity)
        fin = operator_financials.get(operator, {})
        num_auth = sum(1 for v in validators if v in authorities)
        num_bidding = sum(1 for v in validators if v in active_bidders and v not in authorities)
        num_idle = len(validators) - num_auth - num_bidding
        stake_parts = [f'{num_auth} active']
        if num_bidding:
            stake_parts.append(f'{num_bidding} 🌱 bidding')
        if num_idle:
            stake_parts.append(f'{num_idle} 💤')
        fin_lines = [
            f'    💰 Balance: {format_flip(fin.get("operator_balance", 0))} FLIP',
            f'    ⚡ Stake: {format_flip(fin.get("total_stake", 0))} FLIP ({", ".join(stake_parts)})',
            f'    🤝 Delegations: {format_flip(fin.get("total_delegation", 0))} FLIP ({fin.get("num_delegators", 0)} delegators)',
        ]
        for raw, label, current, upcoming, reward in wallet_delegations.get(operator, []):
            fin_lines.append(format_wallet_line(raw, label, current, upcoming, reward))
        sections.append(
            f'{e(op_severity)} {op_link(operator, vanity_map)}\n' +
            '\n'.join(fin_lines) + '\n' +
            '\n'.join(val_lines)
        )

    global_severity = worst(*all_operator_severities) if all_operator_severities else 'ok'
    body = '\n\n'.join(sections)
    msg = f'{e(global_severity)} <b>Chainflip Validator Status</b> (block {current_block}, {relative_time(block_timestamp)})\n\n{body}'
    return global_severity, msg

# ── Monitor loop ──────────────────────────────────────────────────────────────

async def monitor_loop(app, conn, api, poll_interval, reminder_interval):
    log.info('Monitor loop started.')
    try:
        while True:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: run_poll(app, conn, api, reminder_interval)
                )
            except Exception as e:
                log.error(f'Monitor poll error: {e}', exc_info=True)
            await asyncio.sleep(poll_interval)
    except asyncio.CancelledError:
        log.info('Monitor loop stopped.')

def run_poll(app, conn, api, reminder_interval):
    now = time.time()

    subs = conn.execute('SELECT DISTINCT chat_id, operator FROM subscriptions').fetchall()
    if not subs:
        return

    current_block, block_timestamp, vanity_map, all_reputations, all_heartbeats, _, _ = fetch_chain_data(api)

    operator_validators_cache = {}

    for sub in subs:
        chat_id  = sub['chat_id']
        operator = sub['operator']

        if operator not in operator_validators_cache:
            operator_validators_cache[operator] = get_managed_validators(api, operator)
        validators = operator_validators_cache[operator]

        chat_alerts = []  # (severity, validator_name, validator_address, message)
        validator_reps = []

        for validator in validators:
            name = vanity_map.get(validator, short_addr(validator))
            rep = all_reputations.get(validator, 0)
            online = is_online(validator, current_block, all_heartbeats)
            validator_reps.append(rep)

            val_alerts = process_validator(
                conn, chat_id, operator, validator,
                online, rep, now, reminder_interval
            )
            for severity, msg in val_alerts:
                chat_alerts.append((severity, name, validator, msg))

        agg_alerts = process_operator_aggregates(
            conn, chat_id, operator, validator_reps, now, reminder_interval
        )
        for severity, msg in agg_alerts:
            chat_alerts.append((severity, None, None, msg))

        # Determine the actual current severity by examining stored alert state,
        # not just whether any transitions happened this poll.
        current_severity = _current_severity(conn, chat_id, operator)

        if not chat_alerts:
            # Nothing transitioned this poll — only send "All good" if the
            # stored state is actually clear AND we haven't already said so.
            prev_severity = _get_last_severity(conn, chat_id, operator)
            if current_severity == 'ok' and prev_severity != 'ok':
                _set_last_severity(conn, chat_id, operator, 'ok')
                msg = f'{e("ok")} <b>All good</b> — {op_link(operator, vanity_map)}: all validators healthy (block {current_block}, {relative_time(block_timestamp)})'
                asyncio.run_coroutine_threadsafe(
                    app.bot.send_message(
                        chat_id=chat_id, text=msg,
                        parse_mode=ParseMode.HTML,
                    ),
                    app.bot_data['loop'],
                )
            continue

        global_severity = worst(current_severity, *[s for s, _, _, _ in chat_alerts])
        _set_last_severity(conn, chat_id, operator, global_severity)

        lines = [f'{e(global_severity)} <b>Chainflip Alert</b> — {op_link(operator, vanity_map)} (block {current_block}, {relative_time(block_timestamp)})\n']

        for severity, name, validator, msg in chat_alerts:
            if name and validator:
                link = f'<a href="{SCAN_VALIDATOR}/{validator}">{name}</a>'
                lines.append(f'{link}: {msg}')
            else:
                lines.append(msg)

        asyncio.run_coroutine_threadsafe(
            app.bot.send_message(
                chat_id=chat_id,
                text='\n'.join(lines),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            ),
            app.bot_data['loop'],
        )

def _current_severity(conn, chat_id, operator):
    """Determine the current severity from stored alert state (not transitions)."""
    severity = 'ok'
    val_rows = conn.execute(
        '''SELECT alert_offline, alert_warning, alert_alert, alert_critical
           FROM validator_state WHERE chat_id=? AND operator=?''',
        (chat_id, operator)
    ).fetchall()
    for row in val_rows:
        if row['alert_offline'] or row['alert_critical']:
            return 'critical'
        if row['alert_alert']:
            severity = worst(severity, 'alert')
        elif row['alert_warning']:
            severity = worst(severity, 'warning')

    op_row = conn.execute(
        'SELECT alert_alert_agg, alert_critical_agg FROM operator_state WHERE chat_id=? AND operator=?',
        (chat_id, operator)
    ).fetchone()
    if op_row:
        if op_row['alert_critical_agg']:
            return 'critical'
        if op_row['alert_alert_agg']:
            severity = worst(severity, 'alert')

    return severity

def _get_last_severity(conn, chat_id, operator):
    row = conn.execute(
        'SELECT last_severity FROM subscriptions WHERE chat_id=? AND operator=?',
        (chat_id, operator)
    ).fetchone()
    return row['last_severity'] if row else 'ok'

def _set_last_severity(conn, chat_id, operator, severity):
    conn.execute(
        'UPDATE subscriptions SET last_severity=? WHERE chat_id=? AND operator=?',
        (severity, chat_id, operator)
    )
    conn.commit()

# ── Telegram command handlers ─────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        '👋 <b>Chainflip Validator Monitor</b>\n\n'
        'Commands:\n'
        '/register &lt;operator_address&gt; — monitor an operator\n'
        '/unregister — stop monitoring an operator\n'
        '/wallet &lt;0x… address&gt; [label] — register a delegator wallet to track\n'
        '/unwallet — remove a registered wallet\n'
        '/status — show current validator status',
        parse_mode=ParseMode.HTML,
    )

async def _do_register(message, conn, operator):
    """Shared registration logic."""
    chat_id = message.chat_id
    operator = operator.strip()

    if not (operator.startswith('cF') and len(operator) == 49):
        await message.reply_text('⚠️ That doesn\'t look like a valid Chainflip address (should start with cF and be 49 characters).')
        return

    existing = conn.execute(
        'SELECT 1 FROM subscriptions WHERE chat_id=? AND operator=?', (chat_id, operator)
    ).fetchone()
    if existing:
        await message.reply_text(f'You are already monitoring <code>{operator}</code>.', parse_mode=ParseMode.HTML)
        return

    conn.execute('INSERT INTO subscriptions (chat_id, operator) VALUES (?,?)', (chat_id, operator))
    conn.commit()
    await message.reply_text(
        f'✅ Now monitoring operator <code>{operator}</code>.\nUse /status to see current state.',
        parse_mode=ParseMode.HTML,
    )

async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = context.bot_data['conn']

    if not context.args:
        await update.message.reply_text(
            'Please enter the operator address:',
            reply_markup=ForceReply(input_field_placeholder='cF...'),
        )
        return

    await _do_register(update.message, conn, context.args[0])

async def handle_force_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route a ForceReply response back to the matching prompt's handler."""
    conn = context.bot_data['conn']
    prompt = (update.message.reply_to_message.text or '') if update.message.reply_to_message else ''
    if prompt.startswith('Please enter the wallet address'):
        await _do_wallet_add(update.message, conn, update.message.text)
    elif prompt.startswith('Please enter the operator address'):
        await _do_register(update.message, conn, update.message.text)

async def cmd_unregister(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = context.bot_data['conn']
    chat_id = update.effective_chat.id

    subs = conn.execute(
        'SELECT operator FROM subscriptions WHERE chat_id=?', (chat_id,)
    ).fetchall()

    if not subs:
        await update.message.reply_text('You have no operators registered.')
        return

    api = context.bot_data['api']
    vanity_map = dict(api.query('AccountRoles', 'VanityNames').value or [])

    buttons = []
    for row in subs:
        op = row['operator']
        name = vanity_map.get(op)
        label = f'❌ {name} ({short_addr(op)})' if name else f'❌ {short_addr(op)}'
        buttons.append([InlineKeyboardButton(label, callback_data=f'unreg:{op}')])

    await update.message.reply_text(
        'Select an operator to unregister:',
        reply_markup=InlineKeyboardMarkup(buttons),
    )

async def callback_unregister(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = context.bot_data['conn']
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    operator = query.data.removeprefix('unreg:')

    conn.execute('DELETE FROM subscriptions WHERE chat_id=? AND operator=?', (chat_id, operator))
    conn.execute('DELETE FROM validator_state WHERE chat_id=? AND operator=?', (chat_id, operator))
    conn.execute('DELETE FROM operator_state WHERE chat_id=? AND operator=?', (chat_id, operator))
    conn.commit()

    await query.edit_message_text(f'✅ Unregistered operator <code>{operator}</code>.', parse_mode=ParseMode.HTML)

async def _do_wallet_add(message, conn, text):
    """Shared wallet-registration logic. `text` is the user-supplied '<addr> [label]'."""
    chat_id = message.chat_id
    parts = text.strip().split(maxsplit=1)
    addr_s = parts[0] if parts else ''
    label = parts[1].strip() if len(parts) > 1 else None

    raw = parse_eth_addr(addr_s)
    if raw is None:
        await message.reply_text(
            '⚠️ That doesn\'t look like a valid ETH address (should start with 0x and be 42 chars).'
        )
        return

    existing = conn.execute(
        'SELECT label FROM wallets WHERE chat_id=? AND wallet=?', (chat_id, raw)
    ).fetchone()
    if existing:
        if label and existing['label'] != label:
            conn.execute(
                'UPDATE wallets SET label=? WHERE chat_id=? AND wallet=?',
                (label, chat_id, raw),
            )
            conn.commit()
            await message.reply_text(
                f'✅ Updated label for <code>{wallet_display(raw)}</code> to <b>{label}</b>.',
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.reply_text(
                f'You already have <code>{wallet_display(raw)}</code> registered.',
                parse_mode=ParseMode.HTML,
            )
        return

    conn.execute(
        'INSERT INTO wallets (chat_id, wallet, label) VALUES (?,?,?)',
        (chat_id, raw, label),
    )
    conn.commit()
    name = f' as <b>{label}</b>' if label else ''
    await message.reply_text(
        f'✅ Registered wallet <code>{wallet_display(raw)}</code>{name}.',
        parse_mode=ParseMode.HTML,
    )

async def cmd_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = context.bot_data['conn']
    if not context.args:
        await update.message.reply_text(
            'Please enter the wallet address (optionally followed by a label):',
            reply_markup=ForceReply(input_field_placeholder='0x… [label]'),
        )
        return
    await _do_wallet_add(update.message, conn, ' '.join(context.args))

async def cmd_unwallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = context.bot_data['conn']
    chat_id = update.effective_chat.id

    rows = conn.execute(
        'SELECT wallet, label FROM wallets WHERE chat_id=?', (chat_id,)
    ).fetchall()

    if not rows:
        await update.message.reply_text('You have no wallets registered.')
        return

    buttons = []
    for row in rows:
        raw = bytes(row['wallet'])
        label = row['label']
        display = f'❌ {label} ({wallet_short(raw)})' if label else f'❌ {wallet_short(raw)}'
        buttons.append([InlineKeyboardButton(display, callback_data=f'unwal:{raw.hex()}')])

    await update.message.reply_text(
        'Select a wallet to unregister:',
        reply_markup=InlineKeyboardMarkup(buttons),
    )

async def callback_unwallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = context.bot_data['conn']
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    raw = bytes.fromhex(query.data.removeprefix('unwal:'))

    conn.execute('DELETE FROM wallets WHERE chat_id=? AND wallet=?', (chat_id, raw))
    conn.commit()

    await query.edit_message_text(
        f'✅ Unregistered wallet <code>{wallet_display(raw)}</code>.',
        parse_mode=ParseMode.HTML,
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn    = context.bot_data['conn']
    api     = context.bot_data['api']
    chat_id = update.effective_chat.id

    subs = conn.execute(
        'SELECT operator FROM subscriptions WHERE chat_id=?', (chat_id,)
    ).fetchall()

    if not subs:
        await update.message.reply_text('You have no operators registered. Use /register &lt;operator&gt;.', parse_mode=ParseMode.HTML)
        return

    await update.message.reply_text('⏳ Fetching chain data...')

    try:
        api_data = await asyncio.get_event_loop().run_in_executor(None, fetch_chain_data, api)
        _, _, vanity_map, _, _, _, _ = api_data

        operator_validators = {}
        operator_financials = {}
        for sub in subs:
            op = sub['operator']
            validators = await asyncio.get_event_loop().run_in_executor(
                None, get_managed_validators, api, op
            )
            operator_validators[op] = validators
            operator_financials[op] = await asyncio.get_event_loop().run_in_executor(
                None, get_operator_financials, api, op, validators
            )

        wallets = conn.execute(
            'SELECT wallet, label FROM wallets WHERE chat_id=?', (chat_id,)
        ).fetchall()
        wallet_pairs = [(bytes(w['wallet']), w['label']) for w in wallets]
        wallet_delegations = await asyncio.get_event_loop().run_in_executor(
            None, get_wallet_delegations, api, wallet_pairs, list(operator_validators.keys())
        )

        _, msg = build_status_message_for_user(
            conn, chat_id, api_data, operator_validators, vanity_map, operator_financials,
            wallet_delegations,
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    except Exception as ex:
        log.error(f'Status error: {ex}', exc_info=True)
        await update.message.reply_text(f'⚠️ Error fetching status: {ex}')

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_suffix('.toml')
    if not config_path.exists():
        print(f'Error: config file not found: {config_path}', file=sys.stderr)
        sys.exit(1)

    with open(config_path, 'rb') as f:
        config = tomllib.load(f)

    bot_token         = config['telegram']['bot_token']
    endpoint          = config['chainflip']['node_endpoint']
    poll_interval     = config['monitoring']['poll_interval_seconds']
    reminder_interval = config['monitoring']['reminder_interval_seconds']

    emoji_cfg = config.get('emoji', {})
    EMOJI['ok']       = emoji_cfg.get('ok',       '🟢')
    EMOJI['warning']  = emoji_cfg.get('warning',  '🟡')
    EMOJI['alert']    = emoji_cfg.get('alert',    '🟠')
    EMOJI['critical'] = emoji_cfg.get('critical', '🔴')

    if not bot_token:
        print('Error: telegram.bot_token must be set in config.', file=sys.stderr)
        sys.exit(1)

    db_path = Path(config.get('database', {}).get('path', '')) or config_path.with_suffix('.db')
    conn = db_connect(str(db_path))
    log.info(f'Database: {db_path}')

    # Separate API instances: substrate-interface is not thread-safe, and the
    # monitor loop runs in an executor thread while commands run in the async loop.
    cmd_api     = SubstrateInterface(url=endpoint)
    monitor_api = SubstrateInterface(url=endpoint)
    # Force metadata initialization so queries don't fail on first use
    cmd_api.get_chain_head()
    monitor_api.get_chain_head()
    log.info(f'Connected to node: {endpoint}')

    app = Application.builder().token(bot_token).build()
    app.bot_data['conn'] = conn
    app.bot_data['api']  = cmd_api

    app.add_handler(CommandHandler('start',      cmd_start))
    app.add_handler(CommandHandler('register',   cmd_register))
    app.add_handler(CommandHandler('unregister', cmd_unregister))
    app.add_handler(CommandHandler('wallet',     cmd_wallet))
    app.add_handler(CommandHandler('unwallet',   cmd_unwallet))
    app.add_handler(CommandHandler('status',     cmd_status))
    app.add_handler(MessageHandler(
        filters.REPLY & filters.TEXT & ~filters.COMMAND,
        handle_force_reply,
    ))
    app.add_handler(CallbackQueryHandler(callback_unregister, pattern='^unreg:'))
    app.add_handler(CallbackQueryHandler(callback_unwallet,   pattern='^unwal:'))

    async def post_init(app):
        app.bot_data['loop'] = asyncio.get_event_loop()
        app.bot_data['monitor_task'] = asyncio.create_task(
            monitor_loop(app, conn, monitor_api, poll_interval, reminder_interval)
        )

    async def post_shutdown(app):
        task = app.bot_data.get('monitor_task')
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app.post_init = post_init
    app.post_shutdown = post_shutdown

    log.info('Bot started.')
    app.run_polling()

main()

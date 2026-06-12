# Chainflip Telegram Validator Monitor

## Overview

A Telegram bot that monitors Chainflip validator health for operators. Users register operator pubkeys via Telegram and receive alerts on state transitions (offline, reputation drops, aggregate failures). State is tracked per-user per-operator in SQLite.

## Architecture

Single-file Python bot (`monitor_validators.py`) with two concurrent tasks:
- **Telegram bot**: handles commands via `python-telegram-bot` v22 (async polling)
- **Monitor loop**: polls the Chainflip node every N seconds via `substrate-interface`, computes state transitions, sends alerts

### Key data flow

1. `fetch_chain_data()` queries `Reputation::Reputations`, `Reputation::LastHeartbeat`, and `AccountRoles::VanityNames` from the Chainflip state chain
2. `get_managed_validators()` queries `Validator::ManagedValidators` for each subscribed operator
3. `process_validator()` compares current state against stored state in SQLite, fires alerts on transitions
4. `process_operator_aggregates()` checks if >50% of validators are below reputation thresholds
5. Alerts are sent via Telegram with severity-prefixed emojis

### Severity levels (ascending)

- `ok` — all healthy
- `warning` — reputation dropping, below 2500 for 4+ consecutive polls
- `alert` — reputation dropping, below 1000 for 2+ consecutive polls; or >50% below 2500
- `critical` — node offline; reputation below 0; or >50% below 1000

### SQLite tables

- `subscriptions(chat_id, operator)` — user watch list
- `validator_state(chat_id, operator, validator, reputation, consecutive_drops, alert_offline, alert_warning, alert_alert, alert_critical, last_reminder)` — per-user per-validator tracking
- `operator_state(chat_id, operator, alert_alert_agg, alert_critical_agg, last_reminder)` — per-user per-operator aggregate tracking

### Threading model

The monitor loop runs in an executor thread (via `run_in_executor`) because `substrate-interface` is synchronous. Telegram messages from the monitor thread are dispatched via `asyncio.run_coroutine_threadsafe`.

## Configuration

`monitor_validators.toml` — TOML config loaded at startup. Sections:
- `[chainflip]` — `node_endpoint` (HTTP URL)
- `[telegram]` — `bot_token`
- `[database]` — `path` (optional, defaults to `monitor_validators.db` next to config)
- `[monitoring]` — `poll_interval_seconds`, `reminder_interval_seconds`
- `[emoji]` — `ok`, `warning`, `alert`, `critical` (status indicator characters)

## Chainflip-specific details

- Validator online status is determined by `Reputation::LastHeartbeat` — offline if current_block - last_heartbeat >= 150 (the `HEARTBEAT_BLOCK_INTERVAL`)
- Reputation is an integer from -2880 to 2880 (`Reputation::Reputations` → `reputation_points`)
- Operator → validator mapping is in `Validator::ManagedValidators` (operator → BTreeSet<validator>)
- Vanity names are in `AccountRoles::VanityNames` (StorageValue containing a list of (AccountId, name) tuples)
- Chainflip addresses start with `cF` and are 49 characters (SS58 encoding)
- Current authorities are in `Validator::CurrentAuthorities`, active bidders in `Validator::ActiveBidder`
- A managed validator can be: authority (active), bidding but not authority (waiting), or idle (not bidding)
- Validator balances and bonds are in `Flip::Account` → `{balance, bond}`
- Delegations are in `Validator::DelegationChoice` (delegator → (operator, max_bid)); managed validators do not appear here
- Block timestamps are in `Timestamp::Now` (milliseconds)
- Validator status pages are at `https://scan.chainflip.io/validators/<address>`
- Operator status pages are at `https://scan.chainflip.io/operators/<address>`

## Style

- Python 3.11+ (uses `tomllib`)
- No external frameworks beyond `python-telegram-bot` and `substrate-interface`
- Keep it as a single script — don't split into modules unless it becomes unwieldy
- Use severity level names (`ok`/`warning`/`alert`/`critical`) not colour names in code
- Status emojis must always come from the `EMOJI` dict (configured via TOML), never hardcoded

## RPC monitor (separate bot)

A second monitor lives alongside the validator bot in this repo:

- `monitor_rpc.py` — async daemon that polls block heights on btc/eth/arb/sol/dot/hub/tron via httpx and sends Telegram alerts on state transitions. **Send-only** — never calls `getUpdates`, so it can safely share a bot token with `monitor_validators.py` (Telegram only allows one consumer of polled updates per token, but multiple senders are fine).
- `check_rpc.py` — one-shot diagnostic script that imports `monitor_rpc` and runs a single poll across all chains, printing heights/lag/errors. Useful for verifying endpoint config without running the daemon.
- `monitor_rpc.toml` — config (gitignored). Endpoint URLs live here, not in code — they include private VPN IPs and internal DNS.
- `monitor_rpc.toml.sample` — committed template showing structure with placeholder provider URLs.
- `chainflip-rpc-monitor.service` — systemd unit (paired with `chainflip-tg-bot.service`).

### Severity model

Per-endpoint state in memory (no database — restarts cost at most one threshold window of re-detection):

- `target_height`, `target_time`: the earliest unfulfilled chain-max observation. Cleared when the endpoint reaches `target_height`; a new target is set if endpoint < current `max_h`.
- Severity = `now - target_time` mapped through `[thresholds]` from TOML (defaults 30s/120s/300s for warning/alert/critical).
- Unreachable (any RPC error) is critical immediately.

This is "did we reach height X within T seconds of seeing it elsewhere", not "how many blocks behind" — block-time agnostic so the same thresholds work for sub-second Arbitrum and 10-minute Bitcoin.

### Per-chain polling

Each chain runs its own async loop at its own cadence. Default is `monitoring.poll_interval_seconds` (60s) for every chain; override per-chain via `[poll_intervals]` in TOML.

### RPC kinds and ground truth

Each chain has a fixed `CHAIN_RPC` kind (`btc_rpc`, `evm`, `solana`, `substrate`, `btc_blockstream` for ground truth) that selects which JSON-RPC method to call. Ground-truth URLs are public services (publicnode/blockstream/trongrid/etc.) with sensible defaults in `monitor_rpc.py`; users can override via `[ground_truth]` in TOML.

Tron uses `eth_blockNumber` on its EVM-compat jsonrpc endpoint (not the wallet REST `/wallet/getnowblock`) to hit upstream cached/dedupped paths.

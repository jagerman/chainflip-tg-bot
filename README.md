# Chainflip Telegram Monitors

This repo contains two Telegram bots:

1. **Validator Monitor** (`monitor_validators.py`) — multi-user bot that watches Chainflip operators/validators for health issues (offline, reputation drops, aggregate failures). Users register operator addresses via Telegram commands.
2. **RPC Endpoint Monitor** (`monitor_rpc.py`) — send-only daemon that watches block heights across your btc/eth/arb/sol/dot/hub/tron RPC endpoints (and a public ground-truth source per chain), DM-alerting on lag/unreachability. See [RPC Endpoint Monitor](#rpc-endpoint-monitor-companion-bot) below.

## Validator Monitor

A Telegram bot that monitors Chainflip validator nodes for operators. Users can register operator addresses and receive alerts when validators go offline, reputation drops, or aggregate health degrades.

## Setup

### 1. Create a Telegram bot

Message [@BotFather](https://t.me/BotFather) on Telegram:
- Send `/newbot` and follow the prompts to get a bot token.
- Send `/setcommands`, select your bot, and paste:
  ```
  start - Welcome message and command list
  register - Monitor an operator (provide address)
  unregister - Stop monitoring an operator
  status - Show current validator status
  ```

### 2. Install dependencies

Requires Python 3.11+.

```bash
pip install python-telegram-bot substrate-interface requests
```

### 3. Install the bot

```bash
# Create a service user
sudo useradd -r -s /bin/false -d /var/lib/chainflip-tg-bot chainflip-tg-bot

# Create directories
sudo mkdir -p /opt/chainflip-tg-bot /etc/chainflip-tg-bot /var/lib/chainflip-tg-bot
sudo chown chainflip-tg-bot: /var/lib/chainflip-tg-bot

# Install the script
sudo cp monitor_validators.py /opt/chainflip-tg-bot/

# Install and edit the config
sudo cp monitor_validators.toml.sample /etc/chainflip-tg-bot/monitor_validators.toml
sudo editor /etc/chainflip-tg-bot/monitor_validators.toml
```

In the config, set:
- `telegram.bot_token` — the token from BotFather
- `chainflip.node_endpoint` — HTTP URL of your Chainflip node RPC (default: `http://127.0.0.1:9944`)
- `database.path` — uncomment and set to `/var/lib/chainflip-tg-bot/monitor_validators.db`

### 4. Install and start the service

```bash
sudo cp chainflip-tg-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now chainflip-tg-bot
```

### 5. Check status

```bash
sudo systemctl status chainflip-tg-bot
sudo journalctl -u chainflip-tg-bot -f
```

## Usage

### Running directly (without systemd)

```bash
python3 monitor_validators.py [config_file]
```

Config file defaults to `monitor_validators.toml` in the same directory as the script.

### Telegram Commands

| Command | Description |
|---------|-------------|
| `/start` | Welcome message and command list |
| `/register <operator_address>` | Add an operator to your watch list |
| `/unregister` | Remove an operator (shows clickable buttons) |
| `/status` | Show current status of all monitored operators |

## Alert Conditions

### Per-validator alerts

| Severity | Condition | Clears when |
|----------|-----------|-------------|
| Critical | Node is offline (no heartbeat within 150 blocks) | Node comes back online |
| Critical | Reputation dropping and below 0 | Reputation rises above 0 |
| Alert | Reputation dropped 2+ consecutive polls and below 1000 | Reputation rises above 1000 |
| Warning | Reputation dropped 4+ consecutive polls and below 2500 | Reputation rises above 2500 |

### Aggregate alerts (per-operator)

| Severity | Condition | Clears when |
|----------|-----------|-------------|
| Critical | >50% of validators below 1000 reputation | Fewer than half below 1000 |
| Alert | >50% of validators below 2500 reputation | Fewer than half below 2500 |

### Alert behaviour

- Alerts fire on state transitions (e.g. going offline, crossing a threshold).
- An "all good" message fires once when all conditions clear.
- Critical conditions re-send reminder alerts every 2 hours (configurable).
- Each Telegram user's monitoring state is independent.
- Alert state persists across restarts (stored in SQLite).

## Status display

The `/status` command shows for each operator:
- Operator name with link to [scan.chainflip.io](https://scan.chainflip.io) operator page
- Financial summary: operator balance, total validator stake (with active/bidding/idle counts), and delegation totals
- Per-validator list with severity indicator, link to validator page, and current reputation
- Validators that are bidding but not yet authorities are marked with 🌱
- Idle (non-bidding) validators are marked with 💤

## Files

| File | Purpose |
|------|---------|
| `monitor_validators.py` | Main bot + monitoring loop |
| `monitor_validators.toml.sample` | Sample configuration |
| `chainflip-tg-bot.service` | systemd service file |

## Configuration

See `monitor_validators.toml.sample` for all options:

| Setting | Description | Default |
|---------|-------------|---------|
| `chainflip.node_endpoint` | HTTP URL of the Chainflip node RPC | `http://127.0.0.1:9944` |
| `telegram.bot_token` | Telegram bot token from BotFather | (required) |
| `database.path` | Path to SQLite database | `monitor_validators.db` next to config |
| `monitoring.poll_interval_seconds` | How often to poll the chain | `30` |
| `monitoring.reminder_interval_seconds` | How often to re-alert persistent critical conditions | `7200` |
| `emoji.ok` | Status indicator for healthy | 🟢 |
| `emoji.warning` | Status indicator for warning | 🟡 |
| `emoji.alert` | Status indicator for alert | 🟠 |
| `emoji.critical` | Status indicator for critical | 🔴 |

## RPC Endpoint Monitor (companion bot)

A second, send-only daemon — `monitor_rpc.py` — that polls block heights on btc/eth/arb/sol/dot/hub/tron across multiple RPC endpoints (your own + a public ground-truth source per chain) and alerts when an endpoint falls behind or becomes unreachable. Unlike the validator monitor, it sends to a single hardcoded chat: alerts arrive in your DM, no `/register` flow.

### Setup

It's send-only (never calls `getUpdates`), so it can share the same Telegram bot token as the validator monitor — Telegram only restricts polling consumers per token, not senders. No `/setcommands` needed.

```bash
# Install the scripts and service (after the validator monitor setup above)
sudo cp monitor_rpc.py check_rpc.py /opt/chainflip-tg-bot/
sudo cp monitor_rpc.toml.sample /etc/chainflip-tg-bot/monitor_rpc.toml
sudo editor /etc/chainflip-tg-bot/monitor_rpc.toml

sudo cp chainflip-rpc-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now chainflip-rpc-monitor
sudo journalctl -u chainflip-rpc-monitor -f
```

In the config, set:
- `telegram.bot_token` — Telegram bot token (can reuse the validator bot's token)
- `telegram.chat_id` — your Telegram user id (DM [@userinfobot](https://t.me/userinfobot) to find it)
- `[endpoints.<chain>]` sections — one or more endpoint URLs per chain you want to monitor. Labels (`public`, `internal`, `secondary`, etc.) are arbitrary and appear in alert messages.

### One-shot diagnostic

Run a single poll across all configured endpoints and print heights/lag/errors without starting the daemon — useful for verifying endpoint config:

```bash
python3 check_rpc.py            # all chains
python3 check_rpc.py eth tron   # only these
python3 check_rpc.py -c /path/to/monitor_rpc.toml
```

### Alert behaviour

For each endpoint we remember the earliest chain-max height we've observed elsewhere that this endpoint hasn't yet reached. Severity = time since that observation. Unreachable endpoints are critical immediately.

| Severity | Default time-behind |
|---|---|
| Warning  | 30s |
| Alert    | 120s |
| Critical | 300s |

Block time is intentionally not part of the formula — the semantic is *"did we reach height X within T seconds of observing it elsewhere"*, which works the same for sub-second Arbitrum and 10-minute Bitcoin.

Per-chain poll intervals are configurable via `[poll_intervals]`. The default for every chain is `monitoring.poll_interval_seconds` (60s); override per chain if you want to slow down (or speed up) polling for specific chains:

```toml
[poll_intervals]
btc = 120
eth = 30
```

### Ground truth

A public reference RPC is queried per chain to detect cases where all of your nodes are stuck at the same height. Defaults (overridable via `[ground_truth]`):

| Chain | Default ground truth |
|---|---|
| btc  | `https://blockstream.info/api/blocks/tip/height` |
| eth  | `https://ethereum-rpc.publicnode.com` |
| arb  | `https://arb1.arbitrum.io/rpc` |
| sol  | `https://api.mainnet-beta.solana.com` |
| dot  | `https://rpc.polkadot.io` |
| hub  | `https://polkadot-asset-hub-rpc.polkadot.io` |
| tron | `https://api.trongrid.io/jsonrpc` |

### Files

| File | Purpose |
|---|---|
| `monitor_rpc.py` | RPC monitor daemon |
| `check_rpc.py` | One-shot diagnostic tool |
| `monitor_rpc.toml.sample` | Sample configuration |
| `chainflip-rpc-monitor.service` | systemd service file |

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).

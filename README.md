# Chainflip Telegram Validator Monitor

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

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).

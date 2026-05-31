# Deploying the TSX watcher on an always-on host (Oracle Cloud / VPS)

This runs the watcher 24/7 on a free Linux VM instead of GitHub Actions —
**no 6h job cap, no Actions minutes, repo stays private, no Make triggers.**
A systemd **timer** launches it each weekday at 06:55 ET; it runs pre-market
(7:00–9:30) + market hours (9:30–16:00) in one process, then exits. Saturday it
runs the weekly discovery scan. State (`seen_urls.json`, `open_positions.json`)
just persists on the VM's disk — no git commits needed.

## 1. Create the VM (Oracle Cloud Always Free)

1. Sign up at <https://www.oracle.com/cloud/free/> (credit card required for
   identity verification — **not charged** on Always Free).
2. Create a **Compute instance**:
   - Image: **Canonical Ubuntu 22.04** (or 24.04)
   - Shape: **VM.Standard.A1.Flex** (Ampere ARM, Always Free — give it 1 OCPU /
     6 GB) or **VM.Standard.E2.1.Micro** (AMD, Always Free).
   - Add your SSH public key (or let Oracle generate one and download it).
3. Note the instance's **public IP**.

## 2. SSH in and clone the repo

```bash
ssh ubuntu@<PUBLIC_IP>          # 'opc' instead of 'ubuntu' on some images

# Clone the PRIVATE repo. Easiest one-time: HTTPS with a GitHub token that has
# 'repo' read scope (you can reuse your existing PAT). Or set up a deploy key.
git clone https://<YOUR_GITHUB_PAT>@github.com/LucTrombert/tsx-watcher.git ~/tsx-watcher
```

## 3. Run the installer

```bash
cd ~/tsx-watcher
bash deploy/setup.sh
```

It installs Python + deps in a venv, creates `deploy/tsx-watcher.env`, installs
the systemd service + timer (auto-fixing the user/paths for your box), and
enables the timer.

## 4. Add your secrets

```bash
nano ~/tsx-watcher/deploy/tsx-watcher.env
```

Fill in `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `ANTHROPIC_API_KEY` (same values
as your GitHub Actions secrets). The file is `chmod 600` and gitignored.

## 5. Test it

```bash
sudo systemctl start tsx-watcher.service      # run a session now
journalctl -u tsx-watcher.service -f          # follow the logs
```

You should see the startup banner and `[HH:MM] Polling...` lines. Send a test
Telegram alert any time with:

```bash
cd ~/tsx-watcher && ./.venv/bin/python watcher.py --test
```

## Useful commands

| Command | What |
|---|---|
| `systemctl list-timers tsx-watcher.timer` | When it next fires |
| `systemctl status tsx-watcher.service` | Is it running now |
| `journalctl -u tsx-watcher.service -f` | Live logs |
| `journalctl -u tsx-watcher.service --since today` | Today's run |
| `sudo systemctl stop tsx-watcher.service` | Stop the current session |

## Keeping it updated

```bash
cd ~/tsx-watcher && git pull          # pull new code (ticker changes, fixes)
./.venv/bin/pip install -r requirements.txt   # if deps changed
```

The running session picks up changes on its next launch (next morning), or
restart it: `sudo systemctl restart tsx-watcher.service`.

## Decommissioning GitHub Actions

Once the VM is confirmed working, disable the GitHub side so signals don't fire
twice:
- Turn OFF both Make.com scenarios (Run A / Run B).
- In the repo: Settings → Actions → Disable Actions (or just delete the two
  Make triggers; the Saturday discovery cron can stay off too since the VM does it).

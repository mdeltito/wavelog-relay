# wavelog-relay

Headless rigctld daemon with bidirectional Wavelog integration and durable
WSJT-X forwarding.

Conceptually this draws inspiration from both
[WLGate](https://github.com/wavelog/WaveLogGate) and 
[WavelogGoat](https://github.com/johnsonm/WaveLogGoat), and it sits somewhere in between
the two in terms of goals/non-goals and scope.

- No GUI
- `rigctl` only
- First-class WSJT-X support for posting QSOs to Wavelog
- Suitable for running as a systemd service on the same host as `rigctld`

## What it does

- **Push rig state** `POST /api/radio` at 1 Hz. RFPOWER is quantised into
  0.5%-of-full-scale dedupe bins; unchanged state still re-POSTs every 30s
  as a liveness heartbeat. Dedupe state only advances on a successful POST,
  so transient Wavelog outages during a real QSY don't get swallowed.
- **Broadcast `radio_status`** on `ws://127.0.0.1:54322` for the Wavelog
  frontend's live dashboard rig card and bandmap page (otherwise polled
  every 3s). Origin enforced at the WS upgrade handshake.
- **Accept click-to-tune** on `127.0.0.1:54321`. Origin enforced at both the
  CORS layer and inside the handler.
- **Forward WSJT-X QSOs** (opt-in via `--wsjtx`) from the native binary UDP
  protocol on `:2237` (`Logged ADIF`, type 12) to `POST /api/qso`. QSOs are
  spooled to disk and replayed on next startup if Wavelog is down.

## Build

```sh
cargo build --release
sudo install -m 755 target/release/wavelog-relay /usr/local/bin/
```

## Run

```sh
WAVELOG_RELAY_KEY="<YOUR_WAVELOG_API_KEY>" \
wavelog-relay \
  --rigctld 127.0.0.1:4532 \
  --wavelog-url https://wavelog.example.com/index.php \
  --radio FT-710 \
  --power-max 100
```

Every flag also reads `WAVELOG_RELAY_<UPPERCASE>` from the environment:

| Flag | Default | Notes |
|---|---|---|
| `--rigctld <ADDR>` | `127.0.0.1:4532` | rigctld host:port. Accepts IPv4/IPv6 (`[::1]:4532`) or hostnames (`rig.local:4532` — DNS at connect time) |
| `--wavelog-url <URL>` | _(required)_ | Wavelog base URL |
| `--radio <NAME>` | _(required)_ | Identifier sent in the JSON push; must match the radio you configured in Wavelog |
| `--key-file <PATH>` | _(see Secrets)_ | Path to file containing the API key |
| `--power-max <W>` | `100` | Rig's max RF power, used to scale rigctld's `0.0..=1.0` RFPOWER reading |
| `--listen <ADDR>` | `127.0.0.1:54321` | Click-to-tune listener bind |
| `--ws-listen <ADDR>` | `127.0.0.1:54322` | WebSocket bind. Wavelog's frontend hardcodes this port — only change it if you're fronting with a reverse proxy |
| `--no-ws` | _(off)_ | Disable the WebSocket server. Frontend falls back to its 3 s AJAX poll |
| `--wsjtx` | _(off)_ | Enable the WSJT-X UDP listener for forwarding logged QSOs to Wavelog |
| `--wsjtx-listen <ADDR>` | `127.0.0.1:2237` | WSJT-X UDP listener bind (honored only with `--wsjtx`). Must match WSJT-X's `Settings -> Reporting -> UDP Server` *and* its delivery model — unicast (`127.0.0.1`) or multicast (`224.0.0.1`). See [WSJT-X QSO forwarding](#wsjt-x-qso-forwarding) |
| `--station-id <ID>` | _(auto-resolved)_ | Pin a specific Wavelog station profile ID for QSO submissions. When unset, the daemon resolves the currently-active station from Wavelog at first QSO (cached 60s). Run `wavelog-relay stations` to look up IDs |
| `--qso-queue-path <PATH>` | `$XDG_STATE_HOME/wavelog-relay/qso_queue.jsonl` | On-disk JSONL spool for WSJT-X QSOs awaiting Wavelog. Created if absent |
| `--interval <DUR>` | `1s` | Humantime: `1s`, `500ms`, etc. |
| `--rig-timeout <DUR>` | `5s` | Per-command read timeout against rigctld. On expiry the connection is dropped and the actor reconnects via backoff |
| `--config <PATH>` | _(auto)_ | Optional TOML; auto-discovered at `$XDG_CONFIG_HOME/wavelog-relay/config.toml` |
| `--log-level <LEVEL>` | `info` | Tracing filter; `RUST_LOG` env overrides |

Precedence: CLI > env > TOML > built-in defaults.

## Secrets

The API is sourced from the first available of:

1. `WAVELOG_RELAY_KEY` env (raw key value).
2. `WAVELOG_RELAY_KEY_FILE` env (path to a file containing the key).
3. `--key-file <path>` (same).

Whitespace and trailing newlines in the key file are trimmed.

## Config file

Optional. Field names mirror the CLI flags (snake-cased).

```toml
rigctld = "127.0.0.1:4532"
wavelog_url = "https://wavelog.example.com/index.php"
radio = "FT-710"
power_max = 100.0
listen = "127.0.0.1:54321"
ws_listen = "127.0.0.1:54322"
no_ws = false
wsjtx = true
wsjtx_listen = "127.0.0.1:2237"
station_id = "1"
qso_queue_path = "/var/lib/wavelog-relay/qso_queue.jsonl"
interval = "1s"
rig_timeout = "5s"
log_level = "info"

# Optional, per-mode hamlib overrides for the ambiguous 
# `pkt`/`dig` Wavelog modes:
[mode_overrides]
pkt = "PKTUSB"
dig = "PKTLSB"
```

## Power conversion

`rigctld` reports `RFPOWER` as a fraction `0.0..=1.0`. `wavelog-relay` multiplies
by `--power-max` to produce watts for the Wavelog payload. **Set `--power-max`
to your rig's actual maximum**, otherwise your QSO logs will record a
silently-wrong wattage.

If your rig or hamlib backend doesn't expose `RFPOWER` readback (some older rigs
return `RPRT -11`), the `power` field is omitted from both the `/api/radio` POST
and the WebSocket `radio_status` frame. The rig card still updates with
frequency and mode.

## Mode mapping

Wavelog sends the mode as a lowercase URL path segment; we translate to hamlib's
uppercase names:

| Wavelog | hamlib |
|---|---|
| `lsb` | `LSB` |
| `usb` | `USB` |
| `cw` | `CW` |
| `fm` | `FM` |
| `am` | `AM` |
| `rtty` | `RTTY` |
| `pktlsb` | `PKTLSB` |
| `pktusb` | `PKTUSB` |
| `pktfm` | `PKTFM` |
| `pkt` | `PKTUSB` (override via `[mode_overrides] pkt`) |
| `dig` | `PKTUSB` (override via `[mode_overrides] dig`) |

`pkt` and `dig` are ambiguous in the Wavelog UI as they don't carry sideband
information. The defaults assume USB (matches typical FT8/FT4); override per
`[mode_overrides]` if you need LSB.

The reverse direction (rig -> Wavelog) passes the hamlib mode name through
unmodified — Wavelog normalizes `CW-U` -> `CW`, `USB-D` -> `USB`, etc.,
server-side.

## systemd

Run as a systemd service on the same host as `rigctld`, bound to its
lifecycle so it starts after and restarts when `rigctld` does. Example
unit with WSJT-X enabled on multicast:

`/etc/systemd/system/wavelog-relay.service`

```ini
[Unit]
Description=Wavelog Relay
After=rigctld.service
BindsTo=rigctld.service

[Service]
Type=exec
Environment=WAVELOG_RELAY_KEY_FILE=/etc/wavelog-relay/key
ExecStart=/usr/local/bin/wavelog-relay \
  --rigctld 127.0.0.1:4532 \
  --wavelog-url https://wavelog.example.com/index.php \
  --radio FT-710 \
  --power-max 100 \
  --wsjtx \
  --wsjtx-listen 224.0.0.1:2237 \
  --log-level info
Restart=on-failure
RestartSec=5

[Install]
WantedBy=rigctld.target
```

```sh
cargo build --release
sudo install -m 755 target/release/wavelog-relay /usr/local/bin/
sudo install -d -m 700 /etc/wavelog-relay
sudo install -m 600 my-key-file /etc/wavelog-relay/key
sudo systemctl enable --now wavelog-relay
journalctl -u wavelog-relay -f
```

## WebSocket

Serves `ws://127.0.0.1:54322` for the frontend's live `radio_status` updates.
The dashboard rig card and the bandmap page both update in real time instead of
on the 3 s AJAX poll. Disable with `--no-ws` and the frontend falls back to that
poll.

## WSJT-X QSO forwarding

Pass `--wsjtx` (or set `WAVELOG_RELAY_WSJTX=1`, or `wsjtx = true` in TOML) to enable. 

**NOTE** JTDX and MSHV speak the same protocol; the configuration is identical and
it should work with either, but it has not yet been tested and your mileage may vary.

### Station profile

Wavelog's `/api/qso` requires a `station_profile_id`. wavelog-relay handles
this in one of two ways:

- **Auto-resolve (default).** When `--station-id` is unset, the daemon asks
  Wavelog for the currently-active station (`station_active=1` in
  `/api/station_info`) the first time a QSO arrives, then caches the result
  for 60 seconds. Flip the active station in Wavelog's UI and the next QSO
  routes to the new profile — no daemon restart required. Common case:
  setting your home station active for SSB rag-chews and flipping to a POTA
  profile for an activation, with WSJT-X following automatically.
- **Static override.** Pass `--station-id <ID>` to pin a specific profile.
  The daemon never calls `/api/station_info` in this mode and the 60s cache
  is bypassed entirely.

Look up the IDs (and see which one is active) with `wavelog-relay stations`:

```sh
wavelog-relay stations
#     ID  NAME      CALLSIGN
#     --  --------  --------
#     1   Home      N0CALL
# [*] 2   Portable  N0CALL/P
```

`stations` is one-shot — it hits `/api/station_info`, prints the table, and
exits. The `[*]` marker flags the active profile. Same `--wavelog-url` /
`--key-file` / `WAVELOG_RELAY_KEY` resolution as the daemon.

### Pick a delivery model

WSJT-X has one "UDP Server" destination — either a unicast address or a
multicast group. `--wsjtx-listen` has to match WSJT-X's setting or no QSOs
arrive (silently). Pick based on what else consumes the WSJT-X feed on the host:

- **Solo** (only wavelog-relay) -> unicast `127.0.0.1:2237`. Both ends default
  to this; nothing to configure.
- **Shared with GridTracker2 / JTAlert / log4om / etc.** -> multicast
  `224.0.0.1:2237`. Every subscriber gets a copy.

In WSJT-X: **Settings -> Reporting -> UDP Server** = `127.0.0.1` (solo) or
`224.0.0.1` (shared), port `2237`. Then:

```sh
# solo
wavelog-relay ... --wsjtx
# shared
wavelog-relay ... --wsjtx --wsjtx-listen 224.0.0.1:2237
```

For multicast, point every other consumer at `224.0.0.1:2237` and enable its
multicast toggle if applicable.

### Persistent QSO queue

QSOs are spooled to `$XDG_STATE_HOME/wavelog-relay/qso_queue.jsonl` (override
with `--qso-queue-path`) before being handed to the POST worker. Entries are
removed only after Wavelog confirms success — or replies with a permanent
`Rejected` (duplicate, validation failure). Transient errors leave the entry on
disk; the next daemon start replays it.

A Wavelog outage longer than the `[0, 1, 4]` s retry schedule no longer drops
QSOs. Restart the daemon when Wavelog comes back and the spool empties on its
own. The file is capped at 1000 entries (~5 hours of contest-rate FT8); the
oldest are evicted with a WARN beyond that.

## Browsers (Safari note)

Safari may silently block the mixed-content requests to `127.0.0.1` that
click-to-tune and the live rig card depend on. If either doesn't work on Safari,
use Chromium/Firefox or terminate TLS in a reverse proxy.

## Releases

Versioning is driven by [`cargo-release`](https://github.com/crate-ci/cargo-release).

```sh
cargo install cargo-release

cargo release patch              # dry-run, prints what would happen
cargo release patch --execute    # 0.1.0 -> 0.1.1
cargo release minor --execute    # 0.1.1 -> 0.2.0
```

## License

MIT. See [LICENSE](LICENSE).

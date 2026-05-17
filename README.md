# wavelog-bridge

Bridge between `rigctld` and Wavelog, implemented as a static binary and suitable for
running as a system service. Supports bi-directional rig state sync and click-to-tune,
plus optional WSJT-X QSO forwarding.

- **Outbound (HTTP)**: polls rigctld at 1 Hz and pushes rig state (frequency,
  mode, power) to Wavelog's `/api/radio`.
- **Outbound (WebSocket)**: serves `ws://127.0.0.1:54322` and broadcasts
  `radio_status` frames every tick, driving Wavelog's rig card on the dashboard
  and the bandmap page in real time instead of on the 3 s AJAX poll.
- **Inbound (click-to-tune)**: listens on `127.0.0.1:54321` for Wavelog's
  click-to-tune callback and dispatches `F`/`M` commands to rigctld.
- **Inbound (WSJT-X QSO log)**: opt-in via `--wsjtx`. When enabled, listens on
  `udp://127.0.0.1:2237` for WSJT-X's binary "network message" protocol and
  forwards each logged QSO (the `Logged ADIF` message) to Wavelog's `/api/qso`.
  Replaces WavelogGate's WSJT-X bridge.

## Build

```sh
cargo build --release
sudo install -m 755 target/release/wavelog-bridge /usr/local/bin/
```

## Run

```sh
WAVELOG_BRIDGE_KEY="<YOUR_WAVELOG_API_KEY>" \
wavelog-bridge \
  --rigctld 127.0.0.1:4532 \
  --wavelog-url https://wavelog.example.com/index.php \
  --radio FT-710 \
  --power-max 100
```

Every flag also reads `WAVELOG_BRIDGE_<UPPERCASE>` from the environment:

| Flag | Default | Notes |
|---|---|---|
| `--rigctld <ADDR>` | `127.0.0.1:4532` | rigctld host:port. Accepts IPv4/IPv6 (`[::1]:4532`) or hostnames (`rig.local:4532` — DNS at connect time) |
| `--wavelog-url <URL>` | _(required)_ | Wavelog base URL |
| `--radio <NAME>` | _(required)_ | Identifier sent in the JSON push; must match the radio you configured in Wavelog |
| `--key-file <PATH>` | _(see Secrets)_ | Path to file containing the API key |
| `--power-max <W>` | `100` | Rig's max RF power, used to scale rigctld's `0.0..=1.0` RFPOWER reading |
| `--listen <ADDR>` | `127.0.0.1:54321` | Click-to-tune listener bind |
| `--ws-listen <ADDR>` | `127.0.0.1:54322` | WebSocket bind. Wavelog's frontend hardcodes this port — only change it if you're fronting the bridge with a reverse proxy |
| `--no-ws` | _(off)_ | Disable the WebSocket server. Frontend falls back to its 3 s AJAX poll |
| `--wsjtx` | _(off)_ | Enable the WSJT-X UDP listener for forwarding logged QSOs to Wavelog. Requires `--station-id` |
| `--wsjtx-listen <ADDR>` | `127.0.0.1:2237` | WSJT-X UDP listener bind (honored only with `--wsjtx`). Must match WSJT-X's `Settings -> Reporting -> UDP Server` *and* its delivery model — unicast (`127.0.0.1`) or multicast (`224.0.0.1`). See [WSJT-X QSO forwarding](#wsjt-x-qso-forwarding) |
| `--station-id <ID>` | _(required if `--wsjtx`)_ | Wavelog station profile ID for QSO submissions. Run `wavelog-bridge stations` to look up IDs |
| `--qso-queue-path <PATH>` | `$XDG_STATE_HOME/wavelog-bridge/qso_queue.jsonl` | On-disk JSONL spool for WSJT-X QSOs awaiting Wavelog. Created if absent |
| `--interval <DUR>` | `1s` | Humantime: `1s`, `500ms`, etc. |
| `--rig-timeout <DUR>` | `3s` | Per-command read timeout against rigctld. On expiry the connection is dropped and the actor reconnects via backoff |
| `--config <PATH>` | _(auto)_ | Optional TOML; auto-discovered at `$XDG_CONFIG_HOME/wavelog-bridge/config.toml` |
| `--log-level <LEVEL>` | `info` | Tracing filter; `RUST_LOG` env overrides |

Precedence: CLI > env > TOML > built-in defaults.

## Secrets

The API is sourced from the first available of:

1. `WAVELOG_BRIDGE_KEY` env (raw key value).
2. `WAVELOG_BRIDGE_KEY_FILE` env (path to a file containing the key).
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
qso_queue_path = "/var/lib/wavelog-bridge/qso_queue.jsonl"
interval = "1s"
rig_timeout = "3s"
log_level = "info"

# Optional, per-mode hamlib overrides for the ambiguous 
# `pkt`/`dig` Wavelog modes:
[mode_overrides]
pkt = "PKTUSB"
dig = "PKTLSB"
```

## Power conversion

`rigctld` reports `RFPOWER` as a fraction `0.0..=1.0`. `wavelog-bridge` multiplies
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

The intended setup (for my own use-case at least) is running as a systemd
service on the same host as `rigctld`, and running as a dependency of `rigctld`
so it starts after and restarts if `rigctld` dies. 

As an example, with WSJT-X support enabled and bound for multicast:

`/etc/systemd/system/wavelog-bridge.service`

```ini
[Unit]
Description=Wavelog rig-state bridge
After=rigctld.service
BindsTo=rigctld.service

[Service]
Type=exec
Environment=WAVELOG_BRIDGE_KEY_FILE=/etc/wavelog-bridge/key
ExecStart=/usr/local/bin/wavelog-bridge \
  --rigctld 127.0.0.1:4532 \
  --wavelog-url https://wavelog.example.com/index.php \
  --radio FT-710 \
  --power-max 100 \
  --station-id 1 \
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
sudo install -m 755 target/release/wavelog-bridge /usr/local/bin/
sudo install -d -m 700 /etc/wavelog-bridge
sudo install -m 600 my-key-file /etc/wavelog-bridge/key
sudo systemctl enable --now wavelog-bridge
journalctl -u wavelog-bridge -f
```

## WebSocket

Serves `ws://127.0.0.1:54322` for the frontend's live `radio_status` updates.
The dashboard rig card and the bandmap page both update in real time instead of
on the 3 s AJAX poll. Disable with `--no-ws` and the frontend falls back to that
poll.

## WSJT-X QSO forwarding

Pass `--wsjtx` (or set `WAVELOG_BRIDGE_WSJTX=1`, or `wsjtx = true` in TOML) to enable. 

**NOTE** JTDX and MSHV speak the same protocol; the configuration is identical and
it should work with either, but it has not yet been tested and your mileage may vary.

Wavelog's `/api/qso` requires a `station_profile_id`, and the daemon refuses to
start with `--wsjtx` unless `--station-id` is set. Look yours up once:

```sh
wavelog-bridge stations
# ID  NAME      CALLSIGN
# --  --------  --------
#  1  Home      N0CALL
#  2  Portable  N0CALL/P
```

`stations` is one-shot — it hits `/api/station_info`, prints the table, and
exits. Same `--wavelog-url` / `--key-file` / `WAVELOG_BRIDGE_KEY` resolution as
the daemon.

### Pick a delivery model

WSJT-X has one "UDP Server" destination — either a unicast address or a
multicast group. `--wsjtx-listen` has to match WSJT-X's setting or no QSOs
arrive (silently). Pick based on what else consumes the WSJT-X feed on the host:

- **Solo** (only wavelog-bridge) -> unicast `127.0.0.1:2237`. Both ends default
  to this; nothing to configure.
- **Shared with GridTracker2 / JTAlert / log4om / etc.** -> multicast
  `224.0.0.1:2237`. Every subscriber gets a copy.

In WSJT-X: **Settings -> Reporting -> UDP Server** = `127.0.0.1` (solo) or
`224.0.0.1` (shared), port `2237`. Then:

```sh
# solo
wavelog-bridge ... --wsjtx --station-id 1
# shared
wavelog-bridge ... --wsjtx --wsjtx-listen 224.0.0.1:2237 --station-id 1
```

For multicast, point every other consumer at `224.0.0.1:2237` and enable its
multicast toggle if applicable.

### Persistent QSO queue

QSOs are spooled to `$XDG_STATE_HOME/wavelog-bridge/qso_queue.jsonl` (override
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

Each `--execute` run bumps the version in `Cargo.toml` / `Cargo.lock`,
commits it, creates a `vX.Y.Z` tag, and pushes both to `origin`. The
pushed tag triggers the `release` GitHub Actions workflow, which builds
binaries for Linux x86_64, Linux aarch64, and macOS aarch64, then
publishes a GitHub Release with the archives and sha256 sums.

Releases are only permitted from the `main` branch — configured under
`[package.metadata.release]` in `Cargo.toml`.

## License

MIT. See [LICENSE](LICENSE).

# wavelog-bridge

Rust bridge between rigctld and Wavelog. One static binary, both directions:

- **Outbound (HTTP)**: polls rigctld at 1 Hz and pushes rig state (frequency, mode, power) to Wavelog's `/api/radio`.
- **Outbound (WebSocket bandmap)**: serves `ws://127.0.0.1:54322` and broadcasts `radio_status` frames to Wavelog's bandmap client every tick, so the rig card and bandmap update live instead of on the 3 s AJAX poll.
- **Inbound (click-to-tune)**: listens on `127.0.0.1:54321` for Wavelog's click-to-tune callback and dispatches `F`/`M` commands to rigctld.
- **Inbound (WSJT-X QSO log)**: opt-in via `--wsjtx`. When enabled, listens on `udp://127.0.0.1:2237` for WSJT-X's binary "network message" protocol and forwards each logged QSO (the `Logged ADIF` message) to Wavelog's `/api/qso`. Replaces WavelogGate's WSJT-X bridge.

No GUI. Targets Linux and macOS. MIT.

## v1 scope

Single rig, hamlib/rigctld only, single VFO. Multi-radio profiles, flrig XML-RPC, split-mode push, native WSS (port 54323), forwarding the frontend's inbound bandmap messages (`qso_logged` → UDP, `satellite_position` / `lookup_result` → rotctld), and a persistent retry queue for WSJT-X QSOs are deferred.

## Build

```sh
cargo build --release
install -m 755 target/release/wavelog-bridge /usr/local/bin/
```

Stripped release binary is ~4.4 MB.

## Wavelog setup

1. Get an API key with read+write scope: **Account → API Keys**.
2. Set the CAT URL for click-to-tune: **Account → Radios → [your radio] → CAT URL** = `http://127.0.0.1:54321`. The `radio` name configured there must match `--radio`.

## Run

```sh
WAVELOG_BRIDGE_KEY="$(pass show wavelog/api)" \
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
| `--ws-listen <ADDR>` | `127.0.0.1:54322` | WebSocket bandmap bind. Wavelog's frontend hardcodes this port — only change it if you're fronting the bridge with a reverse proxy |
| `--no-ws` | _(off)_ | Disable the WebSocket bandmap server. Frontend falls back to its 3 s AJAX poll |
| `--wsjtx` | _(off)_ | Enable the WSJT-X UDP listener for forwarding logged QSOs to Wavelog. Requires `--station-id` |
| `--wsjtx-listen <ADDR>` | `127.0.0.1:2237` | WSJT-X UDP listener bind (honored only with `--wsjtx`). Must match WSJT-X's `Settings → Reporting → UDP Server` |
| `--station-id <ID>` | _(required if `--wsjtx`)_ | Wavelog station profile ID for QSO submissions. Run `wavelog-bridge stations` to look up IDs |
| `--interval <DUR>` | `1s` | Humantime: `1s`, `500ms`, etc. |
| `--rig-timeout <DUR>` | `3s` | Per-command read timeout against rigctld. On expiry the connection is dropped and the actor reconnects via backoff |
| `--config <PATH>` | _(auto)_ | Optional TOML; auto-discovered at `$XDG_CONFIG_HOME/wavelog-bridge/config.toml` |
| `--log-level <LEVEL>` | `info` | Tracing filter; `RUST_LOG` env overrides |

Precedence: CLI > env > TOML > built-in defaults.

## Secrets

The API key is never accepted as a positional CLI flag — it would show up in `ps` and shell history. Sources, highest precedence first:

1. `WAVELOG_BRIDGE_KEY` env (raw key value).
2. `WAVELOG_BRIDGE_KEY_FILE` env (path to a file containing the key).
3. `--key-file <path>` (same).

Whitespace and trailing newlines in the key file are trimmed.

## Config file

Optional. Field names mirror the CLI flags (snake-cased). Per-mode hamlib overrides for the ambiguous `pkt`/`dig` Wavelog modes live in `[mode_overrides]`:

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
interval = "1s"
rig_timeout = "3s"
log_level = "info"

[mode_overrides]
pkt = "PKTUSB"
dig = "PKTLSB"
```

## Power conversion

rigctld reports RFPOWER as a fraction `0.0..=1.0`. wavelog-bridge multiplies by `--power-max` to produce watts for the Wavelog payload. **Set `--power-max` to your rig's actual maximum** — otherwise your QSO logs will record a silently-wrong wattage. Going through hamlib's `\power2mW` (which would let the rig itself convert) is out of scope for v1.

## Mode mapping

Wavelog sends the mode as a lowercase URL path segment; we translate to hamlib's uppercase names:

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

`pkt` and `dig` are ambiguous in the Wavelog UI — they don't carry sideband information. The defaults assume USB (matches typical FT8/FT4); override per `[mode_overrides]` if you need LSB.

The reverse direction (rig → Wavelog) passes the hamlib mode name through unmodified — Wavelog normalizes `CW-U` → `CW`, `USB-D` → `USB`, etc., server-side.

## systemd

`/etc/systemd/system/wavelog-bridge.service`:

```ini
[Unit]
Description=Wavelog rig-state bridge
After=network-online.target rigctld.service
Wants=network-online.target
Requires=rigctld.service

[Service]
Type=exec
ExecStart=/usr/local/bin/wavelog-bridge \
  --rigctld 127.0.0.1:4532 \
  --wavelog-url https://wavelog.example.com/index.php \
  --radio FT-710 \
  --power-max 100
Environment=WAVELOG_BRIDGE_KEY_FILE=/etc/wavelog-bridge/key
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
```

```sh
sudo install -d -m 700 /etc/wavelog-bridge
sudo install -m 600 my-key-file /etc/wavelog-bridge/key
sudo systemctl enable --now wavelog-bridge
journalctl -u wavelog-bridge -f
```

On `systemctl suspend` / wake, both rigctld and wavelog-bridge recover automatically — the actor's capped exponential backoff (`500 ms → 1 s → 2 s → 5 s → 10 s`) handles the rigctld side, and reqwest retries handle transient Wavelog failures.

## WebSocket bandmap

Wavelog's frontend (`assets/js/cat.js`) opens a WebSocket to the local machine for live `radio_status` updates: first `wss://127.0.0.1:54323`, then falling back to `ws://127.0.0.1:54322`. wavelog-bridge serves the **WS port only** (54322). The frontend's WSS attempt fails fast and the fallback connects within a second, so you'll see one extra connection attempt in devtools and nothing else.

Native WSS (54323) is deferred. If you need it today, terminate TLS in a reverse proxy and forward to `ws://127.0.0.1:54322`, or run with `--no-ws` and let the frontend fall back to its 3 s AJAX poll.

The frontend will also send messages back over the socket (`qso_logged`, `satellite_position`, `lookup_result`) — wavelog-bridge accepts and discards these at debug log level. Forwarding to UDP (N1MM/JTDX) or rotctld is a future iteration with its own configuration.

## WSJT-X QSO forwarding

**Off by default.** Pass `--wsjtx` (or set `WAVELOG_BRIDGE_WSJTX=1`, or `wsjtx = true` in TOML) to enable.

WSJT-X (and forks JTDX / MSHV) broadcast every logged QSO over UDP as a binary "network message". With `--wsjtx` set, wavelog-bridge listens on `udp://127.0.0.1:2237`, parses the `Logged ADIF` (type 12) message, and POSTs the ADIF string to Wavelog's `/api/qso` endpoint — completing a QSO in WSJT-X's Log QSO dialog appears in Wavelog within a second, no manual entry.

**WSJT-X setup**: open **Settings → Reporting → UDP Server**.

| Field | Value |
|---|---|
| UDP Server | `127.0.0.1` |
| UDP Server port number | `2237` |
| Accept UDP requests | (optional, no effect on us) |

If you run JTDX or MSHV, the menu path and field names are the same — they speak the identical protocol.

**Station profile ID**: Wavelog's `/api/qso` requires a `station_profile_id`. The daemon will not start with `--wsjtx` unless `--station-id` is set. Run the `stations` subcommand once to look it up:

```sh
wavelog-bridge stations
# ID  NAME      CALLSIGN
# --  --------  --------
#  1  Home      K1AB
#  2  Portable  K1AB/P
```

The `stations` subcommand is a one-shot — it hits `/api/station_info`, prints a table, and exits. Same `--wavelog-url` / `--key-file` / `WAVELOG_BRIDGE_KEY` resolution as the daemon.

**Limitations**:
- Only `Logged ADIF` (type 12) is forwarded. The structured `QSO Logged` (type 5) message that precedes it is parsed and discarded to avoid double-logging.
- No persistent retry queue. If Wavelog is unreachable longer than the standard `[0, 1, 4]` s retry schedule, the QSO is dropped with a `warn` log line. WavelogGate behaves the same way; if this matters for you, file an issue.
- The listener is unicast on the loopback address only. Multicast (`224.0.0.1`) is deferred.
- A bounded queue (32 entries) sits between the UDP listener and the POST worker; overflow is logged as `wsjtx POST queue full` and drops the newest datagram. In practice the queue holds ~30 s of contest-rate FT8 logs.

## Browsers (Safari note)

Click-to-tune issues `fetch('http://127.0.0.1:54321/<hz>/<mode>')` from an HTTPS Wavelog page, and the bandmap client opens `ws://127.0.0.1:54322` — both are mixed content in the strict sense. Chromium and Firefox treat `127.0.0.1` as a potentially-trustworthy origin per the [Secure Contexts](https://w3c.github.io/webappsec-secure-contexts/) spec and allow both. Safari is stricter and may silently block them. If you're on Safari and either feature doesn't work, switch to Chromium/Firefox or put a TLS terminator in front of wavelog-bridge.

## Verifying

Click any spot in Wavelog's DX Cluster. Two things should happen:

1. Browser devtools (Network tab) shows `GET http://127.0.0.1:54321/<hz>/<mode>` returning 200, with `Access-Control-Allow-Origin` set to your Wavelog origin.
2. The rig retunes to the spot's frequency and mode, and Wavelog's rig card refreshes within a second.

If (1) works but (2) doesn't, check `journalctl -u rigctld` for `RPRT -N` errors (rig refused the command).

If (1) doesn't work, check that:
- The CAT URL in Wavelog → Account → Radios is exactly `http://127.0.0.1:54321` (HTTP, not HTTPS).
- wavelog-bridge is running (`systemctl status wavelog-bridge`).
- The browser isn't Safari, or you've worked around its mixed-content block.

For a manual smoke test from the host shell:

```sh
curl -i http://127.0.0.1:54321/14074000/usb
# HTTP/1.1 200 OK
```

No `Access-Control-Allow-Origin` header on this — that's expected for a curl request without an `Origin` header. The rig should still retune.

Manual WS bandmap smoke (replace `https://wavelog.example.com` with your configured origin — wavelog-bridge rejects mismatched `Origin` headers with `403`):

```sh
websocat ws://127.0.0.1:54322 -H 'Origin: https://wavelog.example.com'
# {"type":"welcome"}
# {"type":"radio_status","radio":"FT-710","frequency":14074000,...}
# (one frame per second; ^C to exit)
```

## License

MIT. See [LICENSE](LICENSE).

# wavelog-bridge

Rust bridge between rigctld and Wavelog. One static binary, both directions:

- **Outbound (HTTP)**: polls rigctld at 1 Hz and pushes rig state (frequency, mode, power) to Wavelog's `/api/radio`.
- **Outbound (WebSocket)**: serves `ws://127.0.0.1:54322` and broadcasts `radio_status` frames every tick, driving Wavelog's rig card on the dashboard and the bandmap page in real time instead of on the 3 s AJAX poll.
- **Inbound (click-to-tune)**: listens on `127.0.0.1:54321` for Wavelog's click-to-tune callback and dispatches `F`/`M` commands to rigctld.
- **Inbound (WSJT-X QSO log)**: opt-in via `--wsjtx`. When enabled, listens on `udp://127.0.0.1:2237` for WSJT-X's binary "network message" protocol and forwards each logged QSO (the `Logged ADIF` message) to Wavelog's `/api/qso`. Replaces WavelogGate's WSJT-X bridge.

No GUI. Targets Linux and macOS. MIT.

## v1 scope

Single rig, hamlib/rigctld only, single VFO. Multi-radio profiles, flrig XML-RPC, split-mode push, native WSS (port 54323), and forwarding the frontend's inbound bandmap messages (`qso_logged` → UDP, `satellite_position` / `lookup_result` → rotctld) are deferred.

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
| `--ws-listen <ADDR>` | `127.0.0.1:54322` | WebSocket bind. Wavelog's frontend hardcodes this port — only change it if you're fronting the bridge with a reverse proxy |
| `--no-ws` | _(off)_ | Disable the WebSocket server. Frontend falls back to its 3 s AJAX poll |
| `--wsjtx` | _(off)_ | Enable the WSJT-X UDP listener for forwarding logged QSOs to Wavelog. Requires `--station-id` |
| `--wsjtx-listen <ADDR>` | `127.0.0.1:2237` | WSJT-X UDP listener bind (honored only with `--wsjtx`). Must match WSJT-X's `Settings → Reporting → UDP Server` *and* its delivery model — unicast (`127.0.0.1`) or multicast (`224.0.0.1`). See [WSJT-X QSO forwarding](#wsjt-x-qso-forwarding) |
| `--station-id <ID>` | _(required if `--wsjtx`)_ | Wavelog station profile ID for QSO submissions. Run `wavelog-bridge stations` to look up IDs |
| `--qso-queue-path <PATH>` | `$XDG_STATE_HOME/wavelog-bridge/qso_queue.jsonl` | On-disk JSONL spool for WSJT-X QSOs awaiting Wavelog. Created if absent. See "Persistent QSO queue" below |
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
qso_queue_path = "/var/lib/wavelog-bridge/qso_queue.jsonl"
interval = "1s"
rig_timeout = "3s"
log_level = "info"

[mode_overrides]
pkt = "PKTUSB"
dig = "PKTLSB"
```

## Power conversion

rigctld reports RFPOWER as a fraction `0.0..=1.0`. wavelog-bridge multiplies by `--power-max` to produce watts for the Wavelog payload. **Set `--power-max` to your rig's actual maximum** — otherwise your QSO logs will record a silently-wrong wattage. Going through hamlib's `\power2mW` (which would let the rig itself convert) is out of scope for v1.

If your rig or hamlib backend doesn't expose RFPOWER readback (some older rigs return `RPRT -11`), the `power` field is omitted from both the `/api/radio` POST and the WebSocket `radio_status` frame — the rig card still updates with frequency and mode. RFPOWER reports the user-set TX level, not delivered TX power; switching to `RFPOWER_METER` (which only meters during transmit and isn't supported by every backend) is a deferred v3 concern.

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

## WebSocket

Wavelog's frontend (`assets/js/cat.js`) opens a WebSocket to the local machine for live `radio_status` updates: first `wss://127.0.0.1:54323`, then falling back to `ws://127.0.0.1:54322`. wavelog-bridge serves the **WS port only** (54322). The frontend's WSS attempt fails fast and the fallback connects within a second, so you'll see one extra connection attempt in devtools and nothing else. The same frames drive the dashboard rig card and the bandmap page; both stay live without the 3 s AJAX poll.

Native WSS (54323) is deferred. If you need it today, terminate TLS in a reverse proxy and forward to `ws://127.0.0.1:54322`, or run with `--no-ws` and let the frontend fall back to its 3 s AJAX poll.

The frontend's bandmap page also sends messages back over the same socket (`qso_logged`, `satellite_position`, `lookup_result`) — wavelog-bridge accepts and discards these at debug log level. Forwarding to UDP (N1MM/JTDX) or rotctld is a future iteration with its own configuration.

## WSJT-X QSO forwarding

**Off by default.** Pass `--wsjtx` (or set `WAVELOG_BRIDGE_WSJTX=1`, or `wsjtx = true` in TOML) to enable.

WSJT-X (and forks JTDX / MSHV) broadcast every logged QSO over UDP as a binary "network message". With `--wsjtx` set, wavelog-bridge parses the `Logged ADIF` (type 12) message and POSTs the ADIF string to Wavelog's `/api/qso` endpoint — completing a QSO in WSJT-X's Log QSO dialog appears in Wavelog within a second, no manual entry. JTDX and MSHV speak the same protocol; the configuration is identical.

**Station profile ID first.** Wavelog's `/api/qso` requires a `station_profile_id`, and the daemon refuses to start with `--wsjtx` unless `--station-id` is set. Look yours up once:

```sh
wavelog-bridge stations
# ID  NAME      CALLSIGN
# --  --------  --------
#  1  Home      K1AB
#  2  Portable  K1AB/P
```

The `stations` subcommand is one-shot — it hits `/api/station_info`, prints the table, and exits. Same `--wavelog-url` / `--key-file` / `WAVELOG_BRIDGE_KEY` resolution as the daemon.

### Pick a delivery model

WSJT-X has exactly one "UDP Server" destination. That destination is either a **unicast** address (delivered to one socket) or a **multicast** group (delivered to every subscribed socket). wavelog-bridge's `--wsjtx-listen` must match WSJT-X's delivery model — point them at different models and you'll see no QSOs at all, silently. Pick based on what else is on the host:

- **Solo (only wavelog-bridge consumes the WSJT-X feed)** → use unicast `127.0.0.1:2237`. Both ends default to this; nothing to configure.
- **Shared with GridTracker2 / JTAlert / log4om / anything else** → use multicast `224.0.0.1:2237`. Every consumer that subscribes to the group gets a copy; no forwarders, no port-stealing.

### Solo setup (no other UDP consumers)

WSJT-X **Settings → Reporting → UDP Server**:

| Field | Value |
|---|---|
| UDP Server | `127.0.0.1` |
| UDP Server port number | `2237` |
| Accept UDP requests | (no effect on us) |

Then run wavelog-bridge with the defaults — `--wsjtx-listen` is already `127.0.0.1:2237`:

```sh
wavelog-bridge ... --wsjtx --station-id 1
```

### Sharing the feed with GridTracker2 / JTAlert (recommended: multicast)

WSJT-X **Settings → Reporting → UDP Server**:

| Field | Value |
|---|---|
| UDP Server | `224.0.0.1` |
| UDP Server port number | `2237` |
| Outgoing interfaces | leave default unless you know you need a specific NIC |

wavelog-bridge:

```sh
wavelog-bridge ... --wsjtx --wsjtx-listen 224.0.0.1:2237 --station-id 1
```

GridTracker2: **Settings → Logging → WSJT-X UDP** → set the address to `224.0.0.1`, port `2237`, and tick **Multicast**. JTAlert and log4om expose equivalent multicast toggles in their WSJT-X integration panels.

That's it — every subscriber on `224.0.0.1:2237` receives every datagram. The pipeline becomes pub/sub: add or remove consumers without touching the others. Any group address in the `224.0.0.0/4` range works; `224.0.0.1` (the all-hosts group) is the convention WSJT-X documentation uses.

#### GridTracker2 forwarder (fallback when you can't change WSJT-X's config)

If you can't reconfigure WSJT-X's UDP Server (shared rig, locked-down install), use GT2 as a unicast forwarder instead. Leave WSJT-X pointing at `127.0.0.1:2237` (where GT2 already listens). In GridTracker2: **Settings → Forwarding → Add Forwarder**, point it at a free port — say `127.0.0.1:2238`. Then:

```sh
wavelog-bridge ... --wsjtx --wsjtx-listen 127.0.0.1:2238 --station-id 1
```

GT2 receives, processes, and forwards an unmodified copy to us. Downside vs. multicast: GT2 becomes a single point of failure for our log path — if GT2 isn't running, QSOs don't reach wavelog-bridge.

### Troubleshooting "WSJT-X sends but wavelog-bridge logs nothing"

The most common cause is a delivery-model mismatch — WSJT-X sending unicast while wavelog-bridge listens on multicast (or vice-versa). Quick checks:

- **WSJT-X log window**: it logs every UDP send with the destination address. Confirm it matches `--wsjtx-listen` *exactly* (including unicast vs. group address).
- **Who holds the port** (Linux): `ss -ulnp | grep 2237`. With multicast you should see wavelog-bridge bound to `224.0.0.1:2237`; with unicast, `127.0.0.1:2237`. If something else is bound to the unicast port, that process is the one receiving — not us.
- **wavelog-bridge debug log**: `--log-level debug` prints `wsjtx logged ADIF received` (or `non-LoggedADIF message ignored` for heartbeats/status) for every datagram. Silence here means nothing is arriving at the socket.

A trap to know about: an earlier multicast bind used a wildcard (`0.0.0.0`) socket, which *also* received unicast traffic to any local IP on the same port. That made misconfigured "WSJT-X unicasts to `127.0.0.1` / wavelog-bridge listens on `224.0.0.1`" setups appear to work — right up until another consumer (e.g. GT2) bound the specific unicast address and silently stole the packets. The current Linux build binds the multicast group address directly, so a delivery-model mismatch fails immediately and consistently rather than intermittently. macOS still uses the wildcard bind — the BSD socket API doesn't allow binding a multicast address — so the trap still applies there. If a setup works on macOS and stops working when you migrate to Linux, a delivery-model mismatch is the first thing to check.

### WavelogGate is a different feed

WavelogGate listens on port `2333` for the **N1MM Logger+ plaintext** WSJT-X output, which is a separate Reporting tab from the binary UDP Server. wavelog-bridge consumes the binary feed (`2237`), so a WavelogGate-on-2333 + GT2-on-2237 setup doesn't conflict with us — they're three independent feeds in WSJT-X.

### Persistent QSO queue

QSOs received from WSJT-X are spooled to `$XDG_STATE_HOME/wavelog-bridge/qso_queue.jsonl` (override with `--qso-queue-path`) **before** being handed to the POST worker. Entries are removed from the file only after Wavelog confirms the submission succeeded — or replies with a permanent `Rejected` (duplicate, validation failure). Transient errors (5xx, transport failures, malformed responses) leave the entry on disk; the next daemon start replays it.

This means a Wavelog outage longer than the standard `[0, 1, 4]` s retry schedule no longer drops QSOs. Restart the daemon when Wavelog comes back and the spool empties on its own.

The file is capped at 1000 entries; the oldest are evicted with a WARN if you somehow accumulate more (~5 hours of contest-rate FT8). If the file becomes corrupt — partial write from a power loss, manual hand-edit gone wrong — it's renamed to `<path>.corrupt-<unix-ms>` and the queue starts fresh; the corrupt copy is yours to inspect or recover. Running multiple `wavelog-bridge` instances against the same file is unsupported.

### Other limitations

- Only `Logged ADIF` (type 12) is forwarded. The structured `QSO Logged` (type 5) message that precedes it is parsed and discarded to avoid double-logging.
- A bounded in-memory queue (32 entries) sits between the UDP listener and the POST worker; overflow is logged as `wsjtx POST queue full` and drops the newest datagram (the entry is still on disk if persistence is enabled). In practice the queue holds ~30 s of contest-rate FT8 logs.

## Browsers (Safari note)

Click-to-tune issues `fetch('http://127.0.0.1:54321/<hz>/<mode>')` from an HTTPS Wavelog page, and the frontend opens `ws://127.0.0.1:54322` for live rig status — both are mixed content in the strict sense. Chromium and Firefox treat `127.0.0.1` as a potentially-trustworthy origin per the [Secure Contexts](https://w3c.github.io/webappsec-secure-contexts/) spec and allow both. Safari is stricter and may silently block them. If you're on Safari and either feature doesn't work, switch to Chromium/Firefox or put a TLS terminator in front of wavelog-bridge.

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

Manual WS smoke (replace `https://wavelog.example.com` with your configured origin — wavelog-bridge rejects mismatched `Origin` headers with `403`):

```sh
websocat ws://127.0.0.1:54322 -H 'Origin: https://wavelog.example.com'
# {"type":"welcome"}
# {"type":"radio_status","radio":"FT-710","frequency":14074000,...}
# (one frame per second; ^C to exit)
```

## License

MIT. See [LICENSE](LICENSE).

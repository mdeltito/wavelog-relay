# wavelog-bridge

Rust bridge between rigctld and Wavelog. One static binary, both directions:

- **Outbound**: polls rigctld at 1 Hz and pushes rig state (frequency, mode, power) to Wavelog's `/api/radio`.
- **Inbound**: listens on `127.0.0.1:54321` for Wavelog's click-to-tune callback and dispatches `F`/`M` commands to rigctld.

No GUI. Targets Linux and macOS. MIT.

## v1 scope

Single rig, hamlib/rigctld only, single VFO. Multi-radio profiles, flrig XML-RPC, split-mode push, and the WebSocket bandmap (ports 54322/54323) are deferred.

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
| `--listen <ADDR>` | `127.0.0.1:54321` | Listener bind |
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

## Browsers (Safari note)

Click-to-tune issues `fetch('http://127.0.0.1:54321/<hz>/<mode>')` from an HTTPS Wavelog page — mixed content in the strict sense. Chromium and Firefox treat `127.0.0.1` as a potentially-trustworthy origin per the [Secure Contexts](https://w3c.github.io/webappsec-secure-contexts/) spec and allow it. Safari is stricter and may silently block these requests. If you're on Safari and click-to-tune doesn't work, switch to Chromium/Firefox or put a TLS terminator in front of wavelog-bridge.

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

## License

MIT. See [LICENSE](LICENSE).

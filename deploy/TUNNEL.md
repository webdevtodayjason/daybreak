# DAYBREAK — Cloudflare Tunnel (daybreak.example.com)

Publishes `http://localhost:8811` on the Orange Pi to `https://daybreak.example.com`.
No inbound port forward, no public IP, no TLS to manage on the Pi.

Run everything below **on the Pi** unless a step says otherwise. `sudo` throughout —
the tunnel runs as root so its credentials live in `/etc/cloudflared`.

---

## 1. Install cloudflared (arm64)

```bash
curl -fsSL -o /tmp/cloudflared.deb \
  https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64.deb
sudo dpkg -i /tmp/cloudflared.deb
cloudflared --version
```

`dpkg` not available on your image? Grab the static binary instead:

```bash
sudo curl -fsSL -o /usr/local/bin/cloudflared \
  https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64
sudo chmod +x /usr/local/bin/cloudflared
```

## 2. Authorize the zone

```bash
sudo cloudflared tunnel login
```

Prints a URL. Open it in a browser **on your laptop**, sign in, pick `example.com`.
Writes `/root/.cloudflared/cert.pem`. One time per machine.

## 3. Create the tunnel

```bash
sudo cloudflared tunnel create daybreak
sudo cloudflared tunnel list
```

Note the UUID it prints — that is `<TUNNEL_UUID>` below.

## 4. Install the config

```bash
sudo mkdir -p /etc/cloudflared
TUNNEL_UUID=$(sudo cloudflared tunnel list --output json \
  | python3 -c "import sys,json;print([t['id'] for t in json.load(sys.stdin) if t['name']=='daybreak'][0])")
echo "tunnel: $TUNNEL_UUID"

sudo cp "/root/.cloudflared/$TUNNEL_UUID.json" /etc/cloudflared/
sudo chmod 600 "/etc/cloudflared/$TUNNEL_UUID.json"
sudo sed "s/<TUNNEL_UUID>/$TUNNEL_UUID/g" /opt/daybreak/deploy/cloudflared-config.yml \
  | sudo tee /etc/cloudflared/config.yml >/dev/null
```

## 5. Point DNS at it

```bash
sudo cloudflared tunnel route dns daybreak daybreak.example.com
```

Already-exists error? The name is taken by an old record — overwrite it:

```bash
sudo cloudflared tunnel route dns --overwrite-dns daybreak daybreak.example.com
```

## 6. Run it as a service

```bash
sudo cloudflared service install
sudo systemctl enable --now cloudflared
sudo systemctl status cloudflared --no-pager
```

## 7. Verify

```bash
curl -sS https://daybreak.example.com/healthz ; echo
curl -sSI https://daybreak.example.com/ | head -n1
sudo cloudflared tunnel info daybreak          # should show 2+ connector edges
```

Then open <https://daybreak.example.com> — live wire populating, TIINY panel moving,
rack cam streaming.

---

## After you edit anything

```bash
sudo systemctl restart cloudflared        # config.yml changes
sudo systemctl restart daybreak           # server.py / index.html changes
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Error **1033** in browser | tunnel not connected | `systemctl status cloudflared`; `journalctl -u cloudflared -n 50` |
| **502** from the edge | `server.py` down | `curl 127.0.0.1:8811/healthz`; `systemctl restart daybreak` |
| **404** on every path | hostname mismatch in `config.yml` | must be exactly `daybreak.example.com`, then restart cloudflared |
| Board loads, cam is dead | ustreamer down | `systemctl status daybreak-camera`; `curl -I 127.0.0.1:8812/snapshot` |
| Cam stalls, then **524** | camera froze for >100 s (Cloudflare's edge cuts a silent request) | the page's `<img>` retries on error; power-cycle the USB cam if it persists |
| `route dns` says record exists | stale CNAME | rerun with `--overwrite-dns` |

## Optional: lock it down

The board is read-only and holds no secrets (the Tiiny key never leaves the Pi), so
public is fine for a showcase. To gate it anyway: Cloudflare dashboard →
**Zero Trust → Access → Applications → Add** → self-hosted, domain
`daybreak.example.com`, policy *Emails ending in @…* or a one-time PIN. No change on
the Pi.

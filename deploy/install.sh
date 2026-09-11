#!/usr/bin/env bash
# DAYBREAK installer — run as root on the Orange Pi.
# Idempotent: safe to re-run after every code change; it re-copies files,
# re-installs the units and restarts them. It never overwrites /etc/daybreak.env.
#
#   bash deploy/install.sh
#
set -euo pipefail

APP_DIR=/opt/daybreak
ENV_FILE=/etc/daybreak.env
UNIT_DIR=/etc/systemd/system
RUN_USER=daybreak
UNITS=(daybreak.service daybreak-pipeline.service daybreak-camera.service)
# Files each worker owns; a missing one means the build is incomplete.
APP_FILES=(schema.sql db.py feeds.py enrich.py pipeline.py server.py static/index.html)

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -t 1 ]; then C1=$'\033[1;36m'; C2=$'\033[32m'; C3=$'\033[33m'; C4=$'\033[31m'; C0=$'\033[0m'
else C1=""; C2=""; C3=""; C4=""; C0=""; fi
say()  { printf '\n%s==>%s %s\n' "$C1" "$C0" "$*"; }
ok()   { printf '    %sok%s    %s\n' "$C2" "$C0" "$*"; }
warn() { printf '    %swarn%s  %s\n' "$C3" "$C0" "$*"; }
die()  { printf '\n%sFATAL%s %s\n' "$C4" "$C0" "$*" >&2; exit 1; }

# ---------------------------------------------------------------- preflight
say "Preflight"
[ "$(id -u)" -eq 0 ] || die "run as root:  sudo bash $0"
command -v systemctl >/dev/null 2>&1 || die "systemd not found; this installer targets a systemd host"
command -v python3   >/dev/null 2>&1 || die "python3 not found;  apt-get install -y python3"
python3 - <<'PY' || die "python3 is older than 3.11 — DAYBREAK needs 3.11+"
import sys
sys.exit(0 if sys.version_info >= (3, 11) else 1)
PY
ok "python3 $(python3 -c 'import sys;print("%d.%d.%d"%sys.version_info[:3])')"

missing=()
for f in "${APP_FILES[@]}"; do [ -f "$SRC/$f" ] || missing+=("$f"); done
[ ${#missing[@]} -eq 0 ] || die "missing source files in $SRC: ${missing[*]}"
ok "source tree $SRC"

# ---------------------------------------------------------------- user
say "Service account"
if id -u "$RUN_USER" >/dev/null 2>&1; then
  ok "user $RUN_USER exists"
else
  NOLOGIN="$(command -v nologin || echo /bin/false)"
  # --user-group is load-bearing: the three units declare Group=daybreak and the
  # install -g calls below name it too. Without it a host whose login.defs sets
  # USERGROUPS_ENAB no would leave every unit failing on an unknown group.
  useradd --system --user-group --home-dir "$APP_DIR" --shell "$NOLOGIN" "$RUN_USER"
  ok "created system user $RUN_USER"
fi
getent group "$RUN_USER" >/dev/null 2>&1 || groupadd --system "$RUN_USER"
id -nG "$RUN_USER" 2>/dev/null | tr ' ' '\n' | grep -qx "$RUN_USER" || \
  usermod -g "$RUN_USER" "$RUN_USER" || warn "could not set primary group $RUN_USER"
# /dev/video0 for the rack cam.
if getent group video >/dev/null 2>&1; then
  usermod -aG video "$RUN_USER" || warn "could not add $RUN_USER to group video"
fi

# ---------------------------------------------------------------- files
say "Install application to $APP_DIR"
install -d -o root -g "$RUN_USER" -m 0775 "$APP_DIR"          # group-writable: sqlite WAL files live here
install -d -o root -g "$RUN_USER" -m 0775 "$APP_DIR/static"
install -d -o root -g root        -m 0755 "$APP_DIR/deploy"
for f in "${APP_FILES[@]}"; do
  install -o root -g root -m 0644 "$SRC/$f" "$APP_DIR/$f"
done
install -o root -g root -m 0755 "$SRC/deploy/load-models.sh" "$APP_DIR/load-models.sh"
for f in TUNNEL.md cloudflared-config.yml; do
  if [ -f "$SRC/deploy/$f" ]; then
    install -o root -g root -m 0644 "$SRC/deploy/$f" "$APP_DIR/deploy/$f"
  fi
done
if [ -f "$SRC/README.md" ]; then
  install -o root -g root -m 0644 "$SRC/README.md" "$APP_DIR/README.md"
fi
ok "${#APP_FILES[@]} app files + load-models.sh"

# ---------------------------------------------------------------- env file
say "Environment file $ENV_FILE"
if [ -f "$ENV_FILE" ]; then
  ok "exists — left untouched"
else
  cat >"$ENV_FILE" <<'EOF'
# DAYBREAK runtime environment. Read by systemd (root) before privileges drop.
# EDIT TIINY_KEY, then:  systemctl restart daybreak daybreak-pipeline

# Tiiny AI Pocket on the LAN (host only, port is fixed at 8800).
TIINY_HOST=192.168.1.50
# Device API key. Settings -> API key on the Tiiny. REQUIRED.
TIINY_KEY=

DAYBREAK_DB=/opt/daybreak/daybreak.db
PORT=8811
CAM_URL=http://127.0.0.1:8812

# Optional extra source. Leave commented out to run fully keyless.
#GNEWS_API_KEY=
EOF
  ok "template written"
fi
chown root:"$RUN_USER" "$ENV_FILE"
chmod 0640 "$ENV_FILE"

KEY_SET=0
if grep -Eq '^[[:space:]]*TIINY_KEY=[^[:space:]]+' "$ENV_FILE"; then KEY_SET=1; fi

# ---------------------------------------------------------------- ustreamer
say "Rack camera (ustreamer)"
if command -v ustreamer >/dev/null 2>&1; then
  ok "ustreamer present at $(command -v ustreamer)"
elif command -v apt-get >/dev/null 2>&1; then
  DEBIAN_FRONTEND=noninteractive apt-get update -qq || warn "apt-get update failed (offline?)"
  DEBIAN_FRONTEND=noninteractive apt-get install -y ustreamer >/dev/null 2>&1 || true
  if command -v ustreamer >/dev/null 2>&1; then
    ok "installed ustreamer"
  else
    warn "no ustreamer package on this release."
    warn "  fallback A:  apt-get install -y motion   (set stream_port 8812, stream_localhost on)"
    warn "  fallback B:  apt-get install -y ffmpeg   then replace ExecStart in $UNIT_DIR/daybreak-camera.service with:"
    warn "               /usr/bin/ffmpeg -f v4l2 -framerate 15 -video_size 1280x720 -i /dev/video0 \\"
    warn "                 -f mpjpeg -listen 1 http://127.0.0.1:8812/stream"
    warn "  the board renders 'CAM OFFLINE' until :8812 answers — everything else still works."
  fi
else
  warn "no apt-get; install ustreamer by hand and re-run"
fi

# ---------------------------------------------------------------- units
say "systemd units"
for u in "${UNITS[@]}"; do
  install -o root -g root -m 0644 "$SRC/deploy/$u" "$UNIT_DIR/$u"
done
# systemd needs an absolute ExecStart. The units ship with the Debian paths; when a
# binary actually lives somewhere else, pin it with a drop-in instead of editing units.
PY_BIN="$(command -v python3)"
if [ "$PY_BIN" != "/usr/bin/python3" ]; then
  for pair in "daybreak:server" "daybreak-pipeline:pipeline"; do
    u="${pair%%:*}"; script="${pair##*:}"
    install -d -o root -g root -m 0755 "$UNIT_DIR/$u.service.d"
    cat >"$UNIT_DIR/$u.service.d/10-path.conf" <<EOF
[Service]
ExecStart=
ExecStart=$PY_BIN $APP_DIR/$script.py
EOF
  done
  ok "drop-in pins python3 at $PY_BIN"
fi

UST="$(command -v ustreamer || true)"
if [ -n "$UST" ] && [ "$UST" != "/usr/bin/ustreamer" ]; then
  install -d -o root -g root -m 0755 "$UNIT_DIR/daybreak-camera.service.d"
  cat >"$UNIT_DIR/daybreak-camera.service.d/10-path.conf" <<EOF
[Service]
ExecStart=
ExecStart=$UST -d /dev/video0 -r 1280x720 -f 15 --host 127.0.0.1 -p 8812
EOF
  ok "drop-in pins ustreamer at $UST"
fi
systemctl daemon-reload
systemctl enable "${UNITS[@]}" >/dev/null 2>&1 || warn "enable reported a problem"
ok "installed + enabled: ${UNITS[*]}"

# ---------------------------------------------------------------- start
say "Start"
if [ -n "$UST" ]; then
  systemctl restart daybreak-camera.service || warn "daybreak-camera failed to start (camera unplugged? it retries every 10s)"
else
  systemctl stop daybreak-camera.service >/dev/null 2>&1 || true
  warn "daybreak-camera enabled but not started (no ustreamer binary)"
fi

if [ "$KEY_SET" -eq 1 ]; then
  systemctl restart daybreak.service daybreak-pipeline.service
  ok "daybreak + daybreak-pipeline restarted"
else
  warn "TIINY_KEY is empty in $ENV_FILE — services enabled but NOT started."
  warn "  set the key, then:  systemctl restart daybreak daybreak-pipeline"
fi

# ---------------------------------------------------------------- status
say "Status"
systemctl --no-pager --lines=0 status "${UNITS[@]}" 2>/dev/null | \
  grep -E 'daybreak|Active:' || true

if [ "$KEY_SET" -eq 1 ]; then
  PORT_N="$(grep -E '^[[:space:]]*PORT=' "$ENV_FILE" | tail -n1 | cut -d= -f2 | tr -d '[:space:]')"
  PORT_N="${PORT_N:-8811}"
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if curl -fsS -m 2 "http://127.0.0.1:${PORT_N}/healthz" >/dev/null 2>&1; then
      ok "http://127.0.0.1:${PORT_N}/healthz answering"
      break
    fi
    sleep 1
  done
fi

say "Next"
cat <<EOF
  1. set TIINY_KEY        : nano $ENV_FILE  &&  systemctl restart daybreak daybreak-pipeline
  2. load the embedder    : bash $APP_DIR/load-models.sh
  3. publish the board    : see $APP_DIR/deploy/TUNNEL.md
  logs                    : journalctl -fu daybreak-pipeline -u daybreak
  data                    : $APP_DIR/daybreak.db
EOF

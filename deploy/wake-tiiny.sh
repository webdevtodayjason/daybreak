#!/usr/bin/env bash
# wake-tiiny.sh — bring the Tiiny Pocket back to full DAYBREAK duty after a
# power-up: wait for the device, start the three models, verify all three
# actually answer. Idempotent; run from the Mac, the Pi, or anywhere on the LAN.
#
#   TIINY_KEY=<bearer> ./wake-tiiny.sh [device-ip]
#
# Key lookup order: $TIINY_KEY, /etc/daybreak.env, ~/.tiiny-key
# Reminder: the device boots LOCKED — power it on bare, give it ~2 min to
# unlock (gateway :8800 comes up), then this script does the rest.
set -u
HOST="${1:-${TIINY_HOST:-192.168.1.50}}"
B="http://$HOST:8800"

KEY="${TIINY_KEY:-}"
[ -z "$KEY" ] && [ -f /etc/daybreak.env ] && KEY=$(sed -n 's/^TIINY_KEY=//p' /etc/daybreak.env)
[ -z "$KEY" ] && [ -f "$HOME/.tiiny-key" ] && KEY=$(cat "$HOME/.tiiny-key")
[ -z "$KEY" ] && { echo "FATAL: no TIINY_KEY (env, /etc/daybreak.env, or ~/.tiiny-key)"; exit 1; }
AUTH="Authorization: Bearer $KEY"

MODELS=(
  "deepreinforce-ai/Ornith-1.0-35B"
  "Qwen/Qwen3-Embedding-0.6B"
  "Tongyi-MAI/Z-Image-Turbo"
)

urlenc() { printf '%s' "$1" | sed 's|/|%2F|g'; }

echo "== waiting for the device gateway at $HOST:8800 (unlock takes ~2 min after power-on) =="
for i in $(seq 1 60); do
  code=$(curl -s -m4 -o /dev/null -w "%{http_code}" -H "$AUTH" "$B/api/v1/models/npu/status" 2>/dev/null)
  [ "$code" = "200" ] && { echo "   gateway UP"; break; }
  [ "$i" = "60" ] && { echo "FATAL: gateway never came up — is the device unlocked? (boot it BARE, wait 2 min)"; exit 1; }
  printf "   waiting (%s) try %d/60\r" "${code:-down}" "$i"; sleep 5
done
echo

roster() { curl -s -m6 -H "$AUTH" "$B/api/v1/models/npu/status"; }

echo "== starting models =="
for M in "${MODELS[@]}"; do
  state=$(roster | python3 -c "
import sys,json
try:
    for m in json.load(sys.stdin).get('models',[]):
        if m.get('model_id')=='$M': print(m.get('status')); break
except Exception: pass")
  if [ "$state" = "running" ]; then
    echo "   $M: already running"
    continue
  fi
  echo "   $M: starting..."
  curl -s -m30 -X POST -H "$AUTH" "$B/api/v1/models/$(urlenc "$M")/start" >/dev/null
done

echo "== waiting for all three to reach RUNNING (loads take ~30-90s) =="
for i in $(seq 1 40); do
  up=$(roster | python3 -c "
import sys,json
try:
    ms={m['model_id']:m.get('status') for m in json.load(sys.stdin).get('models',[])}
    print(sum(1 for m in '''${MODELS[*]}'''.split() if ms.get(m)=='running'))
except Exception: print(0)")
  [ "$up" = "3" ] && { echo "   all 3 RUNNING"; break; }
  [ "$i" = "40" ] && { echo "WARN: only $up/3 running after 200s — roster:"; roster | python3 -m json.tool; }
  printf "   %s/3 running, try %d/40\r" "$up" "$i"; sleep 5
done
echo

echo "== proof of life =="
echo -n "   chat (Ornith): "
curl -s -m90 -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"model":"deepreinforce-ai/Ornith-1.0-35B","messages":[{"role":"user","content":"Reply with the single word READY"}],"max_tokens":64}' \
  "$B/v1/chat/completions" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin); c=d['choices'][0]['message'].get('content','').strip()
    print(('OK — %r' % c[:20]) if c else 'OK (reasoning-only reply, model is up)')
except Exception as e: print('FAIL', str(e)[:60])"
echo -n "   embeddings:    "
curl -s -m30 -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"model":"Qwen/Qwen3-Embedding-0.6B","input":"ready"}' "$B/v1/embeddings" | python3 -c "
import sys,json
try: print('OK — %d-dim vector' % len(json.load(sys.stdin)['data'][0]['embedding']))
except Exception as e: print('FAIL', str(e)[:60])"
echo -n "   image (Z-Img): "
curl -s -m120 -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"model":"Tongyi-MAI/Z-Image-Turbo","prompt":"a green status light","negative_prompt":"text","width":512,"height":512,"seed":1,"steps":8}' \
  "$B/v1/image/generate" -o /tmp/wake-tiiny-img.bin -w "" ; \
  head -c1 /tmp/wake-tiiny-img.bin | grep -q '{' && echo "FAIL $(head -c 80 /tmp/wake-tiiny-img.bin)" || echo "OK — $(wc -c < /tmp/wake-tiiny-img.bin | tr -d ' ') bytes"
rm -f /tmp/wake-tiiny-img.bin

echo
echo "TIINY IS ON STATION. If the daybreak pipeline was in backoff it recovers"
echo "on its own within ~5 minutes (or: sudo systemctl restart daybreak-pipeline on the Pi)."

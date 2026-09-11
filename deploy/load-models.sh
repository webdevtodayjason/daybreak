#!/usr/bin/env bash
# DAYBREAK — put the embedding model on the Tiiny NPU and prove /v1/embeddings works.
#
#   bash load-models.sh
#
# Idempotent: already-downloaded skips the download, already-running skips the
# start, and a healthy device just re-runs the verification. Safe to re-run any
# time embeddings look dead (the board shows EMBEDDINGS OFF).
#
# Env: TIINY_HOST (default 192.168.1.50), TIINY_KEY (required).
# Falls back to /etc/daybreak.env for both when they are not already exported.
set -euo pipefail

MODEL_ID="${DAYBREAK_EMBED_MODEL:-Qwen/Qwen3-Embedding-0.6B}"
# Model ids MUST be percent-encoded in paths or the device 404s on the slash.
ENC="${MODEL_ID//\//%2F}"

HTTP_TIMEOUT=30
DOWNLOAD_TIMEOUT=2400   # ~1.2 GB over whatever uplink the Pi has
START_TIMEOUT=300
POLL=10

if [ -t 1 ]; then C1=$'\033[1;36m'; C2=$'\033[32m'; C3=$'\033[33m'; C4=$'\033[31m'; C0=$'\033[0m'
else C1=""; C2=""; C3=""; C4=""; C0=""; fi
say()  { printf '\n%s==>%s %s\n' "$C1" "$C0" "$*"; }
ok()   { printf '    %sok%s    %s\n' "$C2" "$C0" "$*"; }
warn() { printf '    %swarn%s  %s\n' "$C3" "$C0" "$*"; }
info() { printf '    ..    %s\n' "$*"; }
die()  { printf '\n%sFATAL%s %s\n' "$C4" "$C0" "$*" >&2; exit 1; }

case "${1:-}" in
  -h|--help)
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

# ---------------------------------------------------------------- env
if [ -z "${TIINY_KEY:-}" ] && [ -r /etc/daybreak.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /etc/daybreak.env
  set +a
fi
TIINY_HOST="${TIINY_HOST:-192.168.1.50}"
[ -n "${TIINY_KEY:-}" ] || die "TIINY_KEY is not set (export it, or fill in /etc/daybreak.env)"
command -v curl    >/dev/null 2>&1 || die "curl not found"
command -v python3 >/dev/null 2>&1 || die "python3 not found"

BASE="http://${TIINY_HOST}:8800"

# ---------------------------------------------------------------- helpers
HTTP_BODY=""
HTTP_CODE=""
api() { # api METHOD PATH [JSON_BODY]
  local method=$1 path=$2 data=${3:-} out
  if [ -n "$data" ]; then
    out="$(curl -sS -m "$HTTP_TIMEOUT" -X "$method" \
      -H "Authorization: Bearer ${TIINY_KEY}" -H 'Content-Type: application/json' \
      -d "$data" -w $'\n%{http_code}' "${BASE}${path}" 2>/dev/null || true)"
  else
    out="$(curl -sS -m "$HTTP_TIMEOUT" -X "$method" \
      -H "Authorization: Bearer ${TIINY_KEY}" \
      -w $'\n%{http_code}' "${BASE}${path}" 2>/dev/null || true)"
  fi
  HTTP_CODE="${out##*$'\n'}"
  HTTP_BODY="${out%$'\n'*}"
  case "$HTTP_CODE" in
    ''|*[!0-9]*) HTTP_CODE=000; HTTP_BODY="$out" ;;
  esac
}

# Read one value out of the last response. Expression sees `d` (parsed JSON).
jexpr() {
  printf '%s' "$HTTP_BODY" | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(3)
try:
    v = eval(sys.argv[1], {"__builtins__": {"len": len, "str": str, "list": list}}, {"d": d})
except Exception:
    sys.exit(4)
print("" if v is None else v)
' "$1" 2>/dev/null || true
}

model_status() { # -> not_found | not_downloaded | downloading | downloaded | ...
  api GET "/api/v1/models/${ENC}/get_progress"
  if [ "$HTTP_CODE" != "200" ]; then
    echo "not_found"
    return
  fi
  local s
  s="$(jexpr 'd.get("status","")')"
  echo "${s:-unknown}"
}

is_running() {
  api GET "/api/v1/models/running"
  [ "$HTTP_CODE" = "200" ] || return 1
  local hit
  hit="$(jexpr '"yes" if "'"$MODEL_ID"'" in (d.get("running") or []) else ""')"
  [ "$hit" = "yes" ]
}

# ---------------------------------------------------------------- 1. device
say "Device ${BASE}"
api GET "/api/v1/models/npu/status"
[ "$HTTP_CODE" = "200" ] || die "device unreachable or key rejected (HTTP ${HTTP_CODE}): ${HTTP_BODY}"
NPU_TOTAL="$(jexpr 'd.get("npu_total","?")')"
NPU_USED="$(jexpr 'd.get("npu_used","?")')"
NPU_FREE="$(jexpr 'd.get("npu_available","?")')"
ok "reachable — NPU ${NPU_USED}/${NPU_TOTAL} used, ${NPU_FREE} free"
LOADED="$(jexpr '", ".join("%s(%su,%s)" % (m.get("model_id"), m.get("npu_usage"), m.get("status")) for m in (d.get("models") or []))')"
if [ -n "$LOADED" ]; then info "loaded: ${LOADED}"; fi

case "$NPU_FREE" in
  ''|*[!0-9]*) : ;;
  *) if [ "$NPU_FREE" -lt 2 ]; then
       warn "only ${NPU_FREE} NPU units free; the embedder needs 1. If start fails, stop an idle model:"
       warn "  curl -X POST -H \"Authorization: Bearer \$TIINY_KEY\" ${BASE}/api/v1/models/<enc-id>/stop"
     fi ;;
esac

# ---------------------------------------------------------------- 2. download
say "Model ${MODEL_ID}"
STATUS="$(model_status)"
info "status: ${STATUS}"

if [ "$STATUS" = "running" ] || is_running; then
  ok "already loaded on the NPU — nothing to download or start"
else
  if [ "$STATUS" = "downloaded" ]; then
    ok "already on disk — skipping download"
  else
    info "requesting download (this is the slow part; ~1.2 GB)"
    api POST "/api/v1/models/${ENC}/download"
    if [ "$HTTP_CODE" != "200" ]; then
      die "download request failed (HTTP ${HTTP_CODE}): ${HTTP_BODY}"
    fi
    waited=0
    while :; do
      STATUS="$(model_status)"
      api GET "/api/v1/models/${ENC}/get_progress"
      PCT="$(jexpr 'd.get("progress","?")')"
      printf '    ..    %-14s %s%%   (%ss elapsed)\n' "$STATUS" "$PCT" "$waited"
      case "$STATUS" in
        downloaded|running) break ;;
        download_stop|failed|error) die "download stopped at ${PCT}% (status=${STATUS}). Re-run to resume." ;;
      esac
      [ "$waited" -lt "$DOWNLOAD_TIMEOUT" ] || die "download still not finished after ${DOWNLOAD_TIMEOUT}s — re-run to resume"
      sleep "$POLL"
      waited=$((waited + POLL))
    done
    ok "downloaded"
  fi

  # -------------------------------------------------------------- 3. start
  say "Loading onto the NPU"
  api POST "/api/v1/models/${ENC}/start"
  if [ "$HTTP_CODE" != "200" ]; then
    die "start failed (HTTP ${HTTP_CODE}): ${HTTP_BODY}"
  fi
  info "$(jexpr 'd.get("message","start requested")')"
  waited=0
  until is_running; do
    [ "$waited" -lt "$START_TIMEOUT" ] || die "model did not reach running within ${START_TIMEOUT}s"
    sleep 5
    waited=$((waited + 5))
    info "waiting for runtime… (${waited}s)"
  done
  ok "running"
fi

# ---------------------------------------------------------------- 4. verify
say "Verifying /v1/embeddings"
api POST "/v1/embeddings" "{\"model\":\"${MODEL_ID}\",\"input\":\"daybreak embedding smoke test\"}"
if [ "$HTTP_CODE" != "200" ]; then
  die "embeddings endpoint returned HTTP ${HTTP_CODE}: ${HTTP_BODY}"
fi
DIM="$(jexpr 'len(d["data"][0]["embedding"])')"
HEAD="$(jexpr '", ".join("%.5f" % x for x in d["data"][0]["embedding"][:4])')"
[ -n "$DIM" ] || die "unexpected embeddings payload: ${HTTP_BODY}"
ok "vector length ${DIM}  [${HEAD}, …]"

api GET "/api/v1/models/npu/status"
ok "NPU now $(jexpr 'd.get("npu_used","?")')/$(jexpr 'd.get("npu_total","?")') used"

say "Done"
cat <<EOF
  ${MODEL_ID} is live. The pipeline picks it up on its next enrichment and
  flips meta embeddings -> on; the board's bottom bar will read EMBEDDINGS ON
  within a minute or two. Nothing to restart.

  If it ever reads OFF again:  bash $0
EOF

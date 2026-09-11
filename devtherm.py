#!/usr/bin/env python3
"""Tiiny device thermals, sampled over SSH because the REST API does not carry them.

The device's own `/api/v1/npu/status` has the right shape — it returns `temp_c` and
`power_w` fields and advertises `hm_smi_available: true` — but both values come back
null. Meanwhile `/sys/class/thermal` on the device holds fourteen live zones including
`npu-thermal`, and `/sys/class/thermal/cooling_device*` says whether anything is being
clamped. So the data exists, it just is not on the HTTP surface.

That matters for a burn-in: the whole point of running a device flat out for a week is
watching what happens to it thermally, and neither the vendor UI nor any API client can
see that today. This module closes the gap from outside.

Design constraints, because this runs beside a live board:

* Never blocks a page render. A background thread samples on an interval and the reader
  gets whatever the last sample was.
* Fails soft and stays quiet. If the device is unreachable, the panel shows nothing
  rather than an error, and the sampler keeps trying.
* Read-only. Every command is a `cat` of a sysfs file.
"""

import os
import subprocess
import threading
import time

import device                       # one spelling of the device host

HOST = device.host()
SSH_KEY = os.environ.get("TIINY_SSH_KEY") or "/etc/daybreak-tiiny.key"
SSH_PORT = int(os.environ.get("TIINY_SSH_PORT") or 3588)
SSH_USER = os.environ.get("TIINY_SSH_USER") or "tiiny"
INTERVAL_S = float(os.environ.get("DAYBREAK_THERM_INTERVAL") or 30)
TIMEOUT_S = float(os.environ.get("DAYBREAK_THERM_TIMEOUT") or 12)

# One round trip collects everything. Splitting it into several would multiply the SSH
# handshake cost, which dwarfs the reads themselves.
SCRIPT = (
    "for z in /sys/class/thermal/thermal_zone*/; do "
    "  echo \"T $(cat $z/type 2>/dev/null) $(cat $z/temp 2>/dev/null)\"; done; "
    "for c in /sys/class/thermal/cooling_device*/; do "
    "  echo \"C $(cat $c/type 2>/dev/null) $(cat $c/cur_state 2>/dev/null) "
    "$(cat $c/max_state 2>/dev/null)\"; done; "
    "for i in 0 4 8; do f=/sys/devices/system/cpu/cpu$i/cpufreq; "
    "  [ -d $f ] && echo \"F cpu$i $(cat $f/scaling_cur_freq) $(cat $f/cpuinfo_max_freq)\"; done; "
    "for d in /sys/class/devfreq/*/; do "
    "  echo \"D $(basename $d) $(cat $d/cur_freq 2>/dev/null)\"; done; "
    "echo \"M $(awk '/MemTotal/{t=$2}/MemAvailable/{a=$2}END{print t, a}' /proc/meminfo)\"; "
    "echo \"U $(cut -d. -f1 /proc/uptime)\"; "
    # devfreq min/max so the GPU and VPU clocks can be drawn against their real range
    "for d in /sys/class/devfreq/*/; do "
    "  echo \"E $(basename $d) $(cat $d/cur_freq 2>/dev/null) $(cat $d/min_freq 2>/dev/null) "
    "$(cat $d/max_freq 2>/dev/null)\"; done; "
    # /data is the encrypted userdata volume where models and the vault live
    "df -m / /data 2>/dev/null | awk '$6==\"/\"||$6==\"/data\"{print \"K\", $6, $3, $2}'; "
    "for n in /sys/class/net/*/; do i=$(basename $n); "
    "  case $i in lo|veth*|docker*|br-*) continue;; esac; "
    "  echo \"N $i $(cat $n/statistics/rx_bytes 2>/dev/null) "
    "$(cat $n/statistics/tx_bytes 2>/dev/null) $(cat $n/carrier 2>/dev/null)\"; done"
)

_state = {"ts": 0.0, "ok": False, "zones": {}, "cooling": [], "cpu": [],
          "devfreq": {}, "mem_total_mb": None, "mem_avail_mb": None,
          "uptime_s": None, "max_temp": None, "hottest": None,
          "npu_temp": None, "throttled": False, "error": None}
_lock = threading.Lock()


def _ssh():
    return subprocess.run(
        ["ssh", "-i", SSH_KEY, "-p", str(SSH_PORT),
         "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
         "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
         "-o", "ConnectTimeout=6", "-o", "LogLevel=ERROR",
         "%s@%s" % (SSH_USER, HOST), SCRIPT],
        capture_output=True, text=True, timeout=TIMEOUT_S)


def sample():
    """One collection pass. Returns the new state dict; never raises."""
    out = {"ts": time.time(), "ok": False, "zones": {}, "cooling": [], "cpu": [],
           "devfreq": {}, "mem_total_mb": None, "mem_avail_mb": None,
           "uptime_s": None, "max_temp": None, "hottest": None,
           "npu_temp": None, "throttled": False, "error": None,
           "clocks": {}, "disk": {}, "net": {}}
    try:
        res = _ssh()
    except subprocess.TimeoutExpired:
        out["error"] = "timeout"
        return out
    except OSError as exc:
        out["error"] = str(exc)[:80]
        return out
    if res.returncode != 0:
        out["error"] = (res.stderr or "ssh failed").strip()[:100]
        return out

    for line in res.stdout.splitlines():
        f = line.split()
        if not f:
            continue
        try:
            if f[0] == "T" and len(f) >= 3:
                # sysfs reports millidegrees
                out["zones"][f[1]] = round(int(f[2]) / 1000.0, 1)
            elif f[0] == "C" and len(f) >= 4:
                cur, mx = int(f[2]), int(f[3])
                out["cooling"].append({"type": f[1], "cur": cur, "max": mx})
                if cur > 0:
                    out["throttled"] = True
            elif f[0] == "F" and len(f) >= 4:
                out["cpu"].append({"cpu": f[1], "cur_mhz": int(f[2]) // 1000,
                                   "max_mhz": int(f[3]) // 1000})
            elif f[0] == "D" and len(f) >= 3 and f[2].isdigit():
                out["devfreq"][f[1]] = int(f[2])
            elif f[0] == "M" and len(f) >= 3:
                out["mem_total_mb"] = int(f[1]) // 1024
                out["mem_avail_mb"] = int(f[2]) // 1024
            elif f[0] == "U" and len(f) >= 2:
                out["uptime_s"] = int(f[1])
            elif f[0] == "E" and len(f) >= 5:
                # strip the address prefix: "15000000.gpu" -> "gpu"
                name = f[1].split(".")[-1]
                out["clocks"][name] = {"cur_mhz": int(f[2]) // 1000000,
                                       "min_mhz": int(f[3]) // 1000000,
                                       "max_mhz": int(f[4]) // 1000000}
            elif f[0] == "K" and len(f) >= 4:
                out["disk"][f[1]] = {"used_mb": int(f[2]), "total_mb": int(f[3])}
            elif f[0] == "N" and len(f) >= 4:
                out["net"][f[1]] = {"rx": int(f[2]), "tx": int(f[3]),
                                    "up": f[4] == "1" if len(f) > 4 else None}
        except (ValueError, IndexError):
            continue                      # one malformed line must not lose the sample

    if out["zones"]:
        out["ok"] = True
        hottest = max(out["zones"].items(), key=lambda kv: kv[1])
        out["hottest"], out["max_temp"] = hottest[0], hottest[1]
        # npu-thermal is the one an engineer actually wants during a burn-in.
        for name, val in out["zones"].items():
            if "npu" in name:
                out["npu_temp"] = val
                break
    else:
        out["error"] = out["error"] or "no thermal zones returned"
    return out


def get():
    """Last known sample. Cheap, never blocks, safe to call per request."""
    with _lock:
        return dict(_state)


def _rates(new, old):
    """Bytes/sec per interface, from the delta between two samples.

    Counters are cumulative and wrap or reset on interface bounce, so a negative delta
    is discarded rather than rendered as a spike.
    """
    dt = (new.get("ts") or 0) - (old.get("ts") or 0)
    if dt <= 0:
        return
    for name, cur in (new.get("net") or {}).items():
        prev = (old.get("net") or {}).get(name)
        if not prev:
            continue
        for k in ("rx", "tx"):
            delta = cur.get(k, 0) - prev.get(k, 0)
            if delta >= 0:
                cur[k + "_bps"] = int(delta / dt)


def _loop(stop=None):
    while stop is None or not stop.is_set():
        s = sample()
        with _lock:
            if s.get("ok") and _state.get("ok"):
                _rates(s, _state)
            _state.update(s)
        if stop is not None:
            if stop.wait(INTERVAL_S):
                return
        else:
            time.sleep(INTERVAL_S)


def start(stop=None):
    """Begin sampling in the background. Returns the thread, or None if no key."""
    if not os.path.exists(SSH_KEY):
        with _lock:
            _state["error"] = "no ssh key at %s" % SSH_KEY
        return None
    th = threading.Thread(target=_loop, args=(stop,), name="devtherm", daemon=True)
    th.start()
    return th


if __name__ == "__main__":
    import json
    s = sample()
    print(json.dumps(s, indent=2))
    if s["ok"]:
        print("\nhottest: %s %.1fC   npu: %s   throttled: %s"
              % (s["hottest"], s["max_temp"], s["npu_temp"], s["throttled"]))

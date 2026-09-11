#!/usr/bin/env python3
"""Watch the device lease and remember what just happened on it.

who() reads the holder record WITHOUT taking the lock, so this can sample as often as it
likes and never delay an inference. That is the only reason a live view of the device is
allowed to exist at all: a monitor that competes with the thing it monitors is worse than
no monitor.

Sampling gives us what a single process cannot see on its own. daybreak is two services
plus chorus, and each only knows about its own calls; the lock file is the one place all
of them are visible. So the board can show every request that touched the NPU, whoever
issued it.
"""

import threading
import time

INTERVAL_S = 0.25          # 4 Hz: fast enough to catch a 1s embedding call
KEEP = 60                  # recent completions retained for the board

# The `why` string every caller passes to device.lease(), mapped to a lane. Kept crude on
# purpose: a lane the board cannot name is better shown as "other" than guessed at.
LANES = (
    ("image",   ("image", "z-image", "imaging")),
    ("story",   ("desk report", "story")),
    ("dossier", ("dossier", "synthesis", "sitrep", "brief", "analyst")),
    ("compare", ("chorus",)),
    ("voice",   ("tts", "speech", "audio")),
    ("search",  ("rerank", "embed", "vector")),
    ("enrich",  ("completions", "enrich", "chat", "label", "geocode")),
)


def lane_of(why):
    w = (why or "").lower()
    for lane, keys in LANES:
        if any(k in w for k in keys):
            return lane
    return "other"


class Watch:
    def __init__(self):
        self.lock = threading.Lock()
        self.recent = []            # completed holds, newest last
        self.now = None             # what is held right this second
        self.queue = 0
        # A change counter and a per-completion sequence number, so a streaming client is
        # pushed only what it has not already been told. Engineers watching this will ask
        # what the observation resolution is: it is INTERVAL_S, and the stream emits at
        # that rate rather than at some slower poll of its own.
        self.version = 0
        self.seq = 0
        # Duty cycle in one-minute buckets. "Is this thing actually running?" is the first
        # question anyone asks of a live panel, and an idle gate at the moment you happen
        # to look is not an answer. The device works in bursts - a feed cycle lands, ~90
        # items get graded, then it is quiet until the next one - so an instant tells you
        # almost nothing and an hour tells you the truth.
        self.buckets = {}          # minute -> [busy samples, total samples, peak queue]
        self._save = None
        self._last_save = 0.0
        # A short ring of recent samples, for a load figure that MOVES. The device's own
        # util_percent is effectively binary - 0 when idle, 85-100 mid-generation,
        # nothing between - because it runs one inference at a time. Sampled every five
        # seconds that reads as a needle teleporting between the pegs. A trailing
        # fraction of a 10s window is the same truth with somewhere to go.
        self._ring = []
        self.totals = {}            # lane -> {n, secs}
        self.err = None
        self._cur = None
        self._t = None
        self._stop = threading.Event()

    # -- sampling ---------------------------------------------------------
    def _sample(self, holder):
        held = bool(holder.get("held"))
        self._ring.append(1 if held else 0)
        del self._ring[:-int(10.0 / INTERVAL_S)]        # trailing 10 seconds
        minute = int(time.time() // 60)
        b = self.buckets.setdefault(minute, [0, 0])
        b[0] += 1 if held else 0
        b[1] += 1
        # Peak queue per minute. Contention here is real but brief - measured at a 0.55s
        # median wait - so an instantaneous reading flashes 1 for two frames and is gone
        # before anyone looks. The peak is what makes a rare event legible.
        q = int(holder.get("queue") or 0)
        if len(b) < 3:
            b.append(0)
        b[2] = max(b[2], q)
        if len(self.buckets) > 70:                      # keep an hour, drop the rest
            for k in sorted(self.buckets)[:-65]:
                del self.buckets[k]
        who = holder.get("why") or holder.get("owner") or ""
        key = (holder.get("owner"), who, holder.get("since") or holder.get("held_for_s"))
        with self.lock:
            before = (self.queue, self._cur["key"][:2] if self._cur else None, held)
            self.queue = int(holder.get("queue") or 0)
            if held:
                if self._cur is None or self._cur["key"][:2] != key[:2]:
                    self._close(holder)          # a different holder: previous one ended
                    self._cur = {"key": key, "owner": holder.get("owner"),
                                 "why": who, "lane": lane_of(who), "start": time.time()}
                self.now = {"owner": self._cur["owner"], "why": self._cur["why"],
                            "lane": self._cur["lane"],
                            "held_s": round(time.time() - self._cur["start"], 1)}
            else:
                self._close(holder)
                self.now = None
            if before != (self.queue, self._cur["key"][:2] if self._cur else None, held):
                self.version += 1

    def _close(self, holder):
        if not self._cur:
            return
        secs = round(time.time() - self._cur["start"], 2)
        self.seq += 1
        self.version += 1
        rec = {"seq": self.seq, "owner": self._cur["owner"], "why": self._cur["why"],
               "lane": self._cur["lane"], "secs": secs, "at": time.time()}
        self.recent.append(rec)
        del self.recent[:-KEEP]
        t = self.totals.setdefault(self._cur["lane"], {"n": 0, "secs": 0.0})
        t["n"] += 1
        t["secs"] = round(t["secs"] + secs, 2)
        self._cur = None

    def _run(self, holder_fn):
        while not self._stop.wait(INTERVAL_S):
            try:
                self._sample(holder_fn())
                self.err = None
            except Exception as exc:            # a monitor must never take the app down
                self.err = "%s: %s" % (type(exc).__name__, str(exc)[:80])
            if self._save and time.time() - self._last_save > 30.0:
                self._last_save = time.time()
                self.persist()

    # -- api ---------------------------------------------------------------
    def start(self, holder_fn, load=None, save=None):
        """`load`/`save` persist the lane totals so they survive a restart.

        Without them the counters read as "since this process started", which is a
        different and much less interesting number than "what this device has done".
        A deploy would silently reset the tally and nobody would know why it dropped.
        """
        if self._t:
            return self._t
        self._save = save
        if load:
            try:
                prior = load() or {}
                with self.lock:
                    for lane, v in prior.items():
                        if isinstance(v, dict) and "n" in v:
                            t = self.totals.setdefault(lane, {"n": 0, "secs": 0.0})
                            t["n"] += int(v.get("n") or 0)
                            t["secs"] = round(t["secs"] + float(v.get("secs") or 0.0), 2)
            except Exception as exc:
                self.err = "load: %s" % str(exc)[:60]
        self._t = threading.Thread(target=self._run, args=(holder_fn,),
                                   name="leasewatch", daemon=True)
        self._t.start()
        return self._t

    def persist(self):
        if not self._save:
            return
        try:
            with self.lock:
                snap = {k: dict(v) for k, v in self.totals.items()}
            self._save(snap)
        except Exception as exc:
            self.err = "save: %s" % str(exc)[:60]

    def duty(self):
        """Busy fraction over the last hour, and over the last five minutes.

        Both, because they answer different doubts: the hour says whether the device is
        earning its keep, the five minutes says whether it is working right now.
        """
        now_m = int(time.time() // 60)

        def frac(span):
            busy = tot = 0
            for m, v in self.buckets.items():        # v is [busy, total, peak_queue]
                if now_m - m < span:
                    busy += v[0]
                    tot += v[1]
            return round(100.0 * busy / tot, 1) if tot else None

        peak5 = 0
        for m, v in self.buckets.items():
            if now_m - m < 5 and len(v) > 2:
                peak5 = max(peak5, v[2])
        ring = list(self._ring)
        live = round(100.0 * sum(ring) / len(ring), 1) if ring else 0.0
        return {"hour": frac(60), "recent": frac(5), "live": live,
                "minutes": min(len(self.buckets), 60), "peak5": peak5}

    def snapshot(self, n=24, since=None):
        """A view of the device. `since` returns only completions newer than that seq,
        which is what the stream uses so a client is never re-sent what it already has."""
        with self.lock:
            rec = list(self.recent)
            if since is not None:
                rec = [r for r in rec if r.get("seq", 0) > since]
            return {"now": self.now, "queue": self.queue, "version": self.version,
                    "seq": self.seq, "hz": round(1.0 / INTERVAL_S, 1),
                    "duty": self.duty(),
                    "recent": rec[-n:][::-1],
                    "totals": {k: dict(v) for k, v in self.totals.items()},
                    "error": self.err}


WATCH = Watch()


def start(holder_fn, load=None, save=None):
    return WATCH.start(holder_fn, load, save)


def persist():
    WATCH.persist()


def get(n=24, since=None):
    return WATCH.snapshot(n, since)


def version():
    return WATCH.version


if __name__ == "__main__":
    # A fake holder that goes busy and idle, so the state machine is checked without
    # needing the device: one hold must produce exactly one completion, with its lane.
    seq = [{"held": False}, {"held": True, "owner": "pipeline", "why": "pipeline image #7"},
           {"held": True, "owner": "pipeline", "why": "pipeline image #7"},
           {"held": False}, {"held": True, "owner": "server", "why": "server rerank"},
           {"held": False}]
    w = Watch()
    for s in seq:
        w._sample(s)
        time.sleep(0.01)
    snap = w.snapshot()
    assert len(snap["recent"]) == 2, snap["recent"]
    assert snap["recent"][0]["lane"] == "search", snap["recent"]
    assert snap["recent"][1]["lane"] == "image", snap["recent"]
    assert snap["totals"]["image"]["n"] == 1 and snap["totals"]["search"]["n"] == 1
    assert snap["now"] is None
    assert lane_of("WRITING DESK REPORT #1755") == "story"
    assert lane_of("chorus comparing accounts") == "compare"
    assert lane_of("something new") == "other"
    print("leasewatch self-check OK: %d completions, lanes %s"
          % (len(snap["recent"]), sorted(snap["totals"])))

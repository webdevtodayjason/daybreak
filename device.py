"""One answer to "where is the device", and one lock directory, for every unit.

Two answers is a silent failure. OneLane keys its lock file on a hash of the host
string, so two spellings of the same device produce two different lock files that
coordinate nobody -- no error, no warning, every acquire instant and uncontended.

This tree had two ways to get there:

  * The defaults disagreed outright. audio.py and devtherm.py fell back to
    172.17.7.x while enrich.py, server.py and pipeline.py fell back to
    192.168.1.50. Production sets TIINY_HOST so they agreed in practice, but
    daybreak-camera.service has no EnvironmentFile, so it is exactly the unit that
    would quietly stop coordinating.
  * The spellings disagreed. enrich.py normalised a pasted URL down to a bare host;
    audio.py did "http://%s:8800" % host on whatever it was given. Feed both
    TIINY_HOST=http://x:8800 and they produce different strings.

Everything imports host() from here so there is one spelling and one default.

The lock directory is the same class of problem wearing different clothes. All three
daybreak units run PrivateTmp=yes, which gives each its own mount namespace for /tmp
(measured: mnt:[4026533314], mnt:[4026533312], and the host's mnt:[4026531832]).
OneLane defaults its lock dir to /tmp, so the naive thing would have been a no-op
between exactly the two processes that need coordinating. We set ONELANE_DIR here
rather than relying on the env file, because a unit that starts without the env file
is precisely the one that must not silently opt out.
"""

import contextlib
import os
import socket
import sys
import threading
import time

# Falls back to the production device. Every module used to carry its own default and
# two of them carried a different one.
DEFAULT_HOST = "192.168.1.50"
PORT = 8800

# Outside /tmp (namespaced per unit) and outside any home directory (ProtectHome=yes
# blocks those). Set before anything imports onelane, so a unit lacking the env file
# still lands in the shared directory instead of its own private /tmp.
LOCK_DIR = os.environ.get("ONELANE_DIR") or "/var/lib/daybreak/locks"
os.environ["ONELANE_DIR"] = LOCK_DIR


def host():
    """The bare host: no scheme, no port. This is the string onelane keys on."""
    raw = (os.environ.get("TIINY_HOST") or DEFAULT_HOST).strip()
    if "//" in raw:
        raw = raw.split("//", 1)[1]
    return raw.strip("/ ").split("/")[0].split(":")[0]


def base_url():
    return "http://%s:%d" % (host(), PORT)


def key():
    """Empty is allowed here and must fail loudly at call time, not at import."""
    return os.environ.get("TIINY_KEY", "")


def prove_shared(unit, quiet=False):
    """Positive proof that this unit shares the lock directory with the others.

    Nothing observable from inside one process can prove a directory IS shared --
    onelane's own check can only spot known ways it is not. Two processes seeing each
    other's file is the only real evidence, so each unit drops a marker and reports
    which peers it can see. Run it at startup: on a correctly configured box the second
    unit to boot names the first, and a permanent solo reading is the symptom of exactly
    the namespace bug this directory exists to avoid.

    Returns the list of peer units visible. Never raises -- a monitor that can take the
    process down is worse than the condition it monitors.
    """
    seen = []
    try:
        os.makedirs(LOCK_DIR, exist_ok=True)
        mark = os.path.join(LOCK_DIR, ".seen-" + unit)
        with open(mark, "w") as fh:
            fh.write("%s %d %.0f\n" % (socket.gethostname(), os.getpid(), time.time()))
        os.chmod(mark, 0o664)
        for name in os.listdir(LOCK_DIR):
            if name.startswith(".seen-") and name != ".seen-" + unit:
                seen.append(name[len(".seen-"):])
    except OSError as e:
        if not quiet:
            print("[device] WARNING cannot write lock dir %s: %s" % (LOCK_DIR, e))
        return seen
    if not quiet:
        print("[device] lock dir %s; peers visible: %s"
              % (LOCK_DIR, ", ".join(sorted(seen)) or "none yet"))
    return seen


# ---------------------------------------------------------------------------------
# The lease. One inference at a time is a property of the hardware, and daybreak is
# roughly ten concurrent device-calling threads across two processes: six daemon
# threads in the pipeline (four of which touch the device) and a ThreadingHTTPServer
# that spawns a thread per request plus a story thread per article. The sqlite
# timestamp flags this replaces are advisory -- every one of those threads has to
# remember to honour them, and there is a real window between reading a timestamp and
# issuing the call. flock has no such window and the kernel releases it if we die.
# ---------------------------------------------------------------------------------

# Long enough to clear a whole sectioned desk report plus retry backoff. A single
# device request cannot exceed the gateway's ~220s hard close, but a story is three
# of them back to back inside one hold, so waiting must be bounded well above that.
LEASE_WAIT = 600.0

# Names the holder in who(), which the board renders. "server" or "pipeline".
UNIT = os.environ.get("DAYBREAK_UNIT") or \
    os.path.splitext(os.path.basename(sys.argv[0] or "daybreak"))[0]

try:
    from onelane import DeviceBusy
except Exception:                          # library absent: nothing can raise it
    class DeviceBusy(RuntimeError):
        pass

_ts = None
_ts_lock = threading.Lock()


def onelane():
    """The one OneLane for this process, or None if the library is missing.

    One object per process matters: onelane keeps a registry keyed on the lock path,
    so all ten threads share a single lock object. Intra-process contention resolves on
    an in-memory RLock with no filesystem traffic, and only genuine cross-process
    contention reaches flock.
    """
    global _ts
    if _ts is None:
        with _ts_lock:
            if _ts is None:
                import onelane as _t
                _ts = _t.OneLane(host=host(), key=key(), owner=UNIT)
    return _ts


@contextlib.contextmanager
def lease(why, wait=LEASE_WAIT):
    """Hold the device for the duration. Nests safely.

    A story thread already holding the lease and calling down into enrich() nests to
    depth 2 rather than deadlocking, because both resolve to the same lock object.
    """
    with onelane().hold(why=why, wait=wait):
        yield


def holder():
    """Who holds the device right now, without taking the lock. Safe to poll."""
    import onelane as _t
    return _t.who(host=host())


if __name__ == "__main__":
    assert host() == "192.168.1.50" or os.environ.get("TIINY_HOST"), host()
    for spelling in ("1.2.3.4", "http://1.2.3.4", "http://1.2.3.4:8800", "1.2.3.4:8800/"):
        os.environ["TIINY_HOST"] = spelling
        assert host() == "1.2.3.4", (spelling, host())
    del os.environ["TIINY_HOST"]
    assert host() == DEFAULT_HOST and base_url() == "http://%s:8800" % DEFAULT_HOST
    assert os.environ["ONELANE_DIR"] == LOCK_DIR
    print("device.py self-check OK -> %s, locks in %s" % (base_url(), LOCK_DIR))

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
import json
import os
import socket
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import time

# Falls back to the production device. Every module used to carry its own default and
# two of them carried a different one.
DEFAULT_HOST = "192.168.1.50"

# Where `farm device` writes what it was told. Somebody who installed Daybreak from
# tiinyapp.farm has already answered "where is the Tiiny and what is the key" once,
# to the farm, and should not have to answer it again to us. The farm also exports
# TIINY_BASE and TIINY_KEY when it launches an app, so under `farm start` the
# environment already carries it; this file is the answer for a hand start.
FARM_DEVICE = os.path.join(os.path.expanduser("~"), ".tiinyapps", "device.json")


def farm_device():
    """{"base", "key"} as `farm device` wrote it, or an empty dict.

    Never raises and never logs: a missing file is the normal case on a box that
    has no farm, and the key inside is nobody's business but the device's.
    """
    try:
        with open(FARM_DEVICE, "rb") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else {}
    except Exception:            # noqa: BLE001 - absent, unreadable or not JSON
        return {}


def _split_base(base):
    """A base URL or a bare address as (host, explicit port or None)."""
    base = (base or "").strip()
    if not base:
        return None, None
    if "//" not in base:
        base = "http://" + base
    try:
        parts = urllib.parse.urlsplit(base)
        return (parts.hostname or None), parts.port
    except ValueError:
        return None, None


def given():
    """(host, explicit port or None, who said so), or (None, None, "").

    In order: the environment the operator set, the environment the farm sets when
    it launches an app, and the file `farm device` wrote. An empty answer is the
    honest one on a box with no Tiiny, and it is what configured() reports.
    """
    for value, source_name in ((os.environ.get("TIINY_HOST"), "TIINY_HOST"),
                               (os.environ.get("TIINY_BASE"), "TIINY_BASE"),
                               (farm_device().get("base"), FARM_DEVICE)):
        found, found_port = _split_base(value)
        if found:
            return found, found_port, source_name
    return None, None, ""


def configured():
    """Is there a device to talk to at all: an address and a key."""
    return bool(given()[0]) and bool(key())


def source():
    """Where the address came from, for a log line. Never includes the key."""
    return given()[2] or "the built-in default"


def _gateway_port(host, timeout=2.0):
    """Which port serves the AI gateway on this device.

    Firmware 1.0.0 moved it. The gateway now binds 172.17.0.1:8800, the docker
    bridge only, and serves the same surface on port 80. Older firmware keeps it
    on 8800 and uses 80 for device management, so a plain TCP probe cannot tell
    the two apart - port 80 answers on both. Asking for an AI route can: the
    firmware that does not serve it 404s.

    TIINY_PORT overrides, for anyone who has put it somewhere else.
    """
    env = os.environ.get("TIINY_PORT")
    if env:
        return int(env)
    for port in (80, 8800):
        try:
            req = urllib.request.Request(
                "http://%s:%d/v1/models" % (host, port),
                headers={"Authorization": "Bearer probe"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                if r.status != 404:
                    return port
        except urllib.error.HTTPError as exc:
            if exc.code != 404:      # 401 still means the gateway is here
                return port
        except Exception:            # noqa: BLE001 - unreachable; try the next
            continue
    return 80


_PORT_CACHE = {}


def port():
    """Gateway port: the override, then the one we were given, then a probe.

    The probe costs two connection timeouts, and it is imported at module level by
    server.py, so it must not run when there is nothing to ask. A box with no Tiiny
    configured gets the answer immediately and the wall comes up in milliseconds.
    """
    override = os.environ.get("TIINY_PORT")
    if override and override.strip().isdigit():
        return int(override)
    found, found_port, _ = given()
    if found_port:
        return found_port
    if not found:
        return 80
    if found not in _PORT_CACHE:
        _PORT_CACHE[found] = _gateway_port(found)
    return _PORT_CACHE[found]


# Kept for callers that import it directly. Prefer port().
PORT = 8800

def _lock_dir():
    """One directory every unit can see, chosen without needing root to exist.

    On the Pi that is /var/lib/daybreak/locks: outside /tmp (namespaced per unit
    by PrivateTmp=yes) and outside any home directory (ProtectHome=yes blocks
    those), and install.sh creates it. Off the Pi there is no installer and
    /var/lib is not ours to write, so fall back to one directory under this
    user's home. Same property that matters, one spelling per user, and it beats
    silently landing somewhere unwritable and coordinating nobody.
    """
    told = os.environ.get("ONELANE_DIR")
    if told:
        return told
    shared = "/var/lib/daybreak/locks"
    if os.path.isdir(shared) or os.access("/var/lib", os.W_OK):
        return shared
    return os.path.join(os.path.expanduser("~"), ".daybreak", "locks")


# Set before anything imports onelane, so a unit lacking the env file still lands in
# the shared directory instead of its own private /tmp.
LOCK_DIR = _lock_dir()
os.environ["ONELANE_DIR"] = LOCK_DIR


def host():
    """The bare host: no scheme, no port. This is the string onelane keys on."""
    return given()[0] or DEFAULT_HOST


def base_url():
    return "http://%s:%d" % (host(), port())


def account_auth_key(addr, serial, password):
    """The device's own static API key, straight from the box, no TiinyOS.

    POST /api/v1/account/auth (password + serial), Host: auth.api.tiiny,
    unlocks /data and hands back `auth_key` -- the same 36-char UUID key()
    below looks for. Confirmed 2026-09-24 against a live device. Full
    writeup: ~/code/tiiny/tools/README-unlock.md.

    Deliberately NOT wired into key(): this server runs unattended (a
    reboot, a Coolify redeploy) with nothing to answer a password prompt,
    and key() itself is documented to never block or print. This is a
    plain capability for an operator's own one-off use -- get a key once,
    same way tiiny-unlock.py does, and put it in TIINY_KEY or the farm
    device file, which key() already reads. Returns "" on any failure.
    """
    body = json.dumps({"password": password, "device_id": serial}).encode()
    req = urllib.request.Request(
        "http://%s/api/v1/account/auth" % addr, data=body, method="POST",
        headers={"Content-Type": "application/json", "Host": "auth.api.tiiny",
                 "x-device-id": serial, "accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            out = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
            ValueError, OSError):
        return ""
    return (out.get("auth_key") or "").strip()


def key():
    """Empty is allowed here and must fail loudly at call time, not at import.

    TIINY_KEY first, then whatever `farm device` was told. Nothing anywhere in this
    tree prints the return value.
    """
    return (os.environ.get("TIINY_KEY") or farm_device().get("key") or "").strip()


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

class LocalLane:
    """The lease when onelane is not installed. One process only, and honest about it.

    The README has always said Daybreak runs without onelane and simply cannot
    coordinate with other apps. It did not: every device call goes through lease(),
    lease() went straight to `import onelane`, and a tree without onelane.py beside it
    raised ModuleNotFoundError on the first article. The supervised loop caught it,
    called the device unreachable and backed off, so a clean clone collected news
    forever and analysed none of it. An install from tiinyapp.farm is exactly that
    clone.

    A reentrant lock is the honest stand-in. It serialises every thread in this
    process, which is the whole story for `daybreak --serve`, where the wall and the
    pipeline are one process and the ten device-calling threads are all in it. It
    cannot see a second process, so the Pi's two units still want the real library,
    and install.sh still puts it there. The record who() returns has the same shape
    the board reads, with queue counted from the threads actually waiting.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._state = threading.Lock()
        self._depth = 0
        self._why = None
        self._since = 0.0
        self._waiting = 0

    @contextlib.contextmanager
    def hold(self, why=None, wait=None):
        with self._state:
            self._waiting += 1
        try:
            got = (self._lock.acquire(timeout=wait) if wait and wait > 0
                   else self._lock.acquire())
        finally:
            with self._state:
                self._waiting -= 1
        if not got:
            raise DeviceBusy("waited %.0fs for the device and it is still busy"
                             % (wait or 0))
        with self._state:
            self._depth += 1
            previous_why, previous_since = self._why, self._since
            if self._depth == 1:
                self._since = time.time()
            self._why = why
        try:
            yield
        finally:
            with self._state:
                self._depth -= 1
                self._why, self._since = previous_why, previous_since
            self._lock.release()

    def who(self, **_ignored):
        """Read without taking the lock, so a monitor never delays an inference."""
        with self._state:
            held = self._depth > 0
            return {"held": held, "owner": UNIT if held else None,
                    "why": self._why if held else None,
                    "since": self._since if held else None,
                    "held_for_s": round(time.time() - self._since, 3) if held else 0.0,
                    "queue": self._waiting, "local": True}


_ts = None
_ts_lock = threading.Lock()


def onelane():
    """The one lane for this process: the real OneLane, or the local stand-in.

    One object per process matters: onelane keeps a registry keyed on the lock path,
    so all ten threads share a single lock object. Intra-process contention resolves on
    an in-memory RLock with no filesystem traffic, and only genuine cross-process
    contention reaches flock. The stand-in has the same property and stops there.
    """
    global _ts
    if _ts is None:
        with _ts_lock:
            if _ts is None:
                try:
                    import onelane as _t
                    _ts = _t.OneLane(host=host(), key=key(), owner=UNIT)
                except Exception:      # noqa: BLE001 - absent, or refuses to build
                    _ts = LocalLane()
    return _ts


def coordinated():
    """True when the real library is in use and other processes can be seen."""
    return not isinstance(onelane(), LocalLane)


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
    lane = onelane()
    if isinstance(lane, LocalLane):
        return lane.who()
    import onelane as _t
    return _t.who(host=host())


if __name__ == "__main__":
    import tempfile

    # This box may well have a real ~/.tiinyapps/device.json, and the assertions below
    # are about what happens when nothing says where the device is. Point the constant
    # at a directory that has no such file, and put one back when we want to read it.
    _real_farm_device, _scratch = FARM_DEVICE, tempfile.mkdtemp(prefix="daybreak-dev-")
    FARM_DEVICE = os.path.join(_scratch, "device.json")
    for _leftover in ("TIINY_HOST", "TIINY_BASE", "TIINY_KEY", "TIINY_PORT"):
        os.environ.pop(_leftover, None)

    assert host() == DEFAULT_HOST, host()
    assert not configured(), "no address and no key is not configured"
    for spelling in ("1.2.3.4", "http://1.2.3.4", "http://1.2.3.4:8800", "1.2.3.4:8800/"):
        os.environ["TIINY_HOST"] = spelling
        assert host() == "1.2.3.4", (spelling, host())
    del os.environ["TIINY_HOST"]
    # Pin the port for the test: the probe needs a live device, and the
    # default host here is not one. Check the override works both ways.
    for p in ("8800", "80"):
        os.environ["TIINY_PORT"] = p
        _PORT_CACHE.clear()
        assert base_url() == "http://%s:%s" % (DEFAULT_HOST, p), base_url()
    del os.environ["TIINY_PORT"]
    _PORT_CACHE.clear()
    assert host() == DEFAULT_HOST
    assert os.environ["ONELANE_DIR"] == LOCK_DIR

    # The farm's file answers when the environment does not, and the environment
    # still wins over it. The key is compared, never printed.
    with open(FARM_DEVICE, "w") as fh:
        json.dump({"base": "http://10.0.0.9:8800", "key": "farm-written-key"}, fh)
    assert host() == "10.0.0.9", host()
    assert port() == 8800, port()
    assert base_url() == "http://10.0.0.9:8800", base_url()
    assert key() == "farm-written-key"
    assert configured(), "an address and a key is configured"
    assert source() == FARM_DEVICE, source()
    os.environ["TIINY_HOST"] = "10.0.0.10"
    os.environ["TIINY_KEY"] = "env-key"
    assert host() == "10.0.0.10" and key() == "env-key", (host(), source())
    assert source() == "TIINY_HOST", source()
    del os.environ["TIINY_HOST"], os.environ["TIINY_KEY"]

    # A file that is not there, not readable or not JSON is the same as no file.
    os.remove(FARM_DEVICE)
    assert farm_device() == {} and host() == DEFAULT_HOST
    with open(FARM_DEVICE, "w") as fh:
        fh.write("this is not json")
    assert farm_device() == {} and not configured()
    os.remove(FARM_DEVICE)
    os.rmdir(_scratch)
    FARM_DEVICE = _real_farm_device

    # The lease has to work with no onelane installed, because that is what an install
    # from tiinyapp.farm looks like. Nesting to depth 2 is the case that matters: a
    # story thread holds the lease and calls down into enrich(), which takes it again.
    _lane = LocalLane()
    assert _lane.who()["held"] is False
    with _lane.hold(why="outer"):
        assert _lane.who()["held"] is True and _lane.who()["why"] == "outer"
        with _lane.hold(why="inner"):
            assert _lane.who()["why"] == "inner", _lane.who()
        assert _lane.who()["why"] == "outer", _lane.who()
    assert _lane.who()["held"] is False and _lane.who()["queue"] == 0

    # account_auth_key(): parses what the device actually sends, against a
    # tiny stub, same as the check in tools/tiiny-unlock.py.
    import threading as _threading
    from http.server import BaseHTTPRequestHandler as _BHRH, ThreadingHTTPServer as _THS

    class _Handler(_BHRH):
        reply = {"status": "ok", "auth_key": "the-real-key"}
        status = 200

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            body = json.dumps(self.reply).encode()
            self.send_response(self.status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    _srv = _THS(("127.0.0.1", 0), _Handler)
    _addr = "127.0.0.1:%d" % _srv.server_port
    _threading.Thread(target=_srv.serve_forever, daemon=True).start()
    assert account_auth_key(_addr, "TNYM000", "x") == "the-real-key"
    _Handler.reply, _Handler.status = {"error": "main_password_wrong"}, 403
    assert account_auth_key(_addr, "TNYM000", "wrong") == ""
    assert account_auth_key("127.0.0.1:1", "TNYM000", "x") == ""
    _srv.shutdown()

    print("device.py self-check OK -> %s, locks in %s, lane %s"
          % (base_url(), LOCK_DIR, "onelane" if coordinated() else "local (no onelane)"))

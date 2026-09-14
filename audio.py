#!/usr/bin/env python3
"""DAYBREAK audio briefing: turn the daily text brief into a podcast episode.

Every word is spoken by the Tiiny Pocket itself — Qwen3-TTS-12Hz-1.7B-CustomVoice
on the device NPU. Nothing is sent to a cloud TTS service.

Pipeline, deliberately the same shape as a proven one (Evy's Nook), minus the
cloud dependency:

    docs.brief (Ornith)  ->  spoken-form cleanup  ->  chunk  ->  device TTS
                         ->  ffmpeg concat + loudnorm  ->  episodes/YYYY-MM-DD.mp3
                         ->  RSS 2.0 + iTunes feed

Two things learned the hard way and encoded here:
  * The CustomVoice model takes NO `voice` parameter. Its sibling
    (…-1.7B-Base) requires one and rejects all 35 names we could find,
    including the value in Tiiny's own docs example — see bug #019. Use
    CustomVoice; do not "improve" this by adding a voice field.
  * TTS is NOT resident. It costs 7 NPU units on top of the three permanent
    models (83) = 90/100, so it is started before a run and stopped after,
    and the whole run takes the device lease like every other job.

Loudness target is EBU R128 (I=-18 LUFS, TP=-2 dBTP), the podcast convention,
so the brief sits at the same volume as commercial shows in a car.
"""

import html
import json
import os
import re
import subprocess
import tempfile
import time
import urllib.request

import device                       # one spelling of the device host

TTS_MODEL = "Qwen/Qwen3-TTS-12Hz-1.7B-CustomVoice"
TTS_NPU_UNITS = 7
CHUNK_CHARS = 1200          # device latency scales with text; small chunks fail less
TTS_TIMEOUT = 300.0
LOAD_TIMEOUT = 120.0
LOUDNORM = "loudnorm=I=-18:TP=-2:LRA=11"

SITE_TITLE = "DAYBREAK Daily Brief"
SITE_LINK = "https://daybreak.example.com"
SITE_DESC = ("A daily world-news intelligence briefing, written and spoken entirely "
             "on a Tiiny Pocket edge device. Narrated by Tiiny, the device's own voice. "
             "No cloud AI is used at any stage.")


# --------------------------------------------------------------------------- #
# text -> speakable
# --------------------------------------------------------------------------- #

_ABBR = [
    (r"\bS([1-5])\b", r"severity \1"),
    (r"\bNORTHCOM\b", "NORTHCOM"), (r"\bEUCOM\b", "EUCOM"),
    (r"\bCENTCOM\b", "CENTCOM"), (r"\bAFRICOM\b", "AFRICOM"),
    (r"\bINDOPACOM\b", "INDO-PACOM"), (r"\bSOUTHCOM\b", "SOUTHCOM"),
    (r"\bUAV\b", "drone"), (r"\bIED\b", "roadside bomb"),
    (r"\bM(\d+\.\d+)\b", r"magnitude \1"),
    (r"\bNWS\b", "National Weather Service"),
    (r"\bCISA\b", "the Cybersecurity and Infrastructure Security Agency"),
]


def speakable(text):
    """Strip anything that would be read aloud as punctuation noise."""
    t = text or ""
    t = re.sub(r"!\[[^\]]*\]\([^)]*\)", " ", t)          # images
    t = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", t)       # links -> label
    t = re.sub(r"[*_`#>]+", " ", t)                      # markdown ornaments
    t = re.sub(r"^\s*[-•]\s*", "", t, flags=re.M)        # bullets
    t = re.sub(r"https?://\S+", "", t)                   # bare urls
    for pat, rep in _ABBR:
        t = re.sub(pat, rep, t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def chunk(text, max_chars=CHUNK_CHARS):
    """Split on sentence boundaries so no chunk ends mid-thought."""
    out, cur = [], ""
    for sent in re.split(r"(?<=[.!?])\s+", text):
        if not sent:
            continue
        if len(cur) + len(sent) + 1 > max_chars and cur:
            out.append(cur.strip())
            cur = sent
        else:
            cur = sent if not cur else cur + " " + sent
    if cur.strip():
        out.append(cur.strip())
    return out


# --------------------------------------------------------------------------- #
# device TTS
# --------------------------------------------------------------------------- #

class Speech:
    """Start/stop the TTS model and synthesize chunks on the device."""

    def __init__(self, host=None, key=None):
        # device.host() normalises; the old default here was a DIFFERENT address
        # from the one enrich.py used, which is a silently unshared lock waiting
        # for any unit that starts without /etc/daybreak.env.
        self.base = "http://%s:%d" % (host or device.host(), device.port())
        self._key = key or None
        self._started = False

    @property
    def key(self):
        """Live, not snapshotted: same reason as enrich.Tiiny.key."""
        return self._key or device.key()

    def _req(self, path, body=None, method=None, timeout=30.0, raw=False):
        # Same split as enrich.py: speech synthesis occupies the NPU and takes the
        # lease, status reads do not and must not queue behind it.
        if "status" in path:
            return self._raw(path, body, method, timeout, raw)
        with device.lease("%s tts" % device.UNIT):
            return self._raw(path, body, method, timeout, raw)

    def _raw(self, path, body=None, method=None, timeout=30.0, raw=False):
        url = self.base + path
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": "Bearer " + self.key}
        if data:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            blob = resp.read()
        return blob if raw else json.loads(blob or b"{}")

    def roster(self):
        try:
            return self._req("/api/v1/models/npu/status", timeout=8.0)
        except Exception:
            return {}

    def running(self):
        for m in (self.roster().get("models") or []):
            if m.get("model_id") == TTS_MODEL and m.get("status") == "running":
                return True
        return False

    def start(self):
        """Load TTS. Returns True once running. Idempotent."""
        if self.running():
            self._started = False       # someone else owns it; don't stop it later
            return True
        try:
            self._req("/api/v1/models/%s/start" % TTS_MODEL.replace("/", "%2F"),
                      body={}, method="POST", timeout=30.0)
        except Exception as exc:
            return False, "start failed: %s" % str(exc)[:120]
        deadline = time.time() + LOAD_TIMEOUT
        while time.time() < deadline:
            if self.running():
                self._started = True
                return True
            time.sleep(4)
        return False

    def stop(self):
        """Free the NPU units — but only if we were the one who loaded it."""
        if not self._started:
            return
        try:
            self._req("/api/v1/models/%s/stop" % TTS_MODEL.replace("/", "%2F"),
                      body={}, method="POST", timeout=30.0)
        except Exception:
            pass
        self._started = False

    def say(self, text, fmt="mp3"):
        """One chunk -> audio bytes. NOTE: no `voice` field; see module docstring."""
        try:
            blob = self._req("/v1/audio/speech",
                             body={"model": TTS_MODEL, "input": text,
                                   "response_format": fmt},
                             method="POST", timeout=TTS_TIMEOUT, raw=True)
        except Exception as exc:
            return None, str(exc)[:160]
        if not blob or blob[:1] == b"{":
            try:
                err = json.loads(blob)
                return None, "device: %s" % str(err.get("error"))[:120]
            except Exception:
                return None, "empty response"
        return blob, None


# --------------------------------------------------------------------------- #
# assembly
# --------------------------------------------------------------------------- #

def _ffmpeg(args, timeout=300):
    try:
        p = subprocess.run(["ffmpeg", "-y", "-loglevel", "error"] + args,
                           capture_output=True, timeout=timeout)
        return p.returncode == 0, (p.stderr or b"").decode()[:200]
    except FileNotFoundError:
        return False, "ffmpeg not installed"
    except subprocess.TimeoutExpired:
        return False, "ffmpeg timeout"


def _dev_ms(ms):
    """Speech is real NPU work that emits no tokens; record the time."""
    try:
        import enrich
        enrich.count_device_ms(ms)
    except Exception:
        pass


def render_episode(text, out_path, speech=None, log=print):
    """Full text -> a single normalised mp3. Returns (path, seconds) or (None, err)."""
    _t0 = time.time()
    sp = speech or Speech()
    body = speakable(text)
    if len(body) < 40:
        return None, "brief too short to narrate"
    parts = chunk(body)
    log("[audio] %d chars -> %d chunks" % (len(body), len(parts)))

    ok = sp.start()
    if ok is not True:
        return None, (ok[1] if isinstance(ok, tuple) else "TTS model would not start")

    tmpdir = tempfile.mkdtemp(prefix="wb-audio-")
    pieces = []
    try:
        for i, part in enumerate(parts, 1):
            blob, err = sp.say(part)
            if err:
                log("[audio] chunk %d/%d failed: %s" % (i, len(parts), err))
                continue                      # a lost sentence beats a lost episode
            f = os.path.join(tmpdir, "p%03d.mp3" % i)
            with open(f, "wb") as fh:
                fh.write(blob)
            pieces.append(f)
            log("[audio] chunk %d/%d ok (%d KB)" % (i, len(parts), len(blob) // 1024))
        if not pieces:
            return None, "every chunk failed"

        listing = os.path.join(tmpdir, "list.txt")
        with open(listing, "w") as fh:
            for f in pieces:
                fh.write("file '%s'\n" % f.replace("'", "'\\''"))
        joined = os.path.join(tmpdir, "joined.mp3")
        ok2, err2 = _ffmpeg(["-f", "concat", "-safe", "0", "-i", listing, "-c", "copy", joined])
        if not ok2:                            # re-encode if stream copy refuses
            ok2, err2 = _ffmpeg(["-f", "concat", "-safe", "0", "-i", listing,
                                 "-c:a", "libmp3lame", "-b:a", "128k", joined])
        if not ok2:
            return None, "concat failed: %s" % err2

        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        ok3, err3 = _ffmpeg(["-i", joined, "-af", LOUDNORM,
                             "-c:a", "libmp3lame", "-b:a", "128k", "-ar", "44100",
                             "-metadata", "artist=Tiiny",
                             "-metadata", "comment=Narrated by Tiiny, on-device",
                             "-metadata", "album=%s" % SITE_TITLE,
                             out_path])
        if not ok3:
            return None, "normalise failed: %s" % err3
        _dev_ms((time.time() - _t0) * 1000.0)
        return out_path, _duration(out_path)
    finally:
        sp.stop()
        for f in pieces:
            try:
                os.unlink(f)
            except OSError:
                pass
        try:
            os.unlink(os.path.join(tmpdir, "list.txt"))
            os.unlink(os.path.join(tmpdir, "joined.mp3"))
            os.rmdir(tmpdir)
        except OSError:
            pass


def _duration(path):
    try:
        p = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "default=nw=1:nk=1", path],
                           capture_output=True, timeout=30)
        return float((p.stdout or b"0").decode().strip() or 0)
    except Exception:
        return 0.0


# --------------------------------------------------------------------------- #
# podcast feed
# --------------------------------------------------------------------------- #

def _rfc2822(ts):
    return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime(ts))


def build_feed(episodes, public_base):
    """RSS 2.0 + iTunes. `episodes`: [{day,title,desc,url,bytes,seconds,ts}] newest first."""
    e = html.escape
    items = []
    for ep in episodes:
        mins, secs = divmod(int(ep.get("seconds") or 0), 60)
        items.append(
            "  <item>\n"
            "    <title>%s</title>\n"
            "    <description>%s</description>\n"
            "    <pubDate>%s</pubDate>\n"
            "    <guid isPermaLink=\"false\">daybreak-%s</guid>\n"
            "    <enclosure url=\"%s\" length=\"%d\" type=\"audio/mpeg\"/>\n"
            "    <itunes:duration>%d:%02d</itunes:duration>\n"
            "    <itunes:explicit>false</itunes:explicit>\n"
            "  </item>"
            % (e(ep.get("title", "")), e(ep.get("desc", "")), _rfc2822(ep.get("ts", time.time())),
               e(ep.get("day", "")), e(ep.get("url", "")), int(ep.get("bytes") or 0), mins, secs))
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">\n'
        '<channel>\n'
        '  <title>%s</title>\n'
        '  <link>%s</link>\n'
        '  <description>%s</description>\n'
        '  <language>en-us</language>\n'
        '  <lastBuildDate>%s</lastBuildDate>\n'
        '  <itunes:author>Tiiny</itunes:author>\n'
        '  <itunes:summary>%s</itunes:summary>\n'
        '  <itunes:explicit>false</itunes:explicit>\n'
        '  <itunes:category text="News"/>\n'
        '%s\n'
        '</channel>\n</rss>\n'
        % (e(SITE_TITLE), e(public_base or SITE_LINK), e(SITE_DESC),
           _rfc2822(time.time()), e(SITE_DESC), "\n".join(items)))


if __name__ == "__main__":
    import sys
    text = ("Daybreak daily brief. Russia struck a shopping mall in Kryvyi Rih, killing at least "
            "sixteen people. A magnitude 7.4 earthquake hit Colombia, affecting fifteen departments. "
            "In cyber, CISA added three exploited vulnerabilities to its catalog.")
    out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/daybreak-brief-test.mp3"
    sp = Speech()
    print("device:", sp.base, "| TTS running:", sp.running())
    path, res = render_episode(text, out, sp)
    if path:
        print("OK ->", path, "%.1f s" % res, os.path.getsize(path), "bytes")
    else:
        print("FAILED:", res)

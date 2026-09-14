#!/usr/bin/env python3
"""Build the release archive that tiinyapp.farm lists, and check it before it ships.

    python3 scripts/release.py

Writes dist/daybreak-<version>.tar.gz with a daybreak-<version>/ root, prints its
SHA-256 and exact byte size, and refuses to write anything the farm would reject.

Three decisions live here rather than in a comment somewhere.

**The contents are a list, not a filter.** An exclude list fails open: the day
somebody adds a module that shells out, it ships and the farm's scanner catches it
after the tag is cut. This names what goes in, and a file that is not on the list is
not in the archive, which fails closed and is checkable by reading it.

**Three modules are deliberately left out.** `audio.py` and `devtherm.py` import
subprocess, for ffmpeg and for ssh, and the farm forbids shell access outright:
there is no permission that buys it. `r2.py` is the offsite sync to Cloudflare R2,
which is inert without four R2_* variables that a farm install does not have.
Every importer of all three already treats them as optional, and the selfcheck at
the bottom of this script proves the wall comes up without them. The repository
keeps all three; a clone still records audio, reads thermals and syncs offsite.

**No AppleDouble files.** macOS writes a `._name` sidecar for a file with extended
attributes when the system tar copies it, and the farm counts those as duplicate
archive entries. COPYFILE_DISABLE is set below for anyone who swaps a shell tar in
here, and the member list is checked for the pattern either way.
"""

import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

os.environ["COPYFILE_DISABLE"] = "1"

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from version import VERSION  # noqa: E402

# Everything the wall and the pipeline need to start, and nothing else.
CONTENTS = [
    "daybreak",
    "LICENSE",
    "README.md",
    "CONTRACT.md",
    "schema.sql",
    "gazetteer.json",
    "version.py",
    "db.py",
    "device.py",
    "enrich.py",
    "feeds.py",
    "jobs.py",
    "leasewatch.py",
    "pipeline.py",
    "research.py",
    "server.py",
    "static/index.html",
    "static/og.png",
    "static/places.json",
    "static/world.json",
]

# Left out on purpose; the module docstring says why. Named here so the check below
# can prove they really are absent rather than trusting the list above.
OMITTED = ["audio.py", "devtherm.py", "r2.py", "deploy"]

# What the farm's scanner refuses outright, whatever permissions an app declares.
REFUSED = ("import subprocess", "from subprocess", "import ctypes", "from ctypes",
           "__import__(", "os.system(", "os.popen(")


def stage(into):
    """Copy the contents list into a daybreak-<version>/ directory."""
    root = into / ("daybreak-" + VERSION)
    for relative in CONTENTS:
        source = ROOT / relative
        if not source.is_file():
            raise SystemExit("release: %s is on the contents list and not in the tree"
                             % relative)
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return root


def check_staged(root):
    """Everything that can be known about the archive before it is an archive."""
    problems = []
    for path in sorted(root.rglob("*")):
        if path.name.startswith("._"):
            problems.append("%s is an AppleDouble sidecar" % path.name)
        if path.suffix != ".py" and path.name != "daybreak":
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for phrase in REFUSED:
            if phrase in text:
                problems.append("%s contains %s, which the farm scanner refuses"
                                % (path.relative_to(root), phrase))
    for name in OMITTED:
        if (root / name).exists():
            problems.append("%s was meant to be left out and is in the archive" % name)
    if problems:
        for problem in problems:
            sys.stderr.write("release: %s\n" % problem)
        raise SystemExit(1)


def selfcheck(root):
    """Run the entry the manifest names, from the staged tree, the way CI will.

    Not the container run: that needs Docker and belongs in the farm's own checker.
    This is the cheap version, and it is the one that catches a module left off the
    contents list, because the staged tree is the only place that mistake shows.
    """
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    done = subprocess.run([sys.executable, "daybreak", "--serve", "--selfcheck"],
                          cwd=root, capture_output=True, text=True, timeout=120,
                          env=environment)
    sys.stdout.write("".join("    " + line + "\n"
                             for line in done.stdout.strip().splitlines()))
    if done.returncode != 0:
        sys.stderr.write(done.stderr[-2000:])
        raise SystemExit("release: the staged tree does not pass its own selfcheck")


def members():
    """Every archive path, directories included, in the order they are written."""
    names, seen = [], set()
    for relative in CONTENTS:
        parts = relative.split("/")
        for depth in range(1, len(parts)):
            directory = "/".join(parts[:depth])
            if directory not in seen:
                seen.add(directory)
                names.append(directory)
        names.append(relative)
    return names


def build(root, archive, mtime):
    """One tar.gz, from the contents list, with ownership and times normalised.

    From the LIST, not from a walk of the staged directory. The selfcheck above runs
    a Python interpreter inside that directory, and the first build of this archive
    shipped nine __pycache__ files it wrote on the way past. A walk trusts whatever
    happens to be on disk; the list is the thing that was decided.
    """

    def normalise(info):
        info.uid = info.gid = 0
        info.uname = info.gname = "root"
        info.mtime = mtime
        info.mode = 0o755 if (info.isdir() or info.mode & 0o111) else 0o644
        return info

    archive.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "w:gz", format=tarfile.GNU_FORMAT) as tar:
        tar.add(root, arcname=root.name, recursive=False, filter=normalise)
        for relative in members():
            tar.add(root / relative, arcname="%s/%s" % (root.name, relative),
                    recursive=False, filter=normalise)


def verify(archive, root_name):
    """Read the finished archive back and hold it to the contents list."""
    expected = {root_name} | {"%s/%s" % (root_name, name) for name in members()}
    with tarfile.open(archive, "r:gz") as tar:
        found = {info.name.rstrip("/") for info in tar}
    extra, missing = sorted(found - expected), sorted(expected - found)
    if extra or missing:
        for name in extra:
            sys.stderr.write("release: %s is in the archive and not on the list\n" % name)
        for name in missing:
            sys.stderr.write("release: %s is on the list and not in the archive\n" % name)
        raise SystemExit(1)
    print("  members  %d, every one of them on the contents list" % len(found))


def main():
    archive = ROOT / "dist" / ("daybreak-%s.tar.gz" % VERSION)
    with tempfile.TemporaryDirectory(prefix="daybreak-release-") as temporary:
        root = stage(Path(temporary))
        check_staged(root)
        print("daybreak %s: %d files staged, %s left out"
              % (VERSION, len(CONTENTS), ", ".join(OMITTED)))
        selfcheck(root)
        mtime = int(max((root / name).stat().st_mtime for name in CONTENTS))
        build(root, archive, mtime)
        verify(archive, root.name)

    data = archive.read_bytes()
    print("  archive  %s" % archive)
    print("  sha256   %s" % hashlib.sha256(data).hexdigest())
    print("  size     %d" % len(data))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""One place that says which Daybreak this is.

The launcher prints it, /healthz reports it (the farm reads the version from
there when it lists an app), and scripts/release.py names the archive with it.
Three copies of a version number is two copies too many.
"""

VERSION = "0.1.0"

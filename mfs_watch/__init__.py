"""mfs-watch — real-time TUI dashboard for a local mfs-server.

Reads $MFS_HOME/metadata.db directly (sqlite WAL is concurrent-read safe)
and polls /v1/status over HTTP for a couple of fields that the engine
keeps in memory (Milvus backend label, server version). Refreshes on a
1-second tick by default.

Usage:
    mfs-watch
    mfs-watch --db /path/to/metadata.db
    mfs-watch --endpoint http://127.0.0.1:13619 --interval 0.5

Keys (also shown in footer):
    q       quit
    r       refresh now
    p       pause / resume auto-refresh
"""

from __future__ import annotations

from .app import main

__all__ = ["main"]

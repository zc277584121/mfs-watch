"""Textual TUI for mfs-watch.

Layout (one screen, no scroll-switching):

    +----------------------------------------------------------+
    | header: server line + clock                              |
    +----------------------+-----------------------------------+
    | task queue counts    | active jobs (progress)            |
    +----------------------+-----------------------------------+
    | connectors           | recent events                     |
    +----------------------+-----------------------------------+
    | global stats: objects / chunks / failed / partial        |
    +----------------------------------------------------------+
    | footer: keybindings                                      |
    +----------------------------------------------------------+

Ticker: a single `set_interval` fires `refresh_now()` every interval
seconds. Each tick is a sync sqlite snapshot + optional HTTP fetch.
Both are cheap (sqlite read on indexed tables, one HTTP GET); 1-second
refresh on a normal-sized metadata.db is well under 50ms wall time.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Static

from .store import DataStore, Snapshot, discover_db_path


# ---------- formatting helpers ----------


def _fmt_age(iso: Optional[str], now: Optional[datetime] = None) -> str:
    """ISO string -> 'just now' / '12s' / '4m' / '3h' / '2d'."""
    if not iso:
        return "-"
    try:
        # iso may end with +00:00 or Z; fromisoformat handles +00:00 (3.10+)
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        ts = datetime.fromisoformat(iso)
    except ValueError:
        return iso
    now = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = (now - ts).total_seconds()
    if delta < 0:
        return "0s"
    if delta < 1:
        return "just now"
    if delta < 60:
        return f"{int(delta)}s"
    if delta < 3600:
        return f"{int(delta // 60)}m"
    if delta < 86400:
        return f"{int(delta // 3600)}h"
    return f"{int(delta // 86400)}d"


def _shorten(s: Optional[str], width: int) -> str:
    if not s:
        return ""
    if len(s) <= width:
        return s
    if width <= 1:
        return "…"
    # keep the tail — connector URIs and object URIs distinguish at the end
    return "…" + s[-(width - 1) :]


def _status_style(status: str) -> str:
    """Single source of truth for the status color palette."""
    return {
        "succeeded": "green",
        "running": "cyan",
        "preparing": "cyan",
        "queued": "yellow",
        "failed": "red",
        "skipped": "magenta",
        "cancelled": "dim",
        "active": "green",
        "building": "cyan",
        "removing": "red",
        "unavailable": "red",
    }.get(status or "", "white")


def _progress_bar(done: int, total: int, width: int = 16) -> Text:
    """Inline '[████░░░░] 43%' bar fit for a DataTable cell."""
    if total <= 0:
        return Text("- / -", style="dim")
    ratio = max(0.0, min(1.0, done / total))
    fill = int(ratio * width)
    bar = "█" * fill + "░" * (width - fill)
    return Text.assemble((bar, "cyan"), f" {done}/{total}")


# ---------- the app ----------


class MfsWatch(App):
    CSS = """
    Screen {
        layout: vertical;
    }

    #banner {
        height: 1;
        content-align: center middle;
        background: $accent 10%;
        color: $text;
    }
    #banner.stale {
        background: red 20%;
    }
    #banner.paused {
        background: yellow 20%;
    }

    .row {
        height: 1fr;
    }

    .row > Vertical {
        width: 1fr;
        border: round $primary;
    }

    .panel-title {
        height: 1;
        padding: 0 1;
        background: $primary 30%;
        color: $text;
    }

    DataTable {
        height: 1fr;
    }

    #footstats {
        height: 1;
        padding: 0 1;
        background: $accent 10%;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh_now", "Refresh now"),
        ("p", "toggle_pause", "Pause/resume"),
        ("ctrl+c", "quit", "Quit"),
    ]

    paused: reactive[bool] = reactive(False)
    last_error: reactive[str] = reactive("")

    def __init__(self, store: DataStore, interval: float):
        super().__init__()
        self.store = store
        self.interval = interval

    # ---- layout ----

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static("", id="banner")

        with Horizontal(classes="row"):
            with Vertical():
                yield Static("Task queue", classes="panel-title")
                yield DataTable(id="tbl_queue", cursor_type="none", zebra_stripes=True)
            with Vertical():
                yield Static("Active jobs", classes="panel-title")
                yield DataTable(id="tbl_jobs", cursor_type="row", zebra_stripes=True)

        with Horizontal(classes="row"):
            with Vertical():
                yield Static("Connectors", classes="panel-title")
                yield DataTable(
                    id="tbl_connectors", cursor_type="row", zebra_stripes=True
                )
            with Vertical():
                yield Static("Recent task events", classes="panel-title")
                yield DataTable(id="tbl_events", cursor_type="row", zebra_stripes=True)

        yield Static("", id="footstats")
        yield Footer()

    # ---- lifecycle ----

    def on_mount(self) -> None:
        # Each DataTable needs its columns declared once at startup.
        q = self.query_one("#tbl_queue", DataTable)
        q.add_columns("status", "count")

        j = self.query_one("#tbl_jobs", DataTable)
        j.add_columns("connector", "kind", "status", "progress", "started", "heartbeat")

        c = self.query_one("#tbl_connectors", DataTable)
        c.add_columns("uri", "type", "status", "objects", "chunks", "health")

        e = self.query_one("#tbl_events", DataTable)
        e.add_columns("when", "status", "uri", "detail")

        self.refresh_now()
        self.timer = self.set_interval(self.interval, self._on_tick)

    def _on_tick(self) -> None:
        if self.paused:
            return
        self.refresh_now()

    # ---- actions ----

    def action_quit(self) -> None:
        self.store.close()
        self.exit()

    def action_refresh_now(self) -> None:
        self.refresh_now()

    def action_toggle_pause(self) -> None:
        self.paused = not self.paused
        # banner gets re-rendered on next snapshot; for paused with no
        # snapshot change, force an update here too.
        self._update_banner_classes()

    def watch_paused(self, _old: bool, _new: bool) -> None:
        self._update_banner_classes()

    def _update_banner_classes(self) -> None:
        banner = self.query_one("#banner", Static)
        banner.set_class(self.paused, "paused")

    # ---- refresh path ----

    def refresh_now(self) -> None:
        """One synchronous snapshot + UI swap. Cheap (~10-50ms);
        textual's reactive loop is fine with sync work this small."""
        try:
            snap = self.store.snapshot()
            self.last_error = ""
        except Exception as e:  # noqa: BLE001 — TUI must never crash on data
            self.last_error = f"{type(e).__name__}: {e}"
            self.query_one("#banner", Static).set_class(True, "stale")
            return

        self._render_banner(snap)
        self._render_queue(snap)
        self._render_jobs(snap)
        self._render_connectors(snap)
        self._render_events(snap)
        self._render_footstats(snap)

    # ---- per-section renderers ----

    def _render_banner(self, s: Snapshot) -> None:
        parts: list[str] = []
        if s.server_endpoint:
            mark = "●" if s.server_reachable else "○"
            color = "green" if s.server_reachable else "red"
            parts.append(f"[{color}]{mark}[/] {s.server_endpoint}")
        if s.server_version:
            parts.append(f"v{s.server_version}")
        if s.namespace:
            parts.append(f"ns={s.namespace}")
        if s.milvus_backend:
            parts.append(f"milvus={s.milvus_backend}")
        parts.append(f"db={s.db_path}{'' if s.db_present else ' [red](missing)[/]'}")
        parts.append(f"workers≈[bold cyan]{s.active_workers}[/]")
        if self.paused:
            parts.append("[yellow]PAUSED[/]")
        if self.last_error:
            parts.append(f"[red]err: {self.last_error}[/]")

        banner = self.query_one("#banner", Static)
        banner.update("  ·  ".join(parts))
        banner.set_class(not s.db_present, "stale")
        banner.set_class(self.paused, "paused")

    def _render_queue(self, s: Snapshot) -> None:
        t = self.query_one("#tbl_queue", DataTable)
        t.clear()
        # ensure all known statuses show up even when count=0
        order = ["queued", "running", "succeeded", "failed", "skipped", "cancelled"]
        seen = set()
        for status in order:
            n = s.task_status_counts.get(status, 0)
            seen.add(status)
            t.add_row(Text(status, style=_status_style(status)), str(n))
        # surface any unknown status the server starts emitting later
        for status, n in sorted(s.task_status_counts.items()):
            if status in seen:
                continue
            t.add_row(Text(status, style="white"), str(n))

    def _render_jobs(self, s: Snapshot) -> None:
        t = self.query_one("#tbl_jobs", DataTable)
        t.clear()
        if not s.active_jobs:
            t.add_row(Text("(idle — no active jobs)", style="dim"), "", "", "", "", "")
            return
        for j in s.active_jobs:
            uri = _shorten(j.get("root_uri") or j.get("connector_id") or "?", 32)
            kind = j.get("op_kind") or ""
            status = j.get("status") or ""
            done = int(j.get("succeeded_objects") or 0)
            total = int(j.get("total_objects") or 0)
            started = _fmt_age(j.get("started_at"), s.fetched_at)
            hb = _fmt_age(j.get("heartbeat"), s.fetched_at)
            t.add_row(
                uri,
                kind,
                Text(status, style=_status_style(status)),
                _progress_bar(done, total),
                started,
                hb,
            )

    def _render_connectors(self, s: Snapshot) -> None:
        t = self.query_one("#tbl_connectors", DataTable)
        t.clear()
        if not s.connectors:
            t.add_row(Text("(none registered)", style="dim"), "", "", "", "", "")
            return
        for c in s.connectors:
            uri = _shorten(c.get("root_uri") or c.get("id") or "?", 36)
            ctype = c.get("type") or ""
            status = c.get("status") or ""
            objects = str(c.get("object_count") or 0)
            chunks = str(c.get("chunk_count") or 0)
            health = c.get("health_status") or ""
            t.add_row(
                uri,
                ctype,
                Text(status, style=_status_style(status)),
                objects,
                chunks,
                Text(health, style=_status_style(health)) if health else "",
            )

    def _render_events(self, s: Snapshot) -> None:
        t = self.query_one("#tbl_events", DataTable)
        t.clear()
        if not s.recent_events:
            t.add_row(Text("(no recent events)", style="dim"), "", "", "")
            return
        for ev in s.recent_events:
            when = _fmt_age(ev.get("finished_at"), s.fetched_at)
            status = ev.get("status") or ""
            uri = _shorten(ev.get("object_uri") or "", 40)
            err = ev.get("last_error") or ""
            detail = _shorten(err, 40) if err else f"attempts={ev.get('attempts') or 0}"
            t.add_row(
                when,
                Text(status, style=_status_style(status)),
                uri,
                Text(detail, style="red" if err else "dim"),
            )

    def _render_footstats(self, s: Snapshot) -> None:
        st = self.query_one("#footstats", Static)
        bits = [
            f"objects={s.total_objects}",
            f"chunks={s.total_chunks}",
        ]
        if s.failed_objects:
            bits.append(f"[red]failed_objects={s.failed_objects}[/]")
        else:
            bits.append("failed_objects=0")
        if s.partial_objects:
            bits.append(f"[yellow]partial_objects={s.partial_objects}[/]")
        else:
            bits.append("partial_objects=0")
        bits.append(f"refresh={self.interval:.1f}s")
        st.update("  ·  ".join(bits))


# ---------- entry point ----------


def main() -> None:
    p = argparse.ArgumentParser(
        prog="mfs-watch", description="Real-time TUI for mfs-server."
    )
    p.add_argument("--db", help="Path to metadata.db (default: $MFS_HOME/metadata.db).")
    p.add_argument(
        "--endpoint",
        default=os.environ.get("MFS_API_URL", "http://127.0.0.1:13619"),
        help="HTTP endpoint of the running mfs-server (default: $MFS_API_URL or 127.0.0.1:13619).",
    )
    p.add_argument(
        "--token",
        default=None,
        help="API bearer token. Defaults to $MFS_API_TOKEN, then $MFS_HOME/server.token if present.",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Refresh interval in seconds (default 1.0).",
    )
    p.add_argument(
        "--no-http", action="store_true", help="Don't try to reach /v1/status at all."
    )
    args = p.parse_args()

    db_path = discover_db_path(args.db)
    token = args.token or os.environ.get("MFS_API_TOKEN")
    if token is None:
        # fall back to the server's auto-generated token file, like the
        # Rust CLI does. Same logic as the mfs binary on a loopback host.
        candidate = (
            Path(os.environ.get("MFS_HOME") or Path.home() / ".mfs") / "server.token"
        )
        if candidate.is_file():
            try:
                token = candidate.read_text().strip()
            except OSError:
                token = None

    endpoint = None if args.no_http else args.endpoint

    if not db_path.exists() and endpoint is None:
        print(
            f"mfs-watch: metadata.db not found at {db_path} and HTTP is disabled. "
            "Either start mfs-server first, or pass --db / --endpoint.",
            file=sys.stderr,
        )
        sys.exit(2)

    store = DataStore(db_path=db_path, endpoint=endpoint, token=token)
    app = MfsWatch(store=store, interval=args.interval)
    try:
        app.run()
    finally:
        store.close()


if __name__ == "__main__":
    main()

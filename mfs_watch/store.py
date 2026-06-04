"""Data access for mfs-watch.

sqlite is opened read-only with `mode=ro&immutable=0` so the TUI can never
write metadata.db by accident even with a typo. WAL is on (the server
enables it at startup), so a long-running read coexists fine with the
server's writes.

All time fields in metadata.db are ISO-8601 UTC strings produced by
`datetime.now(timezone.utc).isoformat()`. Lexicographic order matches
chronological order for that format, so SQL `WHERE heartbeat > ?` and
ORDER BY work directly on the strings.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx

# How long since a job's last heartbeat counts as "still alive". Bumped one
# server worker-loop tick + a safety margin. mfs-server worker writes
# heartbeat from inside _drain_job; default loop is ~2-5s in flight.
HEARTBEAT_FRESH_S = 30

# How many recently-finished tasks to show in the events panel.
RECENT_EVENTS_N = 25


def discover_db_path(override: Optional[str]) -> Path:
    """Resolve metadata.db path the way mfs-server does for itself.

    Priority: --db arg > $MFS_HOME/metadata.db > ~/.mfs/metadata.db. We
    don't read server.toml — the TUI is a debug tool, "fall back to the
    standard location" is fine when there's no override.
    """
    if override:
        return Path(override).expanduser()
    mfs_home = os.environ.get("MFS_HOME")
    if mfs_home:
        return Path(mfs_home).expanduser() / "metadata.db"
    return Path.home() / ".mfs" / "metadata.db"


@dataclass
class Snapshot:
    """One refresh tick's worth of data — what the UI renders."""

    fetched_at: datetime
    db_path: str
    db_present: bool
    server_endpoint: Optional[str]
    server_reachable: bool
    server_version: Optional[str]
    milvus_backend: Optional[str]
    namespace: Optional[str]

    # task queue distribution: status -> count
    task_status_counts: dict[str, int]

    # active jobs (preparing/running/queued)
    active_jobs: list[dict]
    # active worker approximation: jobs with a fresh heartbeat
    active_workers: int

    # connectors
    connectors: list[dict]

    # recent task events (finished_at desc)
    recent_events: list[dict]

    # global aggregates
    total_chunks: int
    total_objects: int
    failed_objects: int
    partial_objects: int

    # most recent error surface
    recent_errors: list[dict]


def _heartbeat_threshold_iso() -> str:
    """ISO string for 'HEARTBEAT_FRESH_S seconds ago' — used in
    WHERE heartbeat > ? on connector_jobs."""
    return (
        datetime.now(timezone.utc) - timedelta(seconds=HEARTBEAT_FRESH_S)
    ).isoformat()


class DataStore:
    """Holds the read-only sqlite connection and the optional HTTP client.

    Built once per TUI session; `snapshot()` is called every tick. The
    sqlite connection is kept open across ticks (cheaper than reopening
    a uri-mode connection every second) but reopened on demand if the
    file appears or disappears (server stop/start).
    """

    def __init__(self, db_path: Path, endpoint: Optional[str], token: Optional[str]):
        self.db_path = db_path
        self.endpoint = endpoint
        self.token = token
        self._conn: Optional[sqlite3.Connection] = None
        self._http = (
            httpx.Client(
                timeout=2.0,
                headers={"Authorization": f"Bearer {token}"} if token else {},
            )
            if endpoint
            else None
        )

    # ---- sqlite plumbing ----

    def _connect(self) -> Optional[sqlite3.Connection]:
        if self._conn is not None:
            return self._conn
        if not self.db_path.exists():
            return None
        # read-only uri mode: the WAL file the server already wrote will
        # be picked up automatically (sqlite opens *.db-wal next to *.db).
        uri = f"file:{self.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=2.0)
        conn.row_factory = sqlite3.Row
        # WAL readers must NOT take an exclusive lock. The server's
        # writers already keep journal_mode=wal; nothing for us to set.
        self._conn = conn
        return conn

    def _drop_conn(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def close(self) -> None:
        self._drop_conn()
        if self._http is not None:
            try:
                self._http.close()
            except Exception:
                pass

    # ---- snapshot ----

    def snapshot(self) -> Snapshot:
        now = datetime.now(timezone.utc)
        out = Snapshot(
            fetched_at=now,
            db_path=str(self.db_path),
            db_present=False,
            server_endpoint=self.endpoint,
            server_reachable=False,
            server_version=None,
            milvus_backend=None,
            namespace=None,
            task_status_counts={},
            active_jobs=[],
            active_workers=0,
            connectors=[],
            recent_events=[],
            total_chunks=0,
            total_objects=0,
            failed_objects=0,
            partial_objects=0,
            recent_errors=[],
        )

        # HTTP first (server metadata, doesn't block sqlite section)
        if self._http is not None:
            try:
                r = self._http.get(f"{self.endpoint}/v1/status")
                if r.status_code == 200:
                    out.server_reachable = True
                    j = r.json()
                    out.server_version = (
                        j.get("server", {}).get("version")
                        if isinstance(j, dict)
                        else None
                    )
                    out.milvus_backend = (
                        j.get("milvus", {}).get("backend")
                        if isinstance(j, dict)
                        else None
                    )
                    out.namespace = (
                        j.get("namespace") or j.get("server", {}).get("namespace")
                        if isinstance(j, dict)
                        else None
                    )
            except httpx.HTTPError:
                out.server_reachable = False

        # sqlite section — wrap each query so a partial server upgrade /
        # schema mismatch downgrades a panel rather than crashing the TUI.
        conn = self._connect()
        if conn is None:
            return out
        out.db_present = True

        out.task_status_counts = self._task_status_counts(conn)
        out.active_jobs = self._active_jobs(conn)
        out.active_workers = self._active_workers(conn)
        out.connectors = self._connectors(conn)
        out.recent_events = self._recent_events(conn)
        agg = self._object_aggregates(conn)
        out.total_chunks = agg["chunks"]
        out.total_objects = agg["objects"]
        out.failed_objects = agg["failed"]
        out.partial_objects = agg["partial"]
        out.recent_errors = self._recent_errors(conn)

        return out

    # ---- per-section queries ----

    def _safe(
        self, conn: sqlite3.Connection, sql: str, params: tuple = ()
    ) -> list[sqlite3.Row]:
        """Run a query, swallow OperationalError (schema drift) by
        returning []. The TUI shows an empty panel rather than dying."""
        try:
            return list(conn.execute(sql, params))
        except sqlite3.OperationalError:
            return []

    def _task_status_counts(self, conn: sqlite3.Connection) -> dict[str, int]:
        rows = self._safe(
            conn, "SELECT status, count(*) AS n FROM object_tasks GROUP BY status"
        )
        return {r["status"] or "(null)": r["n"] for r in rows}

    def _active_jobs(self, conn: sqlite3.Connection) -> list[dict]:
        rows = self._safe(
            conn,
            """
            SELECT cj.id, cj.connector_id, cj.status, cj.op_kind, cj.started_at,
                   cj.heartbeat, cj.total_objects, cj.succeeded_objects,
                   cj.failed_objects, c.root_uri
            FROM connector_jobs cj
            LEFT JOIN connectors c ON c.id = cj.connector_id
            WHERE cj.status IN ('preparing', 'running', 'queued')
            ORDER BY cj.started_at DESC
            LIMIT 20
            """,
        )
        return [dict(r) for r in rows]

    def _active_workers(self, conn: sqlite3.Connection) -> int:
        """Approximation: a 'worker' is a job currently in status='running'
        with a heartbeat fresh inside the last HEARTBEAT_FRESH_S seconds.
        That counts in-flight per-job worker-coroutines, not OS processes.
        Until the server gets a workers table this is the honest signal."""
        threshold = _heartbeat_threshold_iso()
        rows = self._safe(
            conn,
            "SELECT count(*) AS n FROM connector_jobs "
            "WHERE status='running' AND heartbeat IS NOT NULL AND heartbeat > ?",
            (threshold,),
        )
        return rows[0]["n"] if rows else 0

    def _connectors(self, conn: sqlite3.Connection) -> list[dict]:
        rows = self._safe(
            conn,
            """
            SELECT c.id, c.root_uri, c.type, c.status,
                   COALESCE(o.object_count, 0) AS object_count,
                   COALESCE(o.chunk_count, 0) AS chunk_count,
                   c.last_health, c.health_status
            FROM connectors c
            LEFT JOIN (
                SELECT connector_id,
                       count(*) AS object_count,
                       sum(COALESCE(chunk_count, 0)) AS chunk_count
                FROM objects
                GROUP BY connector_id
            ) o ON o.connector_id = c.id
            ORDER BY c.registered_at
            """,
        )
        return [dict(r) for r in rows]

    def _recent_events(self, conn: sqlite3.Connection) -> list[dict]:
        rows = self._safe(
            conn,
            """
            SELECT t.object_uri, t.status, t.last_error, t.finished_at,
                   t.attempts, c.root_uri
            FROM object_tasks t
            LEFT JOIN connectors c ON c.id = t.connector_id
            WHERE t.finished_at IS NOT NULL
            ORDER BY t.finished_at DESC
            LIMIT ?
            """,
            (RECENT_EVENTS_N,),
        )
        return [dict(r) for r in rows]

    def _object_aggregates(self, conn: sqlite3.Connection) -> dict[str, int]:
        rows = self._safe(
            conn,
            """
            SELECT
                count(*) AS objects,
                COALESCE(sum(chunk_count), 0) AS chunks,
                COALESCE(sum(CASE WHEN search_status='failed' THEN 1 ELSE 0 END), 0) AS failed,
                COALESCE(sum(CASE WHEN search_status='partial' THEN 1 ELSE 0 END), 0) AS partial
            FROM objects
            """,
        )
        if not rows:
            return {"objects": 0, "chunks": 0, "failed": 0, "partial": 0}
        r = rows[0]
        return {
            "objects": r["objects"] or 0,
            "chunks": r["chunks"] or 0,
            "failed": r["failed"] or 0,
            "partial": r["partial"] or 0,
        }

    def _recent_errors(self, conn: sqlite3.Connection) -> list[dict]:
        rows = self._safe(
            conn,
            """
            SELECT object_uri, last_error, finished_at, attempts
            FROM object_tasks
            WHERE status='failed' AND last_error IS NOT NULL
            ORDER BY finished_at DESC
            LIMIT 10
            """,
        )
        return [dict(r) for r in rows]

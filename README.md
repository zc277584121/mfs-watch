# mfs-watch

Real-time TUI dashboard for a locally-running `mfs-server`. Reads
`$MFS_HOME/metadata.db` directly (sqlite WAL — concurrent-read safe)
and polls `/v1/status` over HTTP for the few fields the engine keeps
only in memory. Refreshes once a second by default.

This is a personal debug tool — not part of the mfs product. The on-disk
schema it reads from is internal and may change between mfs-server
releases; if it does, individual panels just go blank, the TUI doesn't
crash.

## Install / run

```bash
cd ~/mfs-watch
uv sync                  # create .venv + install textual + httpx
uv run mfs-watch         # use $MFS_HOME, default endpoint
```

Or one-shot without installing into the project venv:

```bash
uv run --with textual --with httpx python -m mfs_watch
```

## Flags

| Flag | Default | What |
|---|---|---|
| `--db PATH` | `$MFS_HOME/metadata.db` else `~/.mfs/metadata.db` | sqlite file to read |
| `--endpoint URL` | `$MFS_API_URL` else `http://127.0.0.1:13619` | mfs-server HTTP base |
| `--token VALUE` | `$MFS_API_TOKEN` else `$MFS_HOME/server.token` | bearer token for `/v1/status` |
| `--interval N` | `1.0` | refresh interval in seconds |
| `--no-http` | off | skip HTTP, sqlite-only mode |

## Keys

| Key | What |
|---|---|
| `q` / `Ctrl-C` | quit |
| `r` | refresh now (don't wait for the next tick) |
| `p` | pause / resume auto-refresh |

## What you see

```
+----- header (clock) ---------------------------------------------+
| banner: endpoint · version · ns · milvus · db · workers · paused |
+----------------------+-------------------------------------------+
| Task queue           | Active jobs                               |
|   queued    142      |   uri  kind  status  progress  age  hb    |
|   running    7       |   ...                                     |
|   succeeded 2,341    |                                           |
|   failed     3       |                                           |
|   skipped   18       |                                           |
+----------------------+-------------------------------------------+
| Connectors           | Recent task events                        |
|   uri type status... |   age status uri detail                   |
|   ...                |                                           |
+----------------------+-------------------------------------------+
| footstats: objects · chunks · failed · partial · refresh         |
+------------------ footer keybindings ----------------------------+
```

## Known approximations

- **`workers≈N` in the banner** counts mfs-server `connector_jobs`
  rows with `status='running'` and a heartbeat within the last 30s.
  That's per-job worker coroutines, not OS processes — accurate
  enough for "is anything actually being processed right now?".
  A real worker registry would need a server-side `workers` table;
  not done yet.

- **Sink stages** (`already converted` / `already embedded` / `written
  to Milvus`) aren't a distinct field on `object_tasks`. The TUI
  shows `succeeded` once the whole task pipeline finishes for an
  object; the granular intermediate stages would need an enum on the
  task table.

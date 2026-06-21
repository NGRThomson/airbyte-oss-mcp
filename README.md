# Airbyte OSS MCP

Read-only MCP server for self-hosted **Airbyte OSS 1.6.x**.

Wraps:

- **Public API** — `/api/public/v1` (connections, jobs, health)
- **Internal API** — `/api/v1/jobs/get_without_logs` (attempt failure summaries)

Modeled after [`dagster-mcp`](https://pypi.org/project/dagster-mcp/) (FastMCP + env-based config).

## Install (Cursor + `uvx`)

**Prerequisites:** [uv](https://docs.astral.sh/uv/getting-started/installation/) installed, network access to your Airbyte instance.

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "airbyte": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/NGRThomson/airbyte-oss-mcp.git@main",
        "airbyte-mcp"
      ],
      "env": {
        "AIRBYTE_URL": "https://airbyte.example.com",
        "AIRBYTE_READ_ONLY": "true"
      }
    }
  }
}
```

Replace `AIRBYTE_URL` with your instance URL. Default (if unset) is `http://localhost:8000`.

Pin a tag or commit for slower upgrades:

```text
git+https://github.com/NGRThomson/airbyte-oss-mcp.git@v0.3.0
```

See [`examples/cursor-mcp.json`](examples/cursor-mcp.json) for a team install template.

Optional persistent install:

```bash
uv tool install --from 'git+https://github.com/NGRThomson/airbyte-oss-mcp.git@main' airbyte-mcp
```

### Local dev

```json
"args": ["--from", "/path/to/airbyte-oss-mcp", "airbyte-mcp"]
```

Or:

```bash
cd airbyte-oss-mcp
uv sync --extra dev
uv run ruff check airbyte_mcp/
uv run pytest
uv run airbyte-mcp
```

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `AIRBYTE_URL` | `http://localhost:8000` | Base URL of your Airbyte instance |
| `AIRBYTE_API_TOKEN` | (empty) | Bearer token if public API auth is enabled |
| `AIRBYTE_READ_ONLY` | `true` | When false, exposes `cancel_job` and `trigger_sync` |
| `AIRBYTE_ENVS` | (empty) | JSON map of named envs for multi-instance setups |
| `AIRBYTE_DEFAULT_ENV` | (empty) | Default key when `AIRBYTE_ENVS` has multiple entries |

Multi-env example:

```json
"AIRBYTE_ENVS": "{\"prod\":{\"url\":\"https://airbyte.example.com\"},\"staging\":{\"url\":\"https://airbyte-staging.example.com\"}}",
"AIRBYTE_DEFAULT_ENV": "prod"
```

Optional bearer token:

```json
"AIRBYTE_API_TOKEN": "<token from Airbyte Settings → Applications>"
```

## Tools

| Tool | Purpose |
|------|---------|
| `get_instance_status` | Health, connection counts, running/pending/failed job counts (bounded samples) |
| `list_connections` | Browse/filter connections |
| `get_connection` | One connection + recent jobs |
| `list_jobs` | Recent jobs (filter by connection/status) |
| `get_job` | Public job metadata |
| `get_job_details` | Attempt stats + failure summaries |
| `get_job_failure_summary` | Root-cause failure messages for a failed sync |

Write tools (`cancel_job`, `trigger_sync`) register only when `AIRBYTE_READ_ONLY=false`.

## Smoke test

```bash
uv run python -c "
from airbyte_mcp.client import AirbyteClient
c = AirbyteClient('https://airbyte.example.com')
print(c.health())
print('running', len(c.list_jobs(status='running', limit=10)))
"
```

## Notes

- Always pass `orderBy=updatedAt|DESC` when listing jobs (default in client) — otherwise Airbyte returns oldest jobs first.
- Raw job logs are not exposed (public API has no log endpoint); failure summaries come from internal `get_without_logs`.
- **Internal OSS API:** `get_job_details` and `get_job_failure_summary` use `/api/v1/jobs/get_without_logs`, which is not part of the public API and may break on Airbyte upgrades.
- `list_connections` / `list_jobs` paginate automatically when `limit` exceeds the API page size (100).
- **Runtime data:** when pointed at a live instance, tool responses include connection names, job errors, and IDs from *your* Airbyte workspace — keep MCP read-only in shared Cursor configs if that metadata is sensitive.

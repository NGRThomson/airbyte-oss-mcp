"""Airbyte MCP server — OSS public + internal API wrapper."""

from __future__ import annotations

import os
from typing import Any

from fastmcp import FastMCP

from airbyte_mcp.client import (
    AirbyteClient,
    connection_index,
    enrich_job,
    find_duplicate_destination_tables as detect_duplicate_destination_tables,
    normalize_attempts,
    parse_envs,
    resolve_client,
    resolve_connection_ref,
    summarize_connection,
)

AIRBYTE_URL = os.environ.get("AIRBYTE_URL", "http://localhost:8000")
AIRBYTE_API_TOKEN = os.environ.get("AIRBYTE_API_TOKEN", "")
AIRBYTE_DEFAULT_ENV = os.environ.get("AIRBYTE_DEFAULT_ENV", "")
READ_ONLY = os.environ.get("AIRBYTE_READ_ONLY", "true").lower() in ("true", "1", "yes")
_ENVS = parse_envs(os.environ.get("AIRBYTE_ENVS", ""))

_mode = "read-only" if READ_ONLY else "read-write"
_env_info = (
    f"Available environments: {', '.join(_ENVS)}. Pass env=<name> to each tool. "
    if _ENVS
    else ""
)

mcp = FastMCP(
    "airbyte",
    instructions=(
        f"Use these tools to monitor a self-hosted Airbyte OSS instance ({_mode} mode). "
        f"{_env_info}"
        "Start with get_instance_status or get_active_syncs to see what is running, "
        "then list_connections / list_jobs / get_job_failure_summary to drill in. "
        "Use find_duplicate_destination_tables to audit BigQuery table conflicts."
    ),
)


def _client(env: str | None = None) -> AirbyteClient:
    return resolve_client(
        env,
        envs=_ENVS,
        default_env=AIRBYTE_DEFAULT_ENV,
        fallback_url=AIRBYTE_URL,
        fallback_token=AIRBYTE_API_TOKEN,
    )


def _connection_maps(client: AirbyteClient) -> dict[str, dict[str, dict[str, Any]]]:
    return connection_index(client.list_all_connections())


@mcp.tool()
def get_instance_status(env: str | None = None) -> dict[str, Any]:
    """Global Airbyte health snapshot. Start here for monitoring workflows.

    Returns health text, workspace count, connection counts by status,
    and counts of running / pending / failed sync jobs (recent sample).
    """
    client = _client(env)
    health = client.health()
    workspaces = client.list_workspaces()
    connections = client.list_all_connections()

    status_counts: dict[str, int] = {}
    schedule_counts: dict[str, int] = {}
    for conn in connections:
        status = str(conn.get("status", "unknown"))
        status_counts[status] = status_counts.get(status, 0) + 1
        sched = (conn.get("schedule") or {}).get("scheduleType", "unknown")
        schedule_counts[str(sched)] = schedule_counts.get(str(sched), 0) + 1

    running = client.list_all_jobs(status="running")
    pending = client.list_all_jobs(status="pending")
    failed_recent = client.list_jobs_limited(20, status="failed")

    return {
        "healthy": health.lower() in ("ok", "successful operation"),
        "health": health,
        "workspace_count": len(workspaces),
        "connection_count": len(connections),
        "connections_by_status": status_counts,
        "connections_by_schedule_type": schedule_counts,
        "running_sync_count": len(running),
        "pending_sync_count": len(pending),
        "recent_failed_sync_count": len(failed_recent),
    }


@mcp.tool()
def list_connections(
    name_prefix: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    env: str | None = None,
) -> list[dict[str, Any]]:
    """List Airbyte connections with schedule and status metadata.

    Filters:
    - name_prefix: case-insensitive substring on connection name
    - status: exact status filter (e.g. 'active')
    - limit: max results (paginates beyond the 100-item API page size when needed)
    - offset: skip first N connections from the API ordering
    """
    client = _client(env)
    connections = client.list_connections_limited(limit=max(limit, 1), offset=max(offset, 0))

    if name_prefix:
        needle = name_prefix.lower()
        connections = [
            c for c in connections if needle in str(c.get("name", "")).lower()
        ]
    if status:
        connections = [c for c in connections if c.get("status") == status]

    return [
        {
            "connectionId": c.get("connectionId"),
            "name": c.get("name"),
            "status": c.get("status"),
            "scheduleType": (c.get("schedule") or {}).get("scheduleType"),
            "sourceId": c.get("sourceId"),
            "destinationId": c.get("destinationId"),
            "workspaceId": c.get("workspaceId"),
        }
        for c in connections
    ]


@mcp.tool()
def get_connection(connection: str, env: str | None = None) -> dict[str, Any]:
    """Get one connection by exact name or connectionId UUID.

    Returns connection metadata with stream summaries (name, namespace, prefix, syncMode)
    and the three most recent sync jobs.
    """
    client = _client(env)
    index = _connection_maps(client)
    conn = resolve_connection_ref(connection, index)
    conn_id = str(conn["connectionId"])
    detail = client.get_connection(conn_id)
    jobs = client.list_jobs(connection_id=conn_id, limit=3)
    return {
        "connection": summarize_connection(detail),
        "recentJobs": jobs,
    }


@mcp.tool()
def list_jobs(
    connection: str | None = None,
    status: str | None = None,
    limit: int = 20,
    offset: int = 0,
    env: str | None = None,
) -> list[dict[str, Any]]:
    """List recent sync jobs, newest first.

    Returns jobId, status, startTime, duration, bytesSynced, rowsSynced,
    connectionId, and connectionName.

    Filters:
    - connection: connection name, UUID, or substring (must match exactly one)
    - status: pending, running, incomplete, failed, succeeded, cancelled
    - limit: max results (paginates beyond the 100-item API page size when needed)
    - offset: skip first N jobs from the API ordering
    """
    client = _client(env)
    index = _connection_maps(client)
    connection_id = None
    if connection:
        conn = resolve_connection_ref(connection, index)
        connection_id = str(conn["connectionId"])

    jobs = client.list_jobs_limited(
        limit=max(limit, 1),
        offset=max(offset, 0),
        connection_id=connection_id,
        status=status,
    )
    return [enrich_job(job, index["by_id"]) for job in jobs]


@mcp.tool()
def get_active_syncs(env: str | None = None) -> list[dict[str, Any]]:
    """List all currently running sync jobs with connection names.

    Best tool to answer 'what is blocking the Airbyte worker pool?' or
    'why is this sync taking so long?'
    """
    client = _client(env)
    index = _connection_maps(client)
    jobs = client.list_all_jobs(status="running")
    return [enrich_job(job, index["by_id"]) for job in jobs]


@mcp.tool()
def find_duplicate_destination_tables(env: str | None = None) -> dict[str, Any]:
    """Detect multiple active connections writing the same BigQuery project.dataset.table.

    Best-effort audit using connection stream config and destination settings.
    Skips non-BigQuery destinations and inactive connections.
    """
    client = _client(env)
    return detect_duplicate_destination_tables(client)


@mcp.tool()
def get_job(job_id: int, env: str | None = None) -> dict[str, Any]:
    """Get public job metadata for a sync job id."""
    client = _client(env)
    index = _connection_maps(client)
    job = client.get_job_public(job_id)
    return enrich_job(job, index["by_id"])


@mcp.tool()
def get_job_details(job_id: int, env: str | None = None) -> dict[str, Any]:
    """Get job plus attempt stats via the internal OSS jobs API.

    Includes per-attempt totalStats and failure summaries (without raw logs).
    """
    client = _client(env)
    index = _connection_maps(client)
    payload = client.get_job_with_attempts(job_id)
    job = payload.get("job", {})
    conn_id = str(job.get("configId") or job.get("connectionId") or "")
    return {
        "jobId": job.get("id"),
        "status": job.get("status"),
        "connectionId": conn_id or None,
        "connectionName": index["by_id"].get(conn_id, {}).get("name"),
        "createdAt": job.get("createdAt"),
        "updatedAt": job.get("updatedAt"),
        "attempts": normalize_attempts(payload),
    }


@mcp.tool()
def get_job_failure_summary(job_id: int, env: str | None = None) -> dict[str, Any]:
    """Consolidated failure diagnosis for a sync job (prefer over raw logs).

    Returns root-cause failure messages from the latest failed attempt,
    plus job/connection context and row/byte stats when available.
    """
    client = _client(env)
    index = _connection_maps(client)
    payload = client.get_job_with_attempts(job_id)
    job = payload.get("job", {})
    status = job.get("status")
    conn_id = str(job.get("configId") or "")

    if status not in ("failed", "cancelled", "incomplete"):
        return {
            "jobId": job_id,
            "status": status,
            "message": "Job did not fail.",
        }

    attempts = normalize_attempts(payload)
    failed_attempts = [a for a in attempts if a.get("status") == "failed"]
    latest = failed_attempts[-1] if failed_attempts else (attempts[-1] if attempts else {})

    failures = latest.get("failures") or []
    root_messages = []
    for failure in failures:
        msg = failure.get("internalMessage") or failure.get("externalMessage")
        if msg:
            root_messages.append(
                {
                    "origin": failure.get("failureOrigin"),
                    "type": failure.get("failureType"),
                    "message": msg,
                }
            )

    public = client.get_job_public(job_id)
    enriched = enrich_job(public, index["by_id"])

    return {
        "jobId": job_id,
        "status": status,
        "connectionId": conn_id or None,
        "connectionName": index["by_id"].get(conn_id, {}).get("name"),
        "duration": enriched.get("duration"),
        "bytesSynced": enriched.get("bytesSynced"),
        "rowsSynced": enriched.get("rowsSynced"),
        "failedAttemptCount": len(failed_attempts),
        "rootCauseFailures": root_messages,
        "suggestions": _failure_suggestions(root_messages, enriched),
    }


def _failure_suggestions(
    failures: list[dict[str, Any]],
    job: dict[str, Any],
) -> list[str]:
    suggestions: list[str] = []
    blob = " ".join(f.get("message", "") for f in failures).lower()
    if "quota exceeded" in blob or "rate limit" in blob:
        suggestions.append("Destination quota/rate limit hit — retry later or raise quota.")
    if "timeout" in blob:
        suggestions.append("Sync timed out — consider narrowing streams or raising worker resources.")
    if job.get("duration") and str(job["duration"]).startswith("PT") and "H" in str(job["duration"]):
        suggestions.append(
            "Long-running sync — check worker pool concurrency limits in your orchestrator."
        )
    if not suggestions:
        suggestions.append("Use Airbyte UI job logs for full stack traces if needed.")
    return suggestions


def cancel_job(job_id: int, env: str | None = None) -> dict[str, Any]:
    """Cancel a running or pending Airbyte sync job."""
    client = _client(env)
    return client.cancel_job(job_id)


def trigger_sync(connection: str, env: str | None = None) -> dict[str, Any]:
    """Trigger a manual sync for a connection (name or UUID)."""
    client = _client(env)
    index = _connection_maps(client)
    conn = resolve_connection_ref(connection, index)
    return client.trigger_sync(str(conn["connectionId"]))


if not READ_ONLY:
    mcp.tool()(cancel_job)
    mcp.tool()(trigger_sync)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()

"""HTTP client for Airbyte OSS public and internal APIs."""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlencode

import httpx

DEFAULT_ORDER_BY = "updatedAt|DESC"


class AirbyteClient:
    """Thin wrapper around Airbyte 1.x public + internal REST APIs."""

    def __init__(
        self,
        base_url: str,
        api_token: str = "",
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.public_base = f"{self.base_url}/api/public/v1"
        self.internal_base = f"{self.base_url}/api/v1"
        self.timeout = timeout
        self._headers: dict[str, str] = {"Accept": "application/json"}
        if api_token:
            self._headers["Authorization"] = f"Bearer {api_token}"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> AirbyteClient:
        cfg = env or os.environ
        return cls(
            base_url=cfg.get("AIRBYTE_URL", "http://localhost:8000"),
            api_token=cfg.get("AIRBYTE_API_TOKEN", ""),
        )

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        query = f"?{urlencode(params, doseq=True)}" if params else ""
        try:
            response = httpx.get(
                f"{url}{query}",
                headers=self._headers,
                timeout=self.timeout,
            )
        except httpx.ConnectError as exc:
            raise RuntimeError(
                f"Cannot connect to Airbyte at {self.base_url}. "
                "Check AIRBYTE_URL and network/VPN access."
            ) from exc
        except httpx.TimeoutException as exc:
            raise RuntimeError(
                f"Request to Airbyte at {self.base_url} timed out after {self.timeout}s."
            ) from exc

        if response.status_code >= 400:
            raise RuntimeError(
                f"Airbyte returned HTTP {response.status_code}: {response.text[:500]}"
            )
        if not response.content:
            return {}
        return response.json()

    def _post(self, url: str, body: dict[str, Any]) -> Any:
        headers = {**self._headers, "Content-Type": "application/json"}
        try:
            response = httpx.post(
                url,
                headers=headers,
                json=body,
                timeout=self.timeout,
            )
        except httpx.ConnectError as exc:
            raise RuntimeError(
                f"Cannot connect to Airbyte at {self.base_url}. "
                "Check AIRBYTE_URL and network/VPN access."
            ) from exc
        except httpx.TimeoutException as exc:
            raise RuntimeError(
                f"Request to Airbyte at {self.base_url} timed out after {self.timeout}s."
            ) from exc

        if response.status_code >= 400:
            raise RuntimeError(
                f"Airbyte returned HTTP {response.status_code}: {response.text[:500]}"
            )
        if not response.content:
            return {}
        return response.json()

    def health(self) -> str:
        response = httpx.get(
            f"{self.public_base}/health",
            headers=self._headers,
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Airbyte health check failed HTTP {response.status_code}: {response.text[:200]}"
            )
        return response.text.strip() or "ok"

    def list_workspaces(self) -> list[dict[str, Any]]:
        payload = self._get(f"{self.public_base}/workspaces")
        return payload.get("data", [])

    def list_connections(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        payload = self._get(
            f"{self.public_base}/connections",
            {
                "limit": limit,
                "offset": offset,
                "includeDeleted": str(include_deleted).lower(),
            },
        )
        return payload.get("data", [])

    def get_connection(self, connection_id: str) -> dict[str, Any]:
        return self._get(f"{self.public_base}/connections/{connection_id}")

    def list_jobs(
        self,
        *,
        connection_id: str | None = None,
        status: str | None = None,
        job_type: str = "sync",
        limit: int = 20,
        offset: int = 0,
        order_by: str = DEFAULT_ORDER_BY,
        created_at_start: str | None = None,
        created_at_end: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "jobType": job_type,
            "orderBy": order_by,
        }
        if connection_id:
            params["connectionId"] = connection_id
        if status:
            params["status"] = status
        if created_at_start:
            params["createdAtStart"] = created_at_start
        if created_at_end:
            params["createdAtEnd"] = created_at_end

        payload = self._get(f"{self.public_base}/jobs", params)
        return payload.get("data", [])

    def get_job_public(self, job_id: int) -> dict[str, Any]:
        return self._get(f"{self.public_base}/jobs/{job_id}")

    def get_job_with_attempts(self, job_id: int) -> dict[str, Any]:
        return self._post(f"{self.internal_base}/jobs/get_without_logs", {"id": job_id})

    def cancel_job(self, job_id: int) -> dict[str, Any]:
        return self._post(f"{self.internal_base}/jobs/cancel", {"id": job_id})

    def trigger_sync(self, connection_id: str) -> dict[str, Any]:
        return self._post(
            f"{self.public_base}/jobs",
            {"connectionId": connection_id, "jobType": "sync"},
        )


def parse_envs(raw: str) -> dict[str, dict[str, str]]:
    if not raw:
        return {}
    try:
        envs = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "AIRBYTE_ENVS must be valid JSON "
            '(example: \'{"prod": {"url": "https://airbyte.example.com"}, '
            '"staging": {"url": "https://airbyte-staging.example.com"}}\').'
        ) from exc
    if not isinstance(envs, dict):
        raise RuntimeError("AIRBYTE_ENVS must be a JSON object mapping env names to configs.")
    return envs


def resolve_client(
    env: str | None,
    *,
    envs: dict[str, dict[str, str]],
    default_env: str,
    fallback_url: str,
    fallback_token: str,
) -> AirbyteClient:
    if not envs:
        return AirbyteClient(base_url=fallback_url, api_token=fallback_token)

    name = env or default_env
    if not name:
        if len(envs) == 1:
            name = next(iter(envs))
        else:
            raise RuntimeError(
                f"Multiple Airbyte envs configured but no env specified. "
                f"Available: {', '.join(envs)}. Pass env=<name> or set AIRBYTE_DEFAULT_ENV."
            )

    if name not in envs:
        raise RuntimeError(f"Unknown Airbyte env '{name}'. Available: {', '.join(envs)}.")

    cfg = envs[name]
    return AirbyteClient(
        base_url=cfg.get("url", fallback_url),
        api_token=cfg.get("token", fallback_token),
    )


def connection_index(connections: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for conn in connections:
        conn_id = conn.get("connectionId")
        name = conn.get("name")
        if conn_id:
            by_id[str(conn_id)] = conn
        if name:
            by_name[str(name)] = conn
    return {"by_id": by_id, "by_name": by_name}


def resolve_connection_ref(
    ref: str,
    index: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, Any]:
    if ref in index["by_id"]:
        return index["by_id"][ref]
    if ref in index["by_name"]:
        return index["by_name"][ref]
    needle = ref.lower()
    matches = [
        conn
        for name, conn in index["by_name"].items()
        if needle in name.lower()
    ]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        names = ", ".join(sorted(c["name"] for c in matches[:10]))
        raise RuntimeError(
            f"Connection ref '{ref}' matched multiple connections: {names}. "
            "Pass an exact connection name or UUID."
        )
    raise RuntimeError(f"Connection '{ref}' not found.")


def enrich_job(job: dict[str, Any], index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    conn_id = str(job.get("connectionId", ""))
    conn = index.get(conn_id, {})
    return {
        **job,
        "connectionName": conn.get("name"),
        "connectionStatus": conn.get("status"),
        "scheduleType": (conn.get("schedule") or {}).get("scheduleType"),
    }


def normalize_attempts(raw: dict[str, Any]) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for item in raw.get("attempts", []):
        attempt = item.get("attempt", item) if isinstance(item, dict) else item
        if not isinstance(attempt, dict):
            continue
        failures = (attempt.get("failureSummary") or {}).get("failures", [])
        attempts.append(
            {
                "attemptNumber": attempt.get("id"),
                "status": attempt.get("status"),
                "createdAt": attempt.get("createdAt"),
                "endedAt": attempt.get("endedAt"),
                "totalStats": attempt.get("totalStats"),
                "failures": failures,
            }
        )
    return attempts

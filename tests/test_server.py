import asyncio
import re
from unittest.mock import MagicMock

import httpx

from airbyte_mcp import server


class TestGetConnection:
    def test_returns_summarized_connection(self, mock_http, monkeypatch):
        conn_list = {
            "connectionId": "conn-1",
            "name": "my-connection",
            "status": "active",
        }
        conn_detail = {
            "connectionId": "conn-1",
            "name": "my-connection",
            "status": "active",
            "prefix": "pre_",
            "namespaceFormat": "ds",
            "configurations": {
                "streams": [
                    {
                        "name": "events",
                        "syncMode": "full_refresh_overwrite",
                        "cursorField": ["id"],
                    }
                ]
            },
        }
        jobs = [{"jobId": 1, "status": "succeeded", "connectionId": "conn-1"}]

        def route(url, **kwargs):
            response = MagicMock()
            response.status_code = 200
            if "/connections" in url and "/connections/" not in url:
                response.json.return_value = {"data": [conn_list]}
            elif url.endswith("/connections/conn-1"):
                response.json.return_value = conn_detail
            elif "/jobs" in url:
                response.json.return_value = {"data": jobs}
            else:
                response.json.return_value = {}
            response.content = b"{}"
            response.text = "{}"
            return response

        import httpx as httpx_mod

        httpx_mod.get = MagicMock(side_effect=route)
        result = server.get_connection("my-connection")
        assert "configurations" not in result["connection"]
        assert result["connection"]["streams"] == [
            {
                "name": "events",
                "namespace": "ds",
                "prefix": "pre_",
                "syncMode": "full_refresh_overwrite",
            }
        ]
        assert result["recentJobs"] == jobs


class TestListConnectionsPagination:
    def test_limit_above_page_size_fetches_multiple_pages(self, monkeypatch):
        def route(url, **kwargs):
            response = MagicMock()
            response.status_code = 200
            match = re.search(r"offset=(\d+)", url)
            offset = int(match.group(1)) if match else 0
            if offset == 0:
                data = [
                    {
                        "connectionId": f"id-{i}",
                        "name": f"conn-{i}",
                        "status": "active",
                    }
                    for i in range(100)
                ]
            elif offset == 100:
                data = [
                    {
                        "connectionId": f"id-{i}",
                        "name": f"conn-{i}",
                        "status": "active",
                    }
                    for i in range(100, 150)
                ]
            else:
                data = []
            response.json.return_value = {"data": data}
            response.content = b"{}"
            response.text = "{}"
            return response

        mock_get = MagicMock(side_effect=route)
        monkeypatch.setattr(httpx, "get", mock_get)
        result = server.list_connections(limit=150)
        assert len(result) == 150
        assert mock_get.call_count == 2


class TestGetInstanceStatus:
    def test_uses_bounded_fetches(self, monkeypatch):
        calls: list[str] = []

        def route(url, **kwargs):
            calls.append(url)
            response = MagicMock()
            response.status_code = 200
            if url.endswith("/health"):
                response.text = "Successful operation"
                response.content = response.text.encode()
            elif "/workspaces" in url:
                response.json.return_value = {"data": [{"workspaceId": "ws-1"}]}
                response.content = b"{}"
                response.text = "{}"
            elif "/connections" in url:
                response.json.return_value = {
                    "data": [
                        {
                            "connectionId": "c1",
                            "status": "active",
                            "schedule": {"scheduleType": "manual"},
                        }
                    ]
                }
                response.content = b"{}"
                response.text = "{}"
            elif "/jobs" in url:
                response.json.return_value = {
                    "data": [
                        {"jobId": 1, "status": "running"},
                        {"jobId": 2, "status": "failed"},
                    ]
                }
                response.content = b"{}"
                response.text = "{}"
            else:
                response.json.return_value = {}
                response.content = b"{}"
                response.text = "{}"
            return response

        monkeypatch.setattr(httpx, "get", MagicMock(side_effect=route))
        result = server.get_instance_status()
        assert result["healthy"] is True
        assert result["running_sync_count"] == 1
        assert result["recent_failed_sync_count"] == 1
        assert "job_counts_note" in result
        assert sum(1 for url in calls if "/jobs" in url) == 1
        assert not any("status=running" in url for url in calls)


class TestReadOnlyGating:
    def test_write_tools_not_registered_when_read_only(self):
        assert server.READ_ONLY is True
        tools = asyncio.run(server.mcp.list_tools())
        tool_names = {t.name for t in tools}
        assert "cancel_job" not in tool_names
        assert "trigger_sync" not in tool_names
        assert "get_active_syncs" not in tool_names
        assert "find_duplicate_destination_tables" not in tool_names
        assert "get_instance_status" in tool_names

    def test_write_tools_registered_when_not_read_only(self, monkeypatch):
        monkeypatch.setattr(server, "READ_ONLY", False)
        assert hasattr(server, "cancel_job")
        assert hasattr(server, "trigger_sync")

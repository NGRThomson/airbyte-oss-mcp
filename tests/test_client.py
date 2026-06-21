import re
from unittest.mock import MagicMock

import httpx

from airbyte_mcp.client import (
    AirbyteClient,
    _fetch_all_pages,
    _fetch_limited,
    summarize_connection,
)


class TestFetchAllPages:
    def test_empty(self):
        assert _fetch_all_pages(lambda limit, offset: []) == []

    def test_single_page(self):
        data = [{"id": i} for i in range(50)]
        assert _fetch_all_pages(lambda limit, offset: data if offset == 0 else []) == data

    def test_multiple_pages(self):
        def fetch(limit, offset):
            if offset == 0:
                return [{"id": i} for i in range(100)]
            if offset == 100:
                return [{"id": i} for i in range(100, 150)]
            return []

        result = _fetch_all_pages(fetch)
        assert len(result) == 150


class TestFetchLimited:
    def test_under_limit(self):
        data = [{"id": 1}, {"id": 2}]
        result = _fetch_limited(lambda limit, offset: data, limit=10)
        assert result == data

    def test_paginates_for_high_limit(self):
        calls = []

        def fetch(limit, offset):
            calls.append((limit, offset))
            if offset == 0:
                return [{"id": i} for i in range(100)]
            if offset == 100:
                return [{"id": i} for i in range(100, 180)]
            return []

        result = _fetch_limited(fetch, limit=250)
        assert len(result) == 180
        assert calls == [(100, 0), (100, 100)]

    def test_offset_skips_first_page(self):
        def fetch(limit, offset):
            if offset == 10:
                return [{"id": 10}, {"id": 11}]
            return []

        result = _fetch_limited(fetch, limit=5, offset=10)
        assert result == [{"id": 10}, {"id": 11}]


class TestSummarizeConnection:
    def test_strips_raw_stream_fields(self):
        detail = {
            "connectionId": "abc",
            "name": "test-conn",
            "status": "active",
            "prefix": "webapp_",
            "namespaceFormat": "raw_data",
            "configurations": {
                "streams": [
                    {
                        "name": "users",
                        "syncMode": "incremental_append",
                        "cursorField": ["id"],
                        "primaryKey": [["id"]],
                        "mappers": [],
                    }
                ]
            },
        }
        summary = summarize_connection(detail)
        assert "configurations" not in summary
        assert summary["streams"] == [
            {
                "name": "users",
                "namespace": "raw_data",
                "prefix": "webapp_",
                "syncMode": "incremental_append",
            }
        ]
        assert "cursorField" not in summary["streams"][0]


class TestListAllConnectionsPagination:
    def test_fetches_multiple_pages(self, monkeypatch):
        def route(url, **kwargs):
            response = MagicMock()
            response.status_code = 200
            match = re.search(r"offset=(\d+)", url)
            offset = int(match.group(1)) if match else 0
            if offset == 0:
                data = [{"connectionId": f"id-{i}"} for i in range(100)]
            elif offset == 100:
                data = [{"connectionId": f"id-{i}"} for i in range(100, 120)]
            else:
                data = []
            response.json.return_value = {"data": data}
            response.content = b"{}"
            response.text = "{}"
            return response

        monkeypatch.setattr(httpx, "get", MagicMock(side_effect=route))
        client = AirbyteClient("http://test-airbyte:8000")
        connections = client.list_all_connections()
        assert len(connections) == 120

import re
from unittest.mock import MagicMock

import httpx

from airbyte_mcp.client import (
    AirbyteClient,
    _fetch_all_pages,
    _fetch_limited,
    bq_destination_key,
    find_duplicate_destination_tables,
    is_bigquery_destination,
    resolve_bq_dataset,
    resolve_bq_table_name,
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


class TestBigQueryHelpers:
    def test_bq_destination_key(self):
        assert bq_destination_key("proj", "ds", "tbl") == "proj.ds.tbl"

    def test_is_bigquery_destination_by_type(self):
        assert is_bigquery_destination({"destinationType": "bigquery"}) is True
        assert is_bigquery_destination({"destinationType": "snowflake"}) is False

    def test_is_bigquery_destination_by_config(self):
        assert is_bigquery_destination(
            {"configuration": {"project_id": "my-proj"}}
        ) is True

    def test_resolve_bq_dataset_destination_namespace(self):
        conn = {"namespaceDefinition": "destination"}
        stream = {"name": "events"}
        dest_cfg = {"dataset_id": "raw_data"}
        assert resolve_bq_dataset(conn, stream, dest_cfg) == "raw_data"

    def test_resolve_bq_dataset_source_namespace(self):
        conn = {"namespaceDefinition": "source"}
        stream = {"name": "events", "namespace": "salesforce"}
        dest_cfg = {"dataset_id": "raw_data"}
        assert resolve_bq_dataset(conn, stream, dest_cfg) == "salesforce"

    def test_resolve_bq_dataset_custom_format(self):
        conn = {
            "namespaceDefinition": "custom_format",
            "namespaceFormat": "${SOURCE_NAMESPACE}",
        }
        stream = {"name": "events", "namespace": "schema_a"}
        dest_cfg = {"dataset_id": "raw_data"}
        assert resolve_bq_dataset(conn, stream, dest_cfg) == "schema_a"

    def test_resolve_bq_table_name_with_prefix(self):
        conn = {"prefix": "webapp_"}
        stream = {"name": "users"}
        assert resolve_bq_table_name(conn, stream) == "webapp_users"

    def test_resolve_bq_table_name_destination_object(self):
        conn = {"prefix": "webapp_"}
        stream = {"name": "users", "destination_object_name": "custom_users"}
        assert resolve_bq_table_name(conn, stream) == "custom_users"


class TestFindDuplicateDestinationTables:
    def test_detects_duplicates(self, mock_http):
        conn_a = {
            "connectionId": "conn-a",
            "name": "writer_a",
            "status": "active",
            "destinationId": "dest-1",
            "schedule": {"scheduleType": "manual"},
            "prefix": "webapp_",
            "namespaceDefinition": "destination",
            "configurations": {"streams": [{"name": "users", "syncMode": "full_refresh"}]},
        }
        conn_b = {
            "connectionId": "conn-b",
            "name": "writer_b",
            "status": "active",
            "destinationId": "dest-1",
            "schedule": {"scheduleType": "cron"},
            "prefix": "webapp_",
            "namespaceDefinition": "destination",
            "configurations": {"streams": [{"name": "users", "syncMode": "incremental"}]},
        }
        inactive = {
            "connectionId": "conn-c",
            "name": "inactive",
            "status": "inactive",
            "destinationId": "dest-1",
        }
        dest = {
            "name": "bq-dest",
            "destinationType": "bigquery",
            "configuration": {"project_id": "my-proj", "dataset_id": "raw_data"},
        }

        def route(url, **kwargs):
            response = MagicMock()
            response.status_code = 200
            if "/connections" in url and "/connections/" not in url:
                response.json.return_value = {"data": [conn_a, conn_b, inactive]}
                response.content = b"{}"
                response.text = "{}"
            elif url.endswith("/connections/conn-a"):
                response.json.return_value = conn_a
            elif url.endswith("/connections/conn-b"):
                response.json.return_value = conn_b
            elif "/destinations/dest-1" in url:
                response.json.return_value = dest
            else:
                response.json.return_value = {}
            response.content = b"{}"
            response.text = "{}"
            return response

        mock_get = MagicMock(side_effect=route)
        mock_http({"": None})
        import httpx as httpx_mod

        httpx_mod.get = mock_get

        client = AirbyteClient("http://test-airbyte:8000")
        result = find_duplicate_destination_tables(client)
        assert result["active_connection_count"] == 2
        assert result["duplicate_destination_table_count"] == 1
        key = "my-proj.raw_data.webapp_users"
        assert key in result["duplicates"]
        assert len(result["duplicates"][key]) == 2

    def test_skips_non_bigquery(self, mock_http):
        conn = {
            "connectionId": "conn-a",
            "name": "snowflake_writer",
            "status": "active",
            "destinationId": "dest-sf",
            "configurations": {"streams": [{"name": "users"}]},
        }
        dest = {
            "name": "sf-dest",
            "destinationType": "snowflake",
            "configuration": {"host": "sf.example.com"},
        }

        def route(url, **kwargs):
            response = MagicMock()
            response.status_code = 200
            if "/connections" in url and "/connections/" not in url:
                response.json.return_value = {"data": [conn]}
            elif url.endswith("/connections/conn-a"):
                response.json.return_value = conn
            elif "/destinations/dest-sf" in url:
                response.json.return_value = dest
            else:
                response.json.return_value = {"data": []}
            response.content = b"{}"
            response.text = "{}"
            return response

        import httpx as httpx_mod

        httpx_mod.get = MagicMock(side_effect=route)
        client = AirbyteClient("http://test-airbyte:8000")
        result = find_duplicate_destination_tables(client)
        assert result["duplicate_destination_table_count"] == 0
        assert result["unique_destination_tables"] == 0


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

# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-06-20

### Added

- `find_duplicate_destination_tables` MCP tool (BigQuery)
- Client pagination (`list_all_connections`, `list_all_jobs`, `list_*_limited`)
- pytest suite + GitHub Actions CI (`uv sync`, `pytest`, `ruff`)
- `examples/cursor-mcp.json`

### Changed

- `get_connection` returns stream summaries instead of full config blob
- `list_connections` / `list_jobs` paginate beyond the 100-item API page size
- `_connection_maps` uses full connection list (no silent 100-cap)

### Notes

- Internal OSS API (`/api/v1/jobs/get_without_logs`) may break on Airbyte upgrades

## [0.1.0] - 2026-06-19

### Added

- Initial release: read-only MCP server for Airbyte OSS 1.6.x
- Tools: `get_instance_status`, `list_connections`, `get_connection`, `list_jobs`,
  `get_active_syncs`, `get_job`, `get_job_details`, `get_job_failure_summary`
- Optional write tools (`cancel_job`, `trigger_sync`) when `AIRBYTE_READ_ONLY=false`
- Multi-env support via `AIRBYTE_ENVS`

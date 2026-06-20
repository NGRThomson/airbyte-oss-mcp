import json
from unittest.mock import MagicMock

import httpx
import pytest

from airbyte_mcp import server as _server_mod


@pytest.fixture(autouse=True)
def env_defaults(monkeypatch):
    """Set clean env vars before every test."""
    monkeypatch.setenv("AIRBYTE_URL", "http://test-airbyte:8000")
    monkeypatch.delenv("AIRBYTE_API_TOKEN", raising=False)
    monkeypatch.setenv("AIRBYTE_READ_ONLY", "true")
    monkeypatch.delenv("AIRBYTE_ENVS", raising=False)
    monkeypatch.delenv("AIRBYTE_DEFAULT_ENV", raising=False)
    _server_mod.READ_ONLY = True
    _server_mod._ENVS = {}


@pytest.fixture
def mock_http(monkeypatch):
    """Patch httpx.get/post; route responses by URL path substring."""

    routes: dict[str, object] = {}

    def _setup(route_map: dict[str, object]) -> MagicMock:
        routes.clear()
        routes.update(route_map)

        def _get(url, **kwargs):
            response = MagicMock()
            for pattern, data in routes.items():
                if pattern in url:
                    if isinstance(data, Exception):
                        raise data
                    response.status_code = 200
                    if isinstance(data, str):
                        response.text = data
                        response.content = data.encode()
                    else:
                        response.text = json.dumps(data)
                        response.content = response.text.encode()
                        response.json.return_value = data
                    return response
            response.status_code = 404
            response.text = f"unmocked GET {url}"
            response.content = response.text.encode()
            response.json.return_value = {}
            return response

        def _post(url, **kwargs):
            return _get(url, **kwargs)

        mock_get = MagicMock(side_effect=_get)
        mock_post = MagicMock(side_effect=_post)
        monkeypatch.setattr(httpx, "get", mock_get)
        monkeypatch.setattr(httpx, "post", mock_post)
        return mock_get

    return _setup

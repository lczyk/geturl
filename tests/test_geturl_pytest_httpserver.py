import pytest
from geturl import get_with_retry, handle_code

try:
    from pytest_httpserver import HTTPServer
except ImportError:
    pytest.skip("pytest-httpserver is not installed", allow_module_level=True)


def test_geturl_httpserver(httpserver: HTTPServer) -> None:
    httpserver.expect_request("/").respond_with_data("Hello, world!", status=200)

    code, result = get_with_retry(httpserver.url_for("/"))
    assert code == 200
    handle_code(code, result)

    assert result == b"Hello, world!"
    assert isinstance(result, bytes)
    assert result.decode("utf-8") == "Hello, world!"


def test_geturl_httpserver_404(httpserver: HTTPServer) -> None:
    httpserver.expect_request("/").respond_with_data("Not found", status=404)

    code, result = get_with_retry(httpserver.url_for("/"))
    assert code == 404
    with pytest.raises(ConnectionError):
        handle_code(code, result)

from pytest_httpserver import HTTPServer

from geturl import geturl_with_retry, handle_code


def test_headers(httpserver: HTTPServer) -> None:
    httpserver.expect_request("/", headers={"X-Test-Header": "TestValue"}).respond_with_data(
        "Header received", status=200
    )

    code, result = geturl_with_retry(
        httpserver.url_for("/"),
        headers={"X-Test-Header": "TestValue"},
    )
    assert code == 200
    handle_code(code, result)

    assert result == b"Header received"
    assert isinstance(result, bytes)
    assert result.decode("utf-8") == "Header received"

from pytest_httpserver import HTTPServer

from geturl import geturl_with_retry, handle_code


def test_params(httpserver: HTTPServer) -> None:
    httpserver.expect_request("/", query_string="key=value").respond_with_data("Params received", status=200)

    code, result = geturl_with_retry(
        httpserver.url_for("/"),
        params={"key": "value"},
    )
    assert code == 200
    handle_code(code, result)

    assert result == b"Params received"
    assert isinstance(result, bytes)
    assert result.decode("utf-8") == "Params received"

from geturl import get_with_retry, handle_code


def test_geturl_google() -> None:
    code, result = get_with_retry("https://www.google.com")
    assert code == 200
    handle_code(code, result)

    assert isinstance(result, bytes)
    result_str = result.decode("utf-8")
    assert "<!doctype html>" in result_str.lower()

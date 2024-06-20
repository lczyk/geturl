"""
Single-file module for making GET requests with retries and exponential backoff.

Written by Marcin Konowalczyk.
"""

import logging
import time
import urllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Generator, Mapping
from typing import Any, Callable, Optional, Protocol, TypeVar, overload

__version__ = "0.1.0"

logger = logging.getLogger("geturl")

GET_N_RETRIES = 10  # ms
"""Number of times to retry a GET request before giving up."""

GET_DELAY = 1_000  # ms
"""Initial delay between GET requests, in milliseconds."""

GET_MAX_DELAY = 30_000  # ms
"""Maximum delay between GET requests, in milliseconds."""

__all__ = [
    "get_with_retry",
    "handle_code",
]


def add_params_to_url(url: str, params: Optional[Mapping[str, Any]] = None) -> str:
    """Add query parameters to a URL. If the URL already has query parameters, they are preserved."""
    if params is None:
        return url

    # Check if url already has query parameters
    if "?" in url:
        params = dict(params)  # make a modifiable copy
        existing_params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        for key, value in existing_params.items():
            if key in params:
                # If the key is already in params, don't overwrite it
                continue
            params[key] = value

        url = url.split("?")[0]

    return url + "?" + urllib.parse.urlencode(params)


def get(url: str, params: Optional[Mapping[str, Any]] = None) -> tuple[bytes, int]:
    url = add_params_to_url(url, params)

    code: int
    res: bytes

    try:
        with urllib.request.urlopen(url) as r:
            code = r.getcode()
            res = r.read()

    except urllib.error.HTTPError as e:
        code = e.code
        res = e.read()

    assert isinstance(code, int), "Expected code to be int."
    assert isinstance(res, bytes), "Expected response to be bytes."

    return res, code


def exponential_backoff(
    initial_delay: int,
    max_delay: int,
    factor: float = 2.0,
    start_at_zero: bool = False,
) -> Generator[int, None, None]:
    """Generator that yields exponentially increasing delays, capped at max_delay."""

    if start_at_zero:
        yield 0

    delay = initial_delay
    while True:
        yield delay
        delay = min(int(delay * factor), max_delay)


def _get_with_retry(
    url: str,
    params: Optional[Mapping[str, Any]] = None,
    n_retries: int = GET_N_RETRIES,
    retry_delay: int = GET_DELAY,
    max_delay: int = GET_MAX_DELAY,
) -> tuple[bytes, int]:
    delay_gen = exponential_backoff(retry_delay, max_delay, start_at_zero=True)

    if n_retries < 1:
        raise ValueError(f"Expected n_retries to be at least 1, got {n_retries}")

    response: bytes = b""
    code: int = -1

    for _ in range(n_retries):
        # Wait before making request
        delay = next(delay_gen)
        time.sleep(delay / 1000)
        if delay > 0:
            logger.info(f"Retrying in {delay} ms")

        response, code = get(url, params)

        if code == 204:
            # No content
            response = b""
            break
        elif code == 200:
            # Success
            break
        elif code == 429:
            # Too many requests
            message = f"Got HTTPError 429 for {url}"
            logger.warning(message)
        elif 400 <= code < 500:
            # Client error
            return response, code
        elif code == 501:
            # Not implemented. No point retrying.
            # TODO: Should we actually be retrying  this?
            return response, code
        elif 500 <= code < 600:
            # Some other server error
            message = f"Got HTTPError {code} for {url}"
            logger.warning(message)
        else:
            # Unexpected error
            return response, code

    else:
        # loop ended without breaking
        return response, code
        # raise ConnectionError(f"Failed to get {url} after {n_retries} attempts")

    assert isinstance(response, bytes), f"Expected bytes, got {type(response)}"
    return response, code


class _MemorizedFunc(Protocol):
    def call(self, *args: Any, **kwargs: Any) -> Any: ...

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class MemoryProtocol(Protocol):
    def cache(self, fun: Callable) -> _MemorizedFunc: ...


def get_with_retry(
    url: str,
    params: Optional[Mapping[str, Any]] = None,
    *,
    n_retries: int = GET_N_RETRIES,
    retry_delay: int = GET_DELAY,
    max_delay: int = GET_MAX_DELAY,
    memory: Optional[MemoryProtocol] = None,
    refresh_cache: bool = False,
) -> tuple[bytes, int]:
    """Get the response from the given URL, with retries. Optionally use memoization."""

    if memory is not None:
        memoized_fun = memory.cache(_get_with_retry)
        if refresh_cache:
            # don't use memoized version. just call get directly. this will also refresh the cached version
            (response, code), _ = memoized_fun.call(url, params, n_retries, retry_delay, max_delay)
        else:
            response, code = memoized_fun(url, params, n_retries, retry_delay, max_delay)
        assert isinstance(response, bytes), f"Expected bytes, got {type(response)}"
        assert isinstance(code, int), f"Expected int, got {type(code)}"
    else:
        # no memoization
        response, code = _get_with_retry(url, params, n_retries, retry_delay, max_delay)

    return response, code


_T_HandlerReturn = TypeVar("_T_HandlerReturn")


@overload
def handle_code(
    response: bytes,
    code: int,
    handlers: Mapping[int, Callable[[bytes], _T_HandlerReturn]],
) -> _T_HandlerReturn: ...


@overload
def handle_code(
    response: bytes,
    code: int,
    handlers: None = None,
) -> None: ...


def handle_code(
    response: bytes,
    code: int,
    handlers: Optional[Mapping[int, Callable[[bytes], _T_HandlerReturn]]] = None,
) -> Optional[_T_HandlerReturn]:
    if handlers is not None and code in handlers:
        # Call the handler for this code
        return handlers[code](response)

    if code == 204:
        # No content
        pass
    elif code == 200:
        # Success
        pass
    elif code == 429:
        # Too many requests
        raise ConnectionError("Too many requests")
    elif 400 <= code < 500:
        # Client error
        raise ConnectionError(f"Got HTTP Error {code}")
    elif code == 501:
        # Not implemented. No point retrying.
        raise ConnectionError("Got HTTP Error 501 (not implemented)")
    elif 500 <= code < 600:
        # Some other server error
        raise ConnectionError(f"Got HTTP Error {code}")
    else:
        # Unexpected error
        raise ConnectionError(f"Unexpected HTTP Error {code}")

    return None

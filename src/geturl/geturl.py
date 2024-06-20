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
from typing import Any, Callable, NoReturn, Optional, Protocol, TypeVar, Union, cast, overload

__version__ = "0.2.0"

logger = logging.getLogger("geturl")

GET_N_RETRIES = 10
"""Number of times to retry a GET request before giving up."""

GET_DELAY = 1.0  # s
"""Initial delay between GET requests, in milliseconds."""

GET_MAX_DELAY = 30.0  # s
"""Maximum delay between GET requests, in milliseconds."""

__all__ = [
    "get",
    "get_with_retry",
    "GET_N_RETRIES",
    "GET_DELAY",
    "GET_MAX_DELAY",
    "add_params_to_url",
    "exponential_backoff",
    "handle_code",
    "ALL_CODES",
    "DEFAULT_HANDLERS",
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
    initial_delay: float,
    start_at_zero: bool = False,
    max_delay: Optional[float] = None,
    factor: float = 2.0,
) -> Generator[float, None, None]:
    """Generator that yields exponentially increasing delays.
    If `start_at_zero` is True, the first delay will be 0.
    If `max_delay` is specified, the delays will not exceed it.
    The delays are multiplied by `factor` each time (defaults to 2).
    """

    if start_at_zero:
        yield 0.0

    delay = initial_delay
    while True:
        yield delay
        delay = min(delay * factor, max_delay) if max_delay is not None else delay * factor


def _get_with_retry(
    url: str,
    params: Optional[Mapping[str, Any]] = None,
    n_retries: int = GET_N_RETRIES,
    retry_delay: float = GET_DELAY,
    max_delay: float = GET_MAX_DELAY,
) -> tuple[bytes, int]:
    delay_gen = exponential_backoff(retry_delay, max_delay=max_delay, start_at_zero=True)

    if n_retries < 1:
        raise ValueError(f"Expected n_retries to be at least 1, got {n_retries}")

    response: bytes = b""
    code: int = -1

    for _ in range(n_retries):
        # Wait before making request
        delay = next(delay_gen)
        time.sleep(delay)
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
    retry_delay: float = GET_DELAY,
    max_delay: float = GET_MAX_DELAY,
    memory: Optional[MemoryProtocol] = None,
    refresh_cache: bool = False,
) -> tuple[int, bytes]:
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

    return code, response


class Slice:
    """Like a slice, but hashable and with a __contains__ method."""

    __slots__ = ("start", "stop")

    start: int
    stop: int

    def __init__(self, start: int, stop: int) -> None:
        self.start = start
        self.stop = stop

    def __contains__(self, item: int) -> bool:
        return item >= self.start and item < self.stop

    @classmethod
    def from_slice(cls, s: slice) -> "Slice":
        if s.step is not None:
            raise ValueError("Slice with step not supported")
        return cls(s.start, s.stop)


ALL_CODES = cast(int, object())
"""A sentinel value to indicate that the handler should be called for all codes."""


def _raise(error: Exception) -> NoReturn:
    raise error


_T_HandlerReturn = TypeVar("_T_HandlerReturn", covariant=True)
Handlers = Mapping[Union[int, Slice], Callable[[int, Optional[bytes]], _T_HandlerReturn]]

DEFAULT_HANDLERS: Handlers[None] = {
    # Success
    200: lambda _c, _r: None,
    # No content
    204: lambda _c, _r: None,
    # Success of some other kind
    Slice(200, 300): lambda _c, _r: None,
    # Too many requests
    429: lambda _c, _r: _raise(ConnectionError("Too many requests")),
    # Not implemented
    501: lambda _c, _r: _raise(ConnectionError("Got HTTP Error 501 (not implemented)")),
    # Any other error with code in error range
    Slice(100, 600): lambda code, _r: _raise(ConnectionError(f"Got HTTP Error {code}")),
    # Any other error
    ALL_CODES: lambda code, _r: _raise(ConnectionError(f"Unexpected HTTP Error {code}")),
}


@overload
def handle_code(
    code: int,
    response: Optional[bytes],
    handlers: Handlers[_T_HandlerReturn],
) -> _T_HandlerReturn: ...


@overload
def handle_code(
    code: int,
    response: Optional[bytes],
    handlers: None = None,
) -> None: ...


def handle_code(
    code: int,
    response: Optional[bytes] = None,
    handlers: Optional[Handlers[_T_HandlerReturn]] = None,
) -> Optional[_T_HandlerReturn]:
    """Dispatch the response to the appropriate handler based on the HTTP status code.
    The handlers are checked in order, and the first one that matches is called.
    """

    if handlers is None:
        handlers = cast(Handlers[_T_HandlerReturn], DEFAULT_HANDLERS)

    # Iterate through handlers in order
    for key, handler in handlers.items():
        if isinstance(key, int) and code == key:
            return handler(code, response)

        if isinstance(key, Slice) and code in key:
            return handler(code, response)

        if key == ALL_CODES:
            return handler(code, response)

    return None

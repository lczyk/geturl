"""
Single-file module for making GET requests with retries and exponential backoff.

Basic example:

    ```python
    from geturl import geturl, handle_code

    code, result = geturl("https://www.google.com")
    handle_code(code)
    ```

Advanced example:

    ```python
    from geturl import geturl_with_retry, handle_code
    from geturl import DEFAULT_HANDLERS
    from collections import ChainMap
    code, result = geturl_with_retry(
        "https://www.google.com",
        params={"q": "python"},
        n_retries=3,
    )
    handle_code(code, result, handlers=ChainMap({200: lambda c, r: print("OK!")}, DEFAULT_HANDLERS))
    ```

Example with memorization:
    
    ```python
    from geturl import geturl_with_retry, handle_code
    from geturl import Memory

    memory = Memory(location="/tmp/geturl_memory")
    code, result = geturl_with_retry(
        "https://www.google.com",
        params={"q": "python"},
        n_retries=3,
        memory=memory,
    )
    handle_code(code, result)
    ```

This is a single-file module. It does not depend on any other files or external packages.
Its version is tracked internally in a separate repository. It can be used as a package,
or the file can be copied into a project and used directly. In the latter case, any
bugs/updates ought to be copied back to the original repository.

Written by Marcin Konowalczyk.
"""

__version__ = "0.4.1"

import os
import time
import urllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Generator, Mapping
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, NoReturn, Optional, Protocol, TypeVar, Union, cast, overload

if TYPE_CHECKING:
    # import like this not to acquire a dependency on typing_extensions
    from typing_extensions import override
else:
    override = lambda f: f

################################################################################

_logger: "Optional[logging.Logger]" = None

_debug = lambda msg, *args: _logger.log(10, msg, *args, stacklevel=2) if _logger else None
_info = lambda msg, *args: _logger.log(20, msg, *args, stacklevel=2) if _logger else None

if os.environ.get("GETURL_DEBUG", False):
    import logging

    _logger = logging.getLogger("geturl")
    handler = logging.StreamHandler()
    # format = "[%(levelname)s/%(processName)s/%(threadName)s] %(message)s"
    format = "[%(levelname)s] %(message)s"
    try:
        import colorlog

        handler.setFormatter(colorlog.ColoredFormatter("%(log_color)s" + format))
    except ImportError:
        formatter = logging.Formatter(format)
        handler.setFormatter(formatter)
    _logger.addHandler(handler)
    _logger.setLevel(logging.DEBUG)
    _info("geturl module loaded")

################################################################################

GETURL_N_RETRIES = 10
"""Number of times to retry a GET request before giving up."""

GETURL_DELAY = 1.0  # s
"""Initial delay between GET requests, in milliseconds."""

GETURL_MAX_DELAY = 30.0  # s
"""Maximum delay between GET requests, in milliseconds."""

__all__ = [
    "ALL_CODES",
    "DEFAULT_HANDLERS",
    "GETURL_DELAY",
    "GETURL_MAX_DELAY",
    "GETURL_N_RETRIES",
    "Memory",
    "MemoryProtocol",
    "add_params_to_url",
    "exponential_backoff",
    "geturl",
    "geturl_with_retry",
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


def geturl(url: str, params: Optional[Mapping[str, Any]] = None) -> tuple[int, bytes]:
    """Make a GET request to a URL and return the response and status code."""

    url = add_params_to_url(url, params)

    try:
        with urllib.request.urlopen(url) as r:
            code = r.getcode()
            res = r.read()

    except urllib.error.HTTPError as e:
        code = e.code
        res = e.read()

    assert isinstance(code, int), "Expected code to be int."
    assert isinstance(res, bytes), "Expected response to be bytes."

    return code, res


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


def _geturl_with_retry(
    url: str,
    params: Optional[Mapping[str, Any]] = None,
    n_retries: int = GETURL_N_RETRIES,
    retry_delay: float = GETURL_DELAY,
    max_delay: float = GETURL_MAX_DELAY,
    logger: "Optional[logging.Logger]" = None,
) -> tuple[bytes, int]:
    delay_gen = exponential_backoff(retry_delay, max_delay=max_delay, start_at_zero=True)

    if n_retries < 1:
        raise ValueError(f"Expected n_retries to be at least 1, got {n_retries}")

    response: bytes = b""
    code: int = -1

    for _ in range(n_retries):
        delay = next(delay_gen)
        time.sleep(delay)
        if delay > 0 and logger is not None:
            logger.info(f"Retrying in {delay} ms")

        code, response = geturl(url, params)

        if code == 204:
            # No content
            response = b""
            break
        elif code == 200:
            # Success
            break
        elif code == 429:
            # Too many requests. Retry anyway and lean on the backoff.
            pass
        elif 400 <= code < 500:
            # Client error
            return response, code
        elif code == 501:
            # Not implemented. No point retrying.
            # TODO: Should we actually be retrying this?
            return response, code
        elif 500 <= code < 600:
            # Server error. Retry.
            # TODO: Should we be retrying 503 Service Unavailable?
            pass
        else:
            # Unexpected error
            return response, code

    else:
        # loop ended without breaking
        return response, code
        # raise ConnectionError(f"Failed to get {url} after {n_retries} attempts")

    assert isinstance(response, bytes), f"Expected bytes, got {type(response)}"
    return response, code


class MemorizedFuncProtocol(Protocol):
    def call(self, *args: Any, **kwargs: Any) -> tuple[Any, Any]: ...

    """Force the execution of the function with the given arguments.

    The output values will be persisted, i.e., the cache will be updated
    with any new values."""

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

    """Return the output values from the cache, if they exist."""


class MemoryProtocol(Protocol):
    def cache(self, func: Callable) -> Union[MemorizedFuncProtocol, partial]: ...


if TYPE_CHECKING:
    from joblib import Memory as _joblib_Memory  # pyright: ignore[reportMissingImports]

    _memory: MemoryProtocol = _joblib_Memory()


def geturl_with_retry(
    url: str,
    params: Optional[Mapping[str, Any]] = None,
    *,
    n_retries: int = GETURL_N_RETRIES,
    retry_delay: float = GETURL_DELAY,
    max_delay: float = GETURL_MAX_DELAY,
    logger: "Optional[logging.Logger]" = None,
    memory: Optional[MemoryProtocol] = None,
    refresh_cache: bool = False,
) -> tuple[int, bytes]:
    """Get the response from the given URL, with retries. Optionally use memoization."""

    if memory is not None:
        memoized_fun = memory.cache(_geturl_with_retry)
        if refresh_cache:
            # don't use memoized version. just call get directly. this will also refresh the cached version
            if isinstance(memoized_fun, partial):
                response, code = memoized_fun.func(url, params, n_retries, retry_delay, max_delay, logger)
            else:
                (response, code), _ = memoized_fun.call(url, params, n_retries, retry_delay, max_delay, logger)
        else:
            response, code = memoized_fun(url, params, n_retries, retry_delay, max_delay, logger)
        assert isinstance(response, bytes), f"Expected bytes, got {type(response)}"
        assert isinstance(code, int), f"Expected int, got {type(code)}"
    else:
        # no memoization
        response, code = _geturl_with_retry(url, params, n_retries, retry_delay, max_delay, logger)

    return code, response


class Slice:
    """Like a slice, but hashable and with a __contains__ method."""

    __slots__ = ("start", "stop")

    start: int
    stop: int

    @overload
    def __init__(self, start: int, stop: int) -> None: ...

    @overload
    def __init__(self, start: slice) -> None: ...

    @overload
    def __init__(self, start: "Slice") -> None: ...

    def __init__(self, start: Union[int, slice, "Slice"], stop: Optional[int] = None) -> None:
        if stop is not None:
            if not isinstance(start, int):
                raise TypeError("Expected int for start, got {type(s)}")
            self.start = start
            self.stop = stop

        elif isinstance(start, slice):
            if start.step is not None:
                raise ValueError("Slice with step not supported")

            self.start = start.start
            self.stop = start.stop

        elif isinstance(start, Slice):
            # Copy constructor
            self.start = start.start
            self.stop = start.stop

        else:
            raise TypeError("Expected int or slice for start, got {type(s)}")

    def __contains__(self, item: int) -> bool:
        return item >= self.start and item < self.stop


ALL_CODES = cast(int, object())
"""A sentinel value to indicate that the handler should be called for all codes."""


_T_HandlerReturn = TypeVar("_T_HandlerReturn", covariant=True)
Handler = Callable[[int, Optional[bytes]], _T_HandlerReturn]
Handlers = Mapping[Union[int, Slice], Handler[_T_HandlerReturn]]


def _get_handler(
    code: int,
    handlers: Handlers[_T_HandlerReturn],
) -> Optional[Handler[_T_HandlerReturn]]:
    for key, handler in handlers.items():
        if isinstance(key, int) and code == key:
            return handler

        if isinstance(key, (slice, Slice)):
            key = Slice(key)
            if code in key:
                return handler

        if key == ALL_CODES:
            return handler

    return None


def _connection_error(s: str) -> NoReturn:
    raise ConnectionError(s)


DEFAULT_HANDLERS: Handlers[None] = {
    # Success
    200: lambda _c, _r: None,
    # No content
    204: lambda _c, _r: None,
    # Success of some other kind
    Slice(200, 300): lambda _c, _r: None,
    # Too many requests
    429: lambda _c, _r: _connection_error("Too many requests"),
    # Not implemented
    501: lambda _c, _r: _connection_error("Got HTTP Error 501 (not implemented)"),
    # Any other error with code in error range
    Slice(100, 600): lambda code, _r: _connection_error(f"Got HTTP Error {code}"),
    # Any other error
    ALL_CODES: lambda code, _r: _connection_error(f"Unexpected HTTP Error {code}"),
}


@overload
def handle_code(code: int) -> None: ...


@overload
def handle_code(code: int, *, handlers: None = None) -> None: ...


@overload
def handle_code(code: int, *, handlers: Handlers[_T_HandlerReturn]) -> _T_HandlerReturn: ...


@overload
def handle_code(code: int, response: Optional[bytes]) -> None: ...


@overload
def handle_code(code: int, response: Optional[bytes], handlers: None = None) -> None: ...


@overload
def handle_code(code: int, response: Optional[bytes], handlers: Handlers[_T_HandlerReturn]) -> _T_HandlerReturn: ...


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

    handler = _get_handler(code, handlers)
    if handler is not None:
        return handler(code, response)

    return None


##### Memoization #####

import hashlib
import pickle
from pathlib import Path


class Memory:
    """Simple non-joblib-dependent version of joblib.Memory."""

    def __init__(self, location: Union[str, Path]):
        self.location = Path(location)

    def cache(self, func: Callable) -> Union[MemorizedFuncProtocol, partial]:
        return MemorizedFunc(func, location=self.location)


if TYPE_CHECKING:
    _Memory: MemoryProtocol = Memory(location="/tmp/geturl_cache")


class MemorizedFunc:
    """Memoized function with a cache."""

    def __init__(self, func: Callable, location: Union[str, Path]):
        self.func = func
        self.location = Path(location)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._cached_call(args, kwargs)

    def _is_in_cache(self, call_id: str) -> bool:
        return (self.location / call_id).exists()

    def _cached_call(self, args: tuple[Any], kwargs: dict[str, Any]) -> Any:
        call_id = self._get_call_id(args, kwargs)
        if self._is_in_cache(call_id):
            try:
                return self._load_item(call_id)
            except Exception:
                pass
        return self._call(call_id, args, kwargs)

    def call(self, *args: Any, **kwargs: Any) -> tuple[Any, Any]:
        call_id = self._get_call_id(args, kwargs)
        return self._call(call_id, args, kwargs), None

    def _call(self, call_id: str, args: tuple, kwargs: dict) -> Any:
        output = self.func(*args, **kwargs)
        self._save_item(call_id, output)
        return output

    def _get_call_id(self, args: tuple[Any], kwargs: dict[str, Any]) -> str:
        hasher = hashlib.md5(usedforsecurity=False)
        hasher.update(self.func.__name__.encode())
        hasher.update(self.func.__code__.co_code)
        for arg in args:
            hasher.update(str(arg).encode())
        for key, value in kwargs.items():
            hasher.update(str(key).encode())
            hasher.update(str(value).encode())
        return hasher.hexdigest()

    def _save_item(self, call_id: str, item: Any) -> None:
        path = self.location / call_id
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(item, f)

    def _load_item(self, call_id: str) -> Any:
        path = self.location / call_id
        with open(path, "rb") as f:
            return pickle.load(f)


if TYPE_CHECKING:
    _MemorizedFunc: MemorizedFuncProtocol = MemorizedFunc(lambda x: x, location="/tmp/geturl_cache")

__license__ = """
Copyright 2024 Marcin Konowalczyk

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

2.  Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.

3.  Neither the name of the copyright holder nor the names of its
    contributors may be used to endorse or promote products derived from
    this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
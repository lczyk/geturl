from typing import TYPE_CHECKING

from geturl import DEFAULT_HANDLERS, handle_code

if TYPE_CHECKING:
    # Make sure all the overloads are correct
    handle_code(0)
    handle_code(0, b"")
    handle_code(0, b"", DEFAULT_HANDLERS)
    handle_code(0, handlers=DEFAULT_HANDLERS)
    handle_code(0, response=b"", handlers=DEFAULT_HANDLERS)

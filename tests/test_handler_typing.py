from collections import ChainMap
from typing import TYPE_CHECKING

from geturl import DEFAULT_HANDLERS, handle_code
from geturl.geturl import Handlers

if TYPE_CHECKING:
    from typing_extensions import assert_type

    # Make sure all the overloads are correct
    handle_code(0)
    handle_code(0, b"")
    handle_code(0, b"", DEFAULT_HANDLERS)
    handle_code(0, handlers=DEFAULT_HANDLERS)
    handle_code(0, response=b"", handlers=DEFAULT_HANDLERS)

    my_handlers: Handlers[None] = {200: lambda c, r: print(r)}
    handle_code(0, b"", handlers=my_handlers)
    handle_code(0, b"", handlers=DEFAULT_HANDLERS)

    # NOTE: we have to ignore the typing here (or cast) since chainmap takes a *Mutable*Mapping
    #       and handlers are Mappings. We could change handlers to be mutable, but nothing
    #       in the code actually mutates them so it would be less strict.
    handlers_chain: Handlers[None] = ChainMap(my_handlers, DEFAULT_HANDLERS)  # type: ignore[arg-type]
    assert_type(handlers_chain, Handlers[None])
    handle_code(0, handlers=handlers_chain)

    handle_code(0, b"", handlers=ChainMap(my_handlers, DEFAULT_HANDLERS))  # type: ignore[arg-type]

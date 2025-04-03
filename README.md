# geturl

[![Single file](https://img.shields.io/badge/single%20file%20-%20purple)](https://raw.githubusercontent.com/MarcinKonowalczyk/geturl/main/src/geturl/geturl.py)
[![test](https://github.com/MarcinKonowalczyk/geturl/actions/workflows/test.yml/badge.svg)](https://github.com/MarcinKonowalczyk/geturl/actions/workflows/test.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
![Python versions](https://img.shields.io/badge/python-3.9%20~%203.13-blue)

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

### Install

Just copy the single-module file to your project and import it.

```bash
cp ./src/geturl/geturl.py src/your_package/_geturl.py
```

Or even better, without checking out the repository:

```bash
curl https://raw.githubusercontent.com/MarcinKonowalczyk/geturl/main/src/geturl/geturl.py > src/your_package/_geturl.py
```

Note that like this *you take ownership of the code* and you are responsible for keeping it up-to-date. If you change it that's fine (keep the license pls). That's the point here. You can also copy the code to your project and modify it as you wish.

If you want you can also build and install it as a package, but then the source lives somewhere else. That might be what you want though. 🤷‍♀️

```bash
pip install flit
flit build
ls dist/*
pip install dist/*.whl
```

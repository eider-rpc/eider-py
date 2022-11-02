# eider-py

[![CircleCI](https://circleci.com/gh/eider-rpc/eider-py.svg?style=svg)](https://circleci.com/gh/eider-rpc/eider-py)

This is the reference Python implementation of the Eider RPC protocol.

Full documentation is on [Read the Docs](http://eider.readthedocs.io/).

## Quick Start

```sh
python3 -m venv venv
venv/bin/pip install -e .
venv/bin/pip install -r requirements-dev.txt
venv/bin/pytest
```

## Python version compatibility

Version 2.0 of this package dropped support for Python 3.4-3.6 in order to take
advantage of modern Python features (e.g. `async`/`await` syntax).  If you need
to support those older Python versions, use version 1.x of this package.  The
two implementations are entirely compatible with each other over the wire; no
changes were made to the Eider RPC protocol.

#!/usr/bin/env python3
import sys
try:
    import tomllib
except ImportError:
    import tomli as tomllib


proj = tomllib.load(open("pyproject.toml", "rb"))
print(
    " ".join(
        f"-e {tox_env}"
        for tox_env in proj["tool"]["tox"]["gh"]["python"][sys.argv[1]]
    )
)

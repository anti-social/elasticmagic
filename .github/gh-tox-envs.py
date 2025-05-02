#!/usr/bin/env python3
import sys
import tomllib


proj = tomllib.load(open("pyproject.toml", "rb"))
print(
    " ".join(
        f"-e {tox_env}"
        for tox_env in proj["tool"]["tox"]["gh"]["python"][sys.argv[1]]
    )
)

import sys
from contextlib import contextmanager
from pathlib import Path

import yaml

from ampel.cli.main import main


@contextmanager
def argv_context(args: list[str]):
    argv = sys.argv
    try:
        sys.argv = args
        yield
    finally:
        sys.argv = argv


def run(args: list[str]) -> None | int | str:
    try:
        with argv_context(list(args)):
            return main() or None
    except SystemExit as se:
        return se.code


def dump(payload, tmpdir, name: str) -> Path:
    f = Path(tmpdir) / name
    with f.open("w") as fd:
        yaml.dump(payload, fd)
    return f

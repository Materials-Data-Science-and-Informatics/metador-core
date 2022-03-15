"""Various helper functions that packers can use."""

import os
from pathlib import Path

# TODO: directory comparison helper function
# (to help creating updates from changes in a directory)
# should simplify implementation of shallow patches in the packers


def norm_symlink(base: Path, syml: Path) -> str:
    """
    From base path and directory symlink path, normalize it to be relative to base.

    Mainly used to eliminate .. in paths.
    """
    b = base.resolve()
    p = (syml.parent / os.readlink(str(syml))).resolve().relative_to(b)
    return "/" + str(p)

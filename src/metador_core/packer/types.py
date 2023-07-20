from __future__ import annotations

from typing import Any, Dict, List, Optional


class DirValidationErrors(ValueError):
    """Structure to collect record or directory validation errors.

    Returned from validation functions and thrown in other contexts.
    """

    errors: Dict[str, List[Any]]
    """Common type used to collect errors.

    Maps path in container or directory to list of errors with that path.

    The list should contain either strings or other such dicts,
    but Python type checkers are unable to understand recursive types.
    """

    def __init__(self, errs: Optional[Dict[str, Any]] = None):
        self.errors = errs or {}

    def __bool__(self):
        return bool(self.errors)

    def __repr__(self):  # pragma: no cover
        return repr(self.errors)

    def update(self, *err_objs: DirValidationErrors):
        """Add errors from other instances to this object."""
        errs = self.errors
        for more_errs in err_objs:
            for k, v in more_errs.errors.items():
                if k not in errs:
                    errs[k] = v
                else:
                    errs[k] += v

    def add(self, k, v):
        if k not in self.errors:
            self.errors[k] = []
        self.errors[k].append(v)

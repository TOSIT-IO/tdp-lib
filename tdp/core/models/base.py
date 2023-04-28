# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base


def keyvalgen(obj):
    # From: https://stackoverflow.com/a/54034230
    """Generate key-value pairs from an object.

    Exclude keys starting with an underscore and keys that are SQLAlchemy

    Args:
        obj (object): Object to generate key-value pairs from.

    Yields:
        tuple: Key-value pair.
    """
    excl = ("_sa_adapter", "_sa_instance_state")
    for k, v in vars(obj).items():
        if not k.startswith("_") and not any(hasattr(v, a) for a in excl):
            yield k, v


class CustomBase:
    """Custom base class for SQLAlchemy models.

    Methods:
        __repr__ (str): String representation of the object.
    """

    def __repr__(self):
        params = ", ".join(f"{k}={v}" for k, v in keyvalgen(self))
        return f"{self.__class__.__name__}({params})"


metadata = MetaData()
Base = declarative_base(cls=CustomBase, metadata=metadata)

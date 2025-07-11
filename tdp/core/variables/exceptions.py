# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass


@dataclass(frozen=True)
class UpdateError:
    """Represents an error that occurred during a service update."""

    service_name: str
    message: str


class ServicesUpdateError(Exception):
    """Raised when an error occurs during the update of services."""

    def __init__(self, errors: list[UpdateError]):
        super().__init__(
            "Errors occurred during service updates:\n"
            + "\n".join(f"{e.service_name}: {e.message}" for e in errors)
        )
        self.errors = errors

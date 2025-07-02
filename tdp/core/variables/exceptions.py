# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass


@dataclass(frozen=True)
class UnknownService:
    """Represents a service that is referenced but not initialized."""

    service_name: str
    source_definition: str


class ServicesNotInitializedError(Exception):
    """Raised when some expected services are missing from the initialized set."""

    def __init__(self, services: list[UnknownService]):
        super().__init__(
            "The following services are not initialized:\n"
            + "\n".join(
                f"{e.service_name} (from {e.source_definition})" for e in services
            )
        )
        self.services = services


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

# Copyright 2025 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from typing import Optional


class ServiceVariablesNotInitializedError(Exception):
    def __init__(self, service_name: str, source: Optional[str] = None):
        super().__init__(
            f"Variables for service '{service_name}' have not been initialized."
        )
        self.name = service_name
        self.source = source

    def as_list_item(self) -> str:
        """Return a string representation of the error for listing."""
        return f"{self.name}" + (f" (from {self.source})" if self.source else "")


class ServiceVariablesNotInitializedErrorList(Exception):
    base_msg = "The following services are not initialized:"

    def __init__(self, errors: list[ServiceVariablesNotInitializedError]):
        super().__init__(
            self.base_msg + " " + ", ".join(e.as_list_item() for e in errors)
        )
        self.errors = errors

    def __str__(self):
        return (
            self.base_msg
            + "\n"
            + "\n".join(f"- {e.as_list_item()}" for e in self.errors)
        )

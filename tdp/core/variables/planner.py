# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass, field
from pathlib import Path

from tdp.core.variables.messages import ValidationMessageBuilder
from tdp.core.variables.scanner import ServiceDirectoryScanner


@dataclass
class ServiceUpdatePlan:
    """Plan holding input paths and a validation message for a given service."""

    service_name: str
    input_paths: list[Path] = field(default_factory=list)
    validation_message: str = ""


class ServiceUpdatePlanner:
    """Prepares update plans for services based on input paths and validation logic."""

    def __init__(
        self,
        collections,
        validation_builder: ValidationMessageBuilder,
    ):
        self.collections = collections
        self.validation_builder = validation_builder

    def plan_updates(
        self,
        sources: list[tuple[str, Path]],
        merge_inputs: bool,
    ) -> list[ServiceUpdatePlan]:
        updates: dict[str, ServiceUpdatePlan] = {}

        for source_name, source_path in sources:
            is_collection = source_name in self.collections.default_vars_dirs
            base_msg = (
                self.validation_builder.for_collection(source_name)
                if is_collection
                else self.validation_builder.for_override(source_path)
            )

            for (
                service_name,
                service_path,
            ) in ServiceDirectoryScanner.scan(source_path).items():
                msg = base_msg
                if custom := self.validation_builder.for_service(service_path):
                    msg += f"\nUser message: {custom}"

                key = service_name if merge_inputs else f"{service_name}:{service_path}"
                plan = updates.setdefault(
                    key,
                    ServiceUpdatePlan(service_name),
                )
                plan.input_paths.append(service_path)
                if plan.validation_message:
                    plan.validation_message += "\n"
                plan.validation_message += msg

        return list(updates.values())

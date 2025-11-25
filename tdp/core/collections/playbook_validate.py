# Copyright 2025 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""
Extract and validate custom `tdp_lib` metadata from an Ansible playbook.
"""

from pathlib import Path
from typing import Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationError, conlist


class _PlaybookPlayVarsMetaIn(BaseModel):
    """Pydantic model describing the expected structure of a playbook's play[].vars.tdp_lib."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    can_limit: Optional[bool] = Field(
        None,
        description="Can the task be limited to specific host. false will be applied to the whole playbook.",
    )


class _PlaybookPlayVarsIn(BaseModel):
    """Pydantic model describing the expected structure of a playbook's play[].vars."""

    model_config = ConfigDict(frozen=True)

    tdp_lib: Optional[_PlaybookPlayVarsMetaIn] = Field(None)


class _PlaybookPlayIn(BaseModel):
    """Pydantic model describing the expected structure of a playbook's play."""

    model_config = ConfigDict(frozen=True)

    hosts: Union[str, list[str]]
    name: Optional[str] = Field(None)
    vars: Optional[_PlaybookPlayVarsIn] = Field(None)


class PlaybookIn(RootModel[conlist(_PlaybookPlayIn, min_length=1)]):
    """Pydantic model describing the expected structure of a playbook."""

    model_config = ConfigDict(frozen=True)

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]


def validate_playbook(playbook_path: Path) -> PlaybookIn:
    """Validate the content of a playbook file."""
    # Open playbook file and get content
    try:
        with playbook_path.open() as f:
            data = yaml.safe_load(f)
    except Exception as exc:
        raise ValueError(
            f"Parsing error for playbook file: '{playbook_path}':\n{exc}"
        ) from exc

    # Validate the file
    try:
        return PlaybookIn.model_validate(data)
    except ValidationError as exc:
        raise ValueError(
            f"Validation error for playbook file: '{playbook_path}':\n{exc}"
        ) from exc

# Copyright 2025 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest
from pydantic import ValidationError

from tdp.core.collections.playbook_validate import (
    PlaybookIn,
    _PlaybookPlayIn,
    _PlaybookPlayVarsIn,
    _PlaybookPlayVarsMetaIn,
    validate_playbook,
)


class TestPlaybookPlayVarsMetaIn:
    """Tests for _PlaybookPlayVarsMetaIn validation."""

    def test_valid_with_can_limit(self):
        """Valid when can_limit is provided."""
        data = {"can_limit": True}

        result = _PlaybookPlayVarsMetaIn.model_validate(data)

        assert result.can_limit is True

    def test_valid_when_empty(self):
        """Valid when no fields are provided."""
        data = {}

        result = _PlaybookPlayVarsMetaIn.model_validate(data)

        assert result.can_limit is None

    def test_forbids_extra_fields(self):
        """Extra fields are rejected."""
        data = {"extra_field": "not allowed"}

        with pytest.raises(ValidationError):
            _PlaybookPlayVarsMetaIn.model_validate(data)


class TestPlaybookPlayVarsIn:
    """Tests for _PlaybookPlayVarsIn validation."""

    def test_valid_with_tdp_lib_metadata(self):
        """Valid when tdp_lib contains valid metadata."""
        data = {"tdp_lib": {"can_limit": False}}

        result = _PlaybookPlayVarsIn.model_validate(data)

        assert isinstance(result.tdp_lib, _PlaybookPlayVarsMetaIn)
        assert result.tdp_lib.can_limit is False

    def test_valid_when_empty(self):
        """Valid when no fields are provided."""
        data = {}

        result = _PlaybookPlayVarsIn.model_validate(data)

        assert result.tdp_lib is None

    def test_allows_extra_fields_outside_tdp_lib(self):
        """Extra fields outside tdp_lib are allowed (default extra behaviour)."""
        data = {"extra_field": 123, "another_var": "value"}

        # Should not raise
        _PlaybookPlayVarsIn.model_validate(data)


class TestPlaybookPlayIn:
    """Tests for _PlaybookPlayIn validation."""

    def test_allows_missing_name_and_vars(self):
        """name and vars are optional."""
        data = {"hosts": "all"}

        result = _PlaybookPlayIn.model_validate(data)

        assert result.hosts == "all"
        assert result.name is None
        assert result.vars is None

    @pytest.mark.parametrize(
        "hosts_value, expected",
        [
            ("all", "all"),
            (["web", "db"], ["web", "db"]),
        ],
        ids=["string-hosts", "list-hosts"],
    )
    def test_valid_hosts_types(self, hosts_value, expected):
        """hosts can be a string or a list of strings."""
        data = {"hosts": hosts_value}

        result = _PlaybookPlayIn.model_validate(data)

        assert result.hosts == expected

    def test_invalid_hosts_type_raises(self):
        """hosts must not be of an unsupported type."""
        data = {"hosts": 123}

        with pytest.raises(ValidationError):
            _PlaybookPlayIn.model_validate(data)

    def test_missing_hosts_raises(self):
        """hosts is required."""
        data = {}

        with pytest.raises(ValidationError):
            _PlaybookPlayIn.model_validate(data)


class TestPlaybookIn:
    """Tests for PlaybookIn validation."""

    def test_valid_single_play(self):
        """Valid when there is a single play."""
        data = [
            {
                "hosts": "all",
            }
        ]

        result = PlaybookIn.model_validate(data)

        assert len(list(result)) == 1
        assert isinstance(result[0], _PlaybookPlayIn)
        assert result[0].hosts == "all"

    def test_valid_multiple_plays(self):
        """Valid when there are multiple plays."""
        data = [
            {
                "hosts": "all",
            },
            {
                "hosts": ["web", "db"],
            },
        ]

        result = PlaybookIn.model_validate(data)

        assert len(list(result)) == 2
        assert result[0].hosts == "all"
        assert result[1].hosts == ["web", "db"]

    def test_empty_playbook_raises(self):
        """Empty playbook (list) is rejected by min_length."""
        data = []

        with pytest.raises(ValidationError):
            PlaybookIn.model_validate(data)

    def test_invalid_top_level_structure_raises(self):
        """Non-list top-level structure is rejected."""
        data = {"invalid_field": "value"}

        with pytest.raises(ValidationError):
            PlaybookIn.model_validate(data)


class TestValidatePlaybook:
    """Tests for validate_playbook helper function."""

    def test_valid_playbook_file_returns_playbookin(self, tmp_path: Path):
        """A valid YAML playbook file is loaded and validated."""
        content = """
        - hosts: all
          name: my play
        """
        playbook_path = tmp_path / "playbook.yml"
        playbook_path.write_text(content)

        result = validate_playbook(playbook_path)

        assert isinstance(result, PlaybookIn)
        assert len(list(result)) == 1
        assert result[0].hosts == "all"

    def test_parsing_error_wraps_in_value_error(self, tmp_path: Path):
        """YAML parsing errors are wrapped in a ValueError with a helpful message."""
        # Intentionally invalid YAML
        content = ":\n  - bad: [\n"
        playbook_path = tmp_path / "invalid.yml"
        playbook_path.write_text(content)

        with pytest.raises(ValueError) as excinfo:
            validate_playbook(playbook_path)

        msg = str(excinfo.value)
        assert "Parsing error for playbook file" in msg
        assert str(playbook_path) in msg

    def test_validation_error_wraps_in_value_error(self, tmp_path: Path):
        """Validation errors from PlaybookIn are wrapped in a ValueError."""
        # Valid YAML, invalid playbook structure for PlaybookIn
        content = "invalid: structure\n"
        playbook_path = tmp_path / "invalid_structure.yml"
        playbook_path.write_text(content)

        with pytest.raises(ValueError) as excinfo:
            validate_playbook(playbook_path)

        msg = str(excinfo.value)
        assert "Validation error for playbook file" in msg
        assert str(playbook_path) in msg

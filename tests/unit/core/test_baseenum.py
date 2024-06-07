# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.core.utils import BaseEnum


class SampleEnum(BaseEnum):
    """Sample enum."""

    VALUE1 = "value1"
    VALUE2 = "value2"
    VALUE3 = "value3"


class TestBaseEnum:
    def test_base_enum_contains(self):
        """Test BaseEnum.__contains__."""
        assert "value1" in SampleEnum
        assert "value4" not in SampleEnum

    def test_base_enum_equals(self):
        """Test BaseEnum.__eq__."""
        assert SampleEnum.VALUE1 == SampleEnum("value1")
        assert SampleEnum.VALUE1 == "value1"

    def test_base_enum_wrong_value(self):
        """Test BaseEnum with wrong value."""
        with pytest.raises(ValueError):
            SampleEnum("value4")

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.cli.commands.plan.edit import _parse_line


def test_mandatory_operation_name():
    with pytest.raises(ValueError):
        _parse_line("")


def test_operation_name_only():
    result = _parse_line("operation_name")
    assert result == ("operation_name", None, None)


def test_operation_name_with_spaces():
    result = _parse_line("operation_name ")
    assert result == ("operation_name", None, None)


def test_operation_name_with_hostname():
    result = _parse_line("operation_name on myhost")
    assert result == ("operation_name", "myhost", None)


def test_operation_name_with_args():
    result = _parse_line("operation_name with arg1,arg2,arg3")
    assert result == ("operation_name", None, ["arg1", "arg2", "arg3"])


def test_operation_name_with_hostname_and_args():
    result = _parse_line("operation_name on myhost with arg1,arg2,arg3")
    assert result == ("operation_name", "myhost", ["arg1", "arg2", "arg3"])


def test_operation_name_with_args_and_hostname():
    result = _parse_line("operation_name with arg1,arg2,arg3 on myhost")
    assert result == ("operation_name", "myhost", ["arg1", "arg2", "arg3"])


def test_missing_value_after_on():
    with pytest.raises(ValueError):
        _parse_line("operation_name on")


def test_missing_value_after_with():
    with pytest.raises(ValueError):
        _parse_line("operation_name with")


def test_args_with_special_chars():
    result = _parse_line('operation_name with arg1,arg"2",arg,3')
    assert result == ("operation_name", None, ["arg1", 'arg"2"', "arg", "3"])


def test_args_with_quotes():
    result = _parse_line('operation_name with "arg1","arg2","arg3"')
    assert result == ("operation_name", None, ['"arg1"', '"arg2"', '"arg3"'])


def test_args_with_spaces():
    result = _parse_line("operation_name with arg 1, arg 2, arg 3")
    assert result == ("operation_name", None, ["arg 1", "arg 2", "arg 3"])


def test_missing_operation_name():
    with pytest.raises(ValueError):
        _parse_line("on myhost with arg1,arg2")

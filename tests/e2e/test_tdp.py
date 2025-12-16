# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the main CLI application.

This module tests CLI-level functionality including:
- Global options (--env, --log-level, --cwd)
- Command routing and parsing
- Main CLI help system
- Cross-command consistency
"""

import pytest

from tdp.cli.__main__ import cli


def test_main_cli_help(runner):
    """Test that the main CLI shows help with available commands."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Usage: tdp [OPTIONS] COMMAND [ARGS]..." in result.output
    # Check that main commands are listed
    assert "init" in result.output
    assert "deploy" in result.output
    assert "plan" in result.output
    assert "status" in result.output


def test_main_cli_short_help(runner):
    """Test that the main CLI supports -h shortcut."""
    result = runner.invoke(cli, ["-h"])
    assert result.exit_code == 0
    assert "Usage: tdp [OPTIONS] COMMAND [ARGS]..." in result.output


@pytest.mark.parametrize(
    "command",
    [
        "init",
        "deploy",
        "dag",
        "default-diff",
        "ops",
        "browse",
    ],
)
def test_command_help_via_cli(runner, command):
    """Test that all main commands show help when invoked via main CLI."""
    result = runner.invoke(cli, [command, "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output


@pytest.mark.parametrize(
    "subcommand_group,subcommand",
    [
        ("plan", "dag"),
        ("plan", "ops"),
        ("plan", "reconfigure"),
        ("status", "show"),
        ("status", "edit"),
        ("vars", "edit"),
        ("vars", "validate"),
    ],
)
def test_subcommand_help_via_cli(runner, subcommand_group, subcommand):
    """Test that all subcommands show help when invoked via main CLI."""
    result = runner.invoke(cli, [subcommand_group, subcommand, "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output


def test_global_log_level_option(runner):
    """Test that global --log-level option works."""
    result = runner.invoke(cli, ["--log-level", "DEBUG", "--help"])
    assert result.exit_code == 0


def test_invalid_command(runner):
    """Test that invalid commands show appropriate error."""
    result = runner.invoke(cli, ["invalid-command"])
    assert result.exit_code != 0
    assert "No such command" in result.output


def test_command_routing_consistency(runner):
    """Test that command routing works consistently across all commands."""
    # Test that we can reach nested commands
    result = runner.invoke(cli, ["plan", "--help"])
    assert result.exit_code == 0
    assert "dag" in result.output
    assert "ops" in result.output

    result = runner.invoke(cli, ["status", "--help"])
    assert result.exit_code == 0
    assert "show" in result.output
    assert "edit" in result.output

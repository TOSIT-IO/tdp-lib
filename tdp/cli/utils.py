# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click

from tdp.core.collection import Collection


def collection_paths(ctx, param, value):
    if not value:
        raise click.BadParameter("cannot be empty", ctx=ctx, param=param)

    collections = [Collection.from_path(split) for split in value.split(os.pathsep)]

    return collections

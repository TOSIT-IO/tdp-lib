# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.core.dag import Dag

pass_dag = click.make_pass_decorator(Dag)

# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from enum import Enum


class DeploymentTypeEnum(Enum):
    DAG = "Dag"
    OPERATIONS = "Operations"
    RESUME = "Resume"
    RECONFIGURE = "Reconfigure"

    @classmethod
    def has_value(cls, value):
        return isinstance(value, DeploymentTypeEnum) or value in (
            v.value for v in cls.__members__.values()
        )

    @classmethod
    def max_length(cls):
        return max(len(state.value) for state in list(DeploymentTypeEnum))

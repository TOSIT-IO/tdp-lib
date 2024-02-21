# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

from sqlalchemy.engine.row import Row

ServiceComponentHostStatus = tuple[
    str,  # service
    Optional[str],  # component
    Optional[str],  # host
    Optional[str],  # running_version
    Optional[str],  # configured_version
    Optional[int],  # to_config
    Optional[int],  # to_restart
]

SCHStatusRow = Row[ServiceComponentHostStatus]

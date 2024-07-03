# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

# Exception playbooks are playbooks which have to be deployed on all hosts and never on one specific one.
exception_playbooks = [
    "tdp/playbooks/hbase_kerberos_install.yml",
    "tdp/playbooks/hdfs_kerberos_install.yml",
    "tdp/playbooks/yarn_kerberos_install.yml",
    "tdp/playbooks/hadoop_client_config.yml",
]

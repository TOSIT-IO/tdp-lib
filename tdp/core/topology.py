import logging
import ansible.constants as C
from ansible.executor.playbook_executor import PlaybookExecutor
from ansible.parsing.dataloader import DataLoader
from ansible.inventory.manager import InventoryManager
from ansible.vars.manager import VariableManager

logger = logging.getLogger("tdp").getChild("topology")


class Topology:
    def __init__(self, hosts_files=None):
        self._hosts_files = hosts_files
        self._loader = DataLoader()

        if self._hosts_files:
            if not isinstance(self._hosts_files, list):
                raise TypeError(f"hosts_files should be a list")

            self._inventory = InventoryManager(
                loader=self._loader, sources=self._hosts_files
            )
        else:
            self._inventory = InventoryManager(
                loader=self._loader, sources=C.DEFAULT_HOST_LIST
            )

    def get_hosts(self):
        return self._inventory.get_hosts()

    def get_topology(self):
        return {
            group: self._inventory.get_groups_dict()[group]
            for group in DEFAULT_GROUPS_WHITELIST
        }


DEFAULT_GROUPS_WHITELIST = [
    "hdfs_nn",
    "hdfs_jn",
    "hdfs_dn",
    "zk",
    "yarn_rm",
    "yarn_nm",
    "hadoop_client",
    "ranger_admin",
    "ranger_usersync",
    "hive_s2",
    "hive_client",
    "yarn_ats",
    "spark_client",
    "spark_hs",
    "hbase_master",
    "hbase_rs",
    "hbase_rest",
    "hbase_client",
    "knox",
    "phoenix_queryserver_daemon",
    "phoenix_queryserver_client",
]

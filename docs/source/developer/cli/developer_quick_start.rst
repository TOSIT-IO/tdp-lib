Developer Quick Start (sqlite backend)
======================================

#. make sure to clone tdp-collection at first.

#. install sqlite and create db folder

   .. code-block:: shell

     user@yourmachine:sudo apt-get install sqlite
     user@yourmachine:mkdir sqlite-data

#. set environment variables (tdp cli will read it, and you don't need to mention it afterwards)

   .. code-block:: shell

     # this folder is local repo of tdp-collection
     user@yourmachine:export TDP_COLLECTIONS=~/tdp-collection
     # this folder is working directory of tdp deployment in which you put ansible.cfg, inventory.ini and topology.ini.
     user@yourmachine:export TDP_RUN_DIRECTORY=~/working-dir
     # this folder is to store sqlite db file
     user@yourmachine:export TDP_DATABASE_DSN=sqlite:////data/sqlite-data/tdp.db
     # this folder is to store tdp service configuration, please find default values at $TDP_COLLECTIONS/tdp_vars_defaults
     # you must NOT set TDP_VARS to $TDP_COLLECTIONS/tdp_vars_defaults
     # the path must contain the string `tdp_vars`
     user@yourmachine:export TDP_VARS=~/tdp_vars

#. you must configure ansible to point to your TDP_VARS

   .. code-block:: cfg

     [defaults]
     inventory=your_inventory,..,~/tdp_vars

     [inventory]
     enable_plugins = tosit.tdp.inventory,..,your_plugins

#. launch tdp init

   .. code-block:: shell

     user@yourmachine:cd tdp-lib
     user@yourmachine:poetry shell

     Spawning shell within /home/user/.cache/pypoetry/virtualenvs/tdp-lib-wP9YBfm3-py3.6
     user@yourmachine:~/tdp-lib$ . /home/user/.cache/pypoetry/virtualenvs/tdp-lib-wP9YBfm3-py3.6/bin/activate

     user@yourmachine:tdp init
     2022-10-04 12:38:24,823 - DEBUG - tdp.<module> - Logger initialized
     2022-10-04 12:38:25,275 INFO sqlalchemy.engine.Engine BEGIN (implicit)
     2022-10-04 12:38:25,275 INFO sqlalchemy.engine.Engine PRAGMA main.table_info("deployment_log")
     2022-10-04 12:38:25,275 INFO sqlalchemy.engine.Engine [raw sql] ()
     2022-10-04 12:38:25,275 INFO sqlalchemy.engine.Engine PRAGMA main.table_info("operation_log")
     2022-10-04 12:38:25,275 INFO sqlalchemy.engine.Engine [raw sql] ()
     2022-10-04 12:38:25,275 INFO sqlalchemy.engine.Engine PRAGMA main.table_info("service_log")
     2022-10-04 12:38:25,275 INFO sqlalchemy.engine.Engine [raw sql] ()
     2022-10-04 12:38:25,276 INFO sqlalchemy.engine.Engine COMMIT
     2022-10-04 12:38:25,280 - INFO - tdp.cluster_variables.initialize_cluster_variables - spark3 is already initialized at 23d6ecdbffdd72ab038fda80193cb9a0394cde16
     2022-10-04 12:38:25,283 - INFO - tdp.cluster_variables.initialize_cluster_variables - exporter is already initialized at a4e906cbdad42179543d7bc1d19b9f1b3205ac4c
     2022-10-04 12:38:25,287 - INFO - tdp.cluster_variables.initialize_cluster_variables - zookeeper is already initialized at d820a9e62da81a4b2a3e88ec5c0d26b430388238
     2022-10-04 12:38:25,292 - INFO - tdp.cluster_variables.initialize_cluster_variables - knox is already initialized at 6bfffa7074d7a8dd66c221501de15c99f2cb9dbd
     2022-10-04 12:38:25,297 - INFO - tdp.cluster_variables.initialize_cluster_variables - hdfs is already initialized at 3b9dc99b6c734e467e4da3870ee819234d7f9381
     2022-10-04 12:38:25,301 - INFO - tdp.cluster_variables.initialize_cluster_variables - hbase is already initialized at 8ab1262f8ca7a6fbcda6f5878391b676d48de2b0
     2022-10-04 12:38:25,305 - INFO - tdp.cluster_variables.initialize_cluster_variables - yarn is already initialized at dcfde77e17d94a860765f9a391d790b3c82a21c1
     2022-10-04 12:38:25,311 - INFO - tdp.cluster_variables.initialize_cluster_variables - hadoop is already initialized at 7e3bfefb598e82126ad59b967b70b777e177cca5
     2022-10-04 12:38:25,317 - INFO - tdp.cluster_variables.initialize_cluster_variables - tdp_cluster is already initialized at 279f89c2490e2715a19803ddf1c3c41634b6d017
     2022-10-04 12:38:25,320 - INFO - tdp.cluster_variables.initialize_cluster_variables - spark is already initialized at 30b46f768a2e9cf2eb2fececda9848d9069d08f1
     2022-10-04 12:38:25,323 - INFO - tdp.cluster_variables.initialize_cluster_variables - all is already initialized at 1eca4647419b1de0f9f756ec39a3929199c95e98
     2022-10-04 12:38:25,327 - INFO - tdp.cluster_variables.initialize_cluster_variables - ranger is already initialized at db8bdbabf5444fdbd886f34a8b5703f445500fa1
     2022-10-04 12:38:25,330 - INFO - tdp.cluster_variables.initialize_cluster_variables - hive is already initialized at 979c8be37130112dc257ef1036a49c8ee97971db
     2022-10-04 12:38:25,339 - INFO - tdp.cluster_variables.initialize_cluster_variables - hadoop is already initialized at 7e3bfefb598e82126ad59b967b70b777e177cca5
     spark3: 23d6ecdbffdd72ab038fda80193cb9a0394cde16
     exporter: a4e906cbdad42179543d7bc1d19b9f1b3205ac4c
     zookeeper: d820a9e62da81a4b2a3e88ec5c0d26b430388238
     knox: 6bfffa7074d7a8dd66c221501de15c99f2cb9dbd
     hdfs: 3b9dc99b6c734e467e4da3870ee819234d7f9381
     hbase: 8ab1262f8ca7a6fbcda6f5878391b676d48de2b0
     yarn: dcfde77e17d94a860765f9a391d790b3c82a21c1
     hadoop: 7e3bfefb598e82126ad59b967b70b777e177cca5
     tdp_cluster: 279f89c2490e2715a19803ddf1c3c41634b6d017
     spark: 30b46f768a2e9cf2eb2fececda9848d9069d08f1
     all: 1eca4647419b1de0f9f756ec39a3929199c95e98
     ranger: db8bdbabf5444fdbd886f34a8b5703f445500fa1
     hive: 979c8be37130112dc257ef1036a49c8ee97971db

#. list all nodes

   .. code-block:: shell

     user@yourmachine:tdp nodes
     2022-03-16 16:47:08,352 - DEBUG - tdp.<module> - Logger initialized
     2022-03-16 16:47:08,804 - WARNING - tdp.dag.validate - playbooks_dir is not defined, skip playbooks validations
     Component list:
     - hadoop_client_install
     - hadoop_install
     - hbase_client_install
     - hbase_master_install
     - hbase_phoenix_client_install
     - hbase_phoenix_queryserver_client_install
     - hbase_phoenix_queryserver_daemon_install
     - hbase_phoenix_kerberos_install
     - hbase_phoenix_ssl-tls_install
     - hbase_ranger_install
     ...
     ...

#. visualise a subgraph of the dag to a specific node (ex : zookeeper_install)

   .. code-block:: shell

     user@yourmachine:tdp dag zookeeper_install

#. choose a target node in the dag (ex : zookeeper_install) and launch tdp deploy (dry run)

   .. code-block:: shell

     user@yourmachine:tdp deploy --targets zookeeper_install --dry
     2022-03-16 16:50:21,379 - DEBUG - tdp.<module> - Logger initialized
     2022-03-16 16:50:21,838 - WARNING - tdp.dag.validate - playbooks_dir is not defined, skip playbooks validations
     Deploying zookeeper_install
     2022-03-16 16:50:21,918 - DEBUG - tdp.operation_runner.run - Running operation zookeeper_client_install
     2022-03-16 16:50:21,918 - INFO - tdp.ansible_executor.execute - [DRY MODE] Ansible command: ansible-playbook /home/diode-xue01/workspace/tdp-ops/tdp/ansible/collections/ansible_collections/tosit/tdp/playbooks/zookeeper_client_install.yml
     2022-03-16 16:50:21,924 - INFO - tdp.operation_runner._run_operations - Operation zookeeper_client_install success
     2022-03-16 16:50:21,924 - DEBUG - tdp.operation_runner.run - Running operation zookeeper_server_install
     2022-03-16 16:50:21,924 - INFO - tdp.ansible_executor.execute - [DRY MODE] Ansible command: ansible-playbook /home/diode-xue01/workspace/tdp-ops/tdp/ansible/collections/ansible_collections/tosit/tdp/playbooks/zookeeper_server_install.yml
     2022-03-16 16:50:21,924 - INFO - tdp.operation_runner._run_operations - Operation zookeeper_server_install success
     2022-03-16 16:50:21,924 - DEBUG - tdp.operation_runner.run - Running operation zookeeper_kerberos_install
     2022-03-16 16:50:21,924 - INFO - tdp.ansible_executor.execute - [DRY MODE] Ansible command: ansible-playbook /home/diode-xue01/workspace/tdp-ops/tdp/ansible/collections/ansible_collections/tosit/tdp/playbooks/zookeeper_kerberos_install.yml
     2022-03-16 16:50:21,924 - INFO - tdp.operation_runner._run_operations - Operation zookeeper_kerberos_install success

#. check deployment

   .. code-block:: shell

     user@yourmachine:tdp browse
     2022-03-16 17:22:43,295 - DEBUG - tdp.<module> - Logger initialized
     Deployments:
       id  target             filter    start                       end                         state    operations                                                  services
     ----  -----------------  --------  --------------------------  --------------------------  -------  -------------------------------------------------------  ----------
       1  zookeeper_install  None      2022-03-16 16:50:21.918121  2022-03-16 16:50:21.925006  Success  zookeeper_client_install,...,zookeeper_kerberos_install  zookeeper
       2  zookeeper_install  None      2022-03-16 16:54:42.950678  2022-03-16 16:54:42.957124  Success  zookeeper_client_install,...,zookeeper_kerberos_install  zookeeper

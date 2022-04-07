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
     user@yourmachine:export TDP_COLLECTION_PATH=~/tdp-collection
     # this folder is working directory of tdp deployment in which you put ansible.cfg, inventory.ini and topology.ini.
     user@yourmachine:export TDP_RUN_DIRECTORY=~/working-dir  
     # this folder is to store sqlite db file
     user@yourmachine:export TDP_SQLITE_PATH=~/sqlite-data/tdp.db
     # this folder is to store tdp service configuration, please found default value at ~/tdp-collection/tdp_vars_defaults
     user@yourmachine:export TDP_VARS=~/tdp-vars

#. launch tdp init  

   .. code-block:: shell

     user@yourmachine:cd tdp-lib
     user@yourmachine:poetry shell
 
     Spawning shell within /home/user/.cache/pypoetry/virtualenvs/tdp-lib-wP9YBfm3-py3.6
     user@yourmachine:~/tdp-lib$ . /home/user/.cache/pypoetry/virtualenvs/tdp-lib-wP9YBfm3-py3.6/bin/activate

     user@yourmachine:tdp init
     2022-03-16 16:01:10,610 - DEBUG - tdp.<module> - Logger initialized
     2022-03-16 16:01:11,070 INFO sqlalchemy.engine.Engine BEGIN (implicit)
     2022-03-16 16:01:11,070 INFO sqlalchemy.engine.Engine PRAGMA main.table_info("action_log")
     2022-03-16 16:01:11,070 INFO sqlalchemy.engine.Engine [raw sql] ()
     2022-03-16 16:01:11,070 INFO sqlalchemy.engine.Engine PRAGMA main.table_info("deployment_log")
     2022-03-16 16:01:11,071 INFO sqlalchemy.engine.Engine [raw sql] ()
     2022-03-16 16:01:11,071 INFO sqlalchemy.engine.Engine PRAGMA main.table_info("service_log")
     2022-03-16 16:01:11,071 INFO sqlalchemy.engine.Engine [raw sql] ()
     2022-03-16 16:01:11,071 INFO sqlalchemy.engine.Engine COMMIT
     2022-03-16 16:01:11,076 - WARNING - tdp.dag.validate - playbooks_dir is not defined, skip playbooks validations
     2022-03-16 16:01:11,080 - INFO - tdp.git_repository.initialize_service_managers - hbase is already initialized at 47db11a622bfa215cdb7c705692c13dafd2f8ac9
     2022-03-16 16:01:11,085 - INFO - tdp.git_repository.initialize_service_managers - hadoop is already initialized at 4e29c563d1f8bb1dec0698d9726708f85f365903
     2022-03-16 16:01:11,091 - INFO - tdp.git_repository.initialize_service_managers - zookeeper is already initialized at a4e3fe869572f017878d3a1de61955f67bde3d0c
     2022-03-16 16:01:11,099 - INFO - tdp.git_repository.initialize_service_managers - hive is already initialized at 1f565413e4682b76b92f24385a7ade68cccc4d09
     2022-03-16 16:01:11,106 - INFO - tdp.git_repository.initialize_service_managers - ranger is already initialized at 5bcf03f2c6acae7a2248e185ef9cf5634e9d799c
     2022-03-16 16:01:11,113 - INFO - tdp.git_repository.initialize_service_managers - hdfs is already initialized at 18c2097e6f13ea2162ef1217c690a6b62cb005e1
     2022-03-16 16:01:11,117 - INFO - tdp.git_repository.initialize_service_managers - yarn is already initialized at 6e5aaca19c4dafec26e383fc7630a599985c7140
     2022-03-16 16:01:11,120 - INFO - tdp.git_repository.initialize_service_managers - spark is already initialized at 95ff5d25fe7006320cc628aa5a63eefcc098391e
     hbase: 47db11a622bfa215cdb7c705692c13dafd2f8ac9
     hadoop: 4e29c563d1f8bb1dec0698d9726708f85f365903
     zookeeper: a4e3fe869572f017878d3a1de61955f67bde3d0c
     hive: 1f565413e4682b76b92f24385a7ade68cccc4d09
     ranger: 5bcf03f2c6acae7a2248e185ef9cf5634e9d799c
     hdfs: 18c2097e6f13ea2162ef1217c690a6b62cb005e1
     yarn: 6e5aaca19c4dafec26e383fc7630a599985c7140
     spark: 95ff5d25fe7006320cc628aa5a63eefcc098391e

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

     user@yourmachine:tdp deploy zookeeper_install --dry
     2022-03-16 16:50:21,379 - DEBUG - tdp.<module> - Logger initialized
     2022-03-16 16:50:21,838 - WARNING - tdp.dag.validate - playbooks_dir is not defined, skip playbooks validations
     Deploying zookeeper_install
     2022-03-16 16:50:21,918 - DEBUG - tdp.action_runner.run - Running action zookeeper_client_install
     2022-03-16 16:50:21,918 - INFO - tdp.ansible_executor.execute - [DRY MODE] Ansible command: ansible-playbook /home/diode-xue01/workspace/tdp-ops/tdp/ansible/collections/ansible_collections/tosit/tdp/playbooks/zookeeper_client_install.yml
     2022-03-16 16:50:21,924 - INFO - tdp.action_runner._run_actions - Action zookeeper_client_install success
     2022-03-16 16:50:21,924 - DEBUG - tdp.action_runner.run - Running action zookeeper_server_install
     2022-03-16 16:50:21,924 - INFO - tdp.ansible_executor.execute - [DRY MODE] Ansible command: ansible-playbook /home/diode-xue01/workspace/tdp-ops/tdp/ansible/collections/ansible_collections/tosit/tdp/playbooks/zookeeper_server_install.yml
     2022-03-16 16:50:21,924 - INFO - tdp.action_runner._run_actions - Action zookeeper_server_install success
     2022-03-16 16:50:21,924 - DEBUG - tdp.action_runner.run - Running action zookeeper_kerberos_install
     2022-03-16 16:50:21,924 - INFO - tdp.ansible_executor.execute - [DRY MODE] Ansible command: ansible-playbook /home/diode-xue01/workspace/tdp-ops/tdp/ansible/collections/ansible_collections/tosit/tdp/playbooks/zookeeper_kerberos_install.yml
     2022-03-16 16:50:21,924 - INFO - tdp.action_runner._run_actions - Action zookeeper_kerberos_install success

#. check deployment

   .. code-block:: shell

     user@yourmachine:tdp browse
     2022-03-16 17:22:43,295 - DEBUG - tdp.<module> - Logger initialized
     Deployments:
       id  target             filter    start                       end                         state    actions                                                  services
     ----  -----------------  --------  --------------------------  --------------------------  -------  -------------------------------------------------------  ----------
       1  zookeeper_install  None      2022-03-16 16:50:21.918121  2022-03-16 16:50:21.925006  Success  zookeeper_client_install,...,zookeeper_kerberos_install  zookeeper
       2  zookeeper_install  None      2022-03-16 16:54:42.950678  2022-03-16 16:54:42.957124  Success  zookeeper_client_install,...,zookeeper_kerberos_install  zookeeper

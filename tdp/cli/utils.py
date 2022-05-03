# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from tdp.core.dag import DAG_EXTENSION, DAG_FOLDER_NAME, Dag


def create_dag_from_collection_path(collection_path):
    return Dag((collection_path / DAG_FOLDER_NAME).glob("*" + DAG_EXTENSION))

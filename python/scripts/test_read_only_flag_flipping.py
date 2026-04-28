#!/usr/bin/env python3
from dotenv import load_dotenv
from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger

COLUMN_FAMILY = "cf"
logger = get_logger(__name__)


def create_table_on_active_cluster(active_cluster: HBaseDockerClient):
    """Create a new table on the active cluster and assert it exists"""
    tables = active_cluster.list_tables()
    new_table = f't{len(tables)+1}'
    active_cluster.create_table(new_table, COLUMN_FAMILY)
    active_cluster.assert_table_exists(new_table)
    return new_table


def add_data_to_each_table_on_active_cluster(active_cluster, tables):
    """Add data to each table in the active cluster"""
    for i, table in enumerate(tables[::-1], 1):
        active_cluster.put(table, f'r{i}', COLUMN_FAMILY, i)
        active_cluster.flush(table)


def refresh_replica_and_verify_tables(replica_cluster, new_table, tables):
    """
    Refresh meta and HFiles on the replica cluster, and verify the new table
    exists and each table has the correct number of rows
    """
    replica_cluster.refresh_meta()
    replica_cluster.refresh_hfiles()
    replica_cluster.assert_table_exists(new_table)
    for i, table in enumerate(tables[::-1], 1):
        cluster2.verify_table_row_count(table, i)


def test_active_and_replica_clusters(active_cluster: HBaseDockerClient, replica_cluster: HBaseDockerClient):
    new_table = create_table_on_active_cluster(active_cluster)

    # The new table should not exist on the replica cluster before refreshing meta
    replica_cluster.assert_table_does_not_exist(new_table)

    tables = active_cluster.list_tables()
    add_data_to_each_table_on_active_cluster(active_cluster, tables)
    refresh_replica_and_verify_tables(replica_cluster, new_table, tables)


def flip_read_only_flag(new_active_cluster, new_replica_cluster):
    # Make cluster read-only and verify it cannot create a table or put data
    new_replica_cluster.enable_read_only_mode()
    new_replica_cluster.verify_read_only_error_occurs('create', 't1', COLUMN_FAMILY)
    new_replica_cluster.verify_read_only_error_occurs(
        'put', 't1', COLUMN_FAMILY, row='r2', data='2')

    # Make cluster active
    new_active_cluster.disable_read_only_mode()


if __name__ == '__main__':
    # Load settings from .env file
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")

    cluster1 = HBaseDockerClient(container_name=container_name,
                                 local_conf=get_env('ACTIVE_CLUSTER_CONF'),
                                 hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                 cluster_name="Cluster 1")
    cluster2 = HBaseDockerClient(container_name=f'{container_name}-2',
                                 local_conf=get_env('REPLICA_CLUSTER_CONF'),
                                 hbase_ui_port=get_env('REPLICA_CLUSTER_PORT'),
                                 cluster_name="Cluster 2")

    iterations = 1
    for i in range(1, iterations+1):
        logger.info(f"----- Iteration {i} -----")
        # Create table on active cluster
        cluster1.disable_read_only_mode()
        cluster2.enable_read_only_mode()
        HBaseDockerClient.clean_up_tables(cluster1, cluster2)

        test_active_and_replica_clusters(active_cluster=cluster1, replica_cluster=cluster2)
        flip_read_only_flag(new_active_cluster=cluster2, new_replica_cluster=cluster1)
        test_active_and_replica_clusters(active_cluster=cluster2, replica_cluster=cluster1)
        flip_read_only_flag(new_active_cluster=cluster1, new_replica_cluster=cluster2)
        # This next line is commented out to prevent HBASE-30090
        # test_active_and_replica_clusters(active_cluster=cluster1, replica_cluster=cluster2)
        logger.info(f"Finished iteration {i} of {iterations}")

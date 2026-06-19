#!/usr/bin/env python3
import os
import time

import python.proto.generated.ActiveClusterSuffix_pb2 as acs

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


def add_data_to_each_table_on_active_cluster(active_cluster: HBaseDockerClient, tables: list):
    """Add data to each table in the active cluster"""
    for i, table in enumerate(tables[::-1], 1):
        active_cluster.put(table, f'r{i}', COLUMN_FAMILY, i)
        active_cluster.flush(table)


def refresh_replica_and_verify_tables(replica_cluster: HBaseDockerClient, new_table: str, tables: list):
    """
    Refresh meta and HFiles on the replica cluster, and verify the new table
    exists and each table has the correct number of rows
    """
    replica_cluster.refresh_meta()
    replica_cluster.refresh_hfiles()
    replica_cluster.assert_table_exists(new_table)
    for i, table in enumerate(tables[::-1], 1):
        replica_cluster.assert_table_row_count(table, i)


def create_table_and_test_active_and_replica_clusters(active_cluster: HBaseDockerClient,
                                                      replica_cluster: HBaseDockerClient):
    """
    Creates a new table and iteratively adds data to each existing table, including the new one.
    Also verifies expected behavior for the replica cluster, such as verifying the new table is not
    on the replica before refreshing meta, and then verify new table and data existence after
    refreshing meta and HFiles.
    """
    new_table = create_table_on_active_cluster(active_cluster)

    # The new table should not exist on the replica cluster before refreshing meta
    replica_cluster.assert_table_does_not_exist(new_table)

    tables = active_cluster.list_tables()
    # HBase sorts table list by string: ['t1', 't10', 't2, ..., 't9']
    # We want the list sorted by creation time, so we're sorting on the integer: ['t1', 't2, ..., 't9', 't10']
    tables.sort(key=lambda x: int(x[1:]))
    add_data_to_each_table_on_active_cluster(active_cluster, tables)
    refresh_replica_and_verify_tables(replica_cluster, new_table, tables)


def flip_read_only_flag(new_active_cluster: HBaseDockerClient,
                        new_replica_cluster: HBaseDockerClient):
    # Make cluster read-only and verify it cannot create a table or put data
    new_replica_cluster.enable_read_only_mode()
    new_replica_cluster.assert_read_only_error_occurs('create', 'testTable', COLUMN_FAMILY)
    new_replica_cluster.assert_read_only_error_occurs(
        'put', 't1', COLUMN_FAMILY, row='r2', data='2')

    # Make cluster active
    new_active_cluster.disable_read_only_mode()


def assert_correct_active_cluster_suffix(cluster: HBaseDockerClient, data_store_root: str):
    logger.info(f"Verifying active cluster suffix file matches 'hbase.meta.table.suffix' "
                f"in conf file for {cluster.name}")
    active_cluster_file = f'{data_store_root}/data-store/hbase/active.cluster.suffix.id'
    active_cluster_suffix = acs.ActiveClusterSuffix()

    # The active cluster suffix file may not get created right away
    retries = 0
    while not os.path.exists(active_cluster_file):
        if retries >= 5:
            raise RuntimeError(f"Timed out waiting for active cluster file to exist: {active_cluster_file}")
        logger.info(f"Waiting for active cluster file to exist: {active_cluster_file}")
        time.sleep(1)
        retries += 1

    # Parse the active cluster suffix protobuf message file
    with open(active_cluster_file, 'rb') as f:
        data = f.read()
        header = b'PBUF'
        if data.startswith(header):
            active_cluster_suffix.ParseFromString(data[len(header):])
        else:
            active_cluster_suffix.ParseFromString(data)
        actual_suffix = active_cluster_suffix.suffix

    # Assume the meta table suffix is blank if hbase.meta.table.suffix does not exist in HBase conf
    expected_suffix = cluster.get_hbase_conf_property_value('hbase.meta.table.suffix')
    if expected_suffix is None:
        expected_suffix = ''

    # Verify the active cluster suffix file has the expected meta table suffix
    assert actual_suffix == expected_suffix, (f"Expected {cluster.name} to have meta table suffix '{expected_suffix}', "
                                              f"but got '{actual_suffix}' instead")


def create_table_and_test_clusters_then_flip_read_only_flag(cluster1, cluster2, data_store_root):
    create_table_and_test_active_and_replica_clusters(active_cluster=cluster1, replica_cluster=cluster2)
    flip_read_only_flag(new_active_cluster=cluster2, new_replica_cluster=cluster1)
    assert_correct_active_cluster_suffix(cluster2, data_store_root)


if __name__ == '__main__':
    # Load settings from .env file
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")
    data_store_root = get_env("HBASE_DATA_STORE_ROOT")

    cluster1 = HBaseDockerClient(container_name=container_name,
                                 local_conf=f"{get_env('ACTIVE_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                 hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                 cluster_name="Cluster 1")
    cluster2 = HBaseDockerClient(container_name=f'{container_name}-2',
                                 local_conf=f"{get_env('REPLICA_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                 hbase_ui_port=get_env('REPLICA_CLUSTER_PORT'),
                                 cluster_name="Cluster 2")

    HBaseDockerClient.stop_containers(data_store_root)
    cluster1.disable_read_only_mode(run_update_all_config=False)
    cluster2.enable_read_only_mode(run_update_all_config=False)
    HBaseDockerClient.start_or_restart_containers()
    HBaseDockerClient.wait_for_clusters_to_start([cluster1, cluster2])

    test_iterations = 3
    read_only_flag_flips_per_iteration = 6
    for i in range(1, test_iterations + 1):
        logger.info(f"---------- Iteration {i} ----------")
        if i > 1:
            logger.info(f"Ensuring clusters are in proper modes. "
                        f"Making both clusters a replica, and then making {cluster1.name} the active cluster")
            cluster1.enable_read_only_mode()
            cluster2.enable_read_only_mode()
            cluster1.disable_read_only_mode()

        # Create table on active cluster
        HBaseDockerClient.clean_up_tables(cluster1, cluster2)

        # One iteration flips the read-only flag on each cluster and then flips it back.
        flip_num = 1
        while flip_num <= read_only_flag_flips_per_iteration:
            logger.info(f"*** Testing read-only flag flip number {flip_num} ***")
            if flip_num % 2 == 1:
                # Cluster 1 is active and Cluster 2 is replica
                create_table_and_test_clusters_then_flip_read_only_flag(cluster1, cluster2, data_store_root)
            else:
                # Cluster 2 is active and Cluster 1 is replica
                create_table_and_test_clusters_then_flip_read_only_flag(cluster2, cluster1, data_store_root)
            logger.info(f"Finished read-only flag flip {flip_num} of {read_only_flag_flips_per_iteration}")
            flip_num += 1
        logger.info(f"Finished iteration {i} of {test_iterations}")

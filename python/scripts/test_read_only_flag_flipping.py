#!/usr/bin/env python3
"""
This test starts with two Read-Replica HBase clusters, where one cluster is the active cluster and the other cluster is
the replica cluster. The test creates a table on the active cluster, adds data to the cluster, and verifies this data
is consistent on the replica cluster after refreshing the meta HFiles. It also verifies write operations cannot be
performed on the replica cluster. Then, the two clusters swap roles, where the active cluster becomes a replica and the
former replica becomes the new active cluster. The previous steps then repeat in an iterative fashion.

This test script verifies behavior for multiple bug fixes:

1. HBASE-30090: Table on replica cluster not refreshing after flipping read-only flag twice
   https://issues.apache.org/jira/browse/HBASE-30090

   Before implementing this fix, an existing table on a read-replica cluster was not getting updated after making that
   cluster the active cluster and then making it read-only again.

2. HBASE-30180: Can still add data to read-only region after flipping read-only flag multiple times
   https://issues.apache.org/jira/browse/HBASE-30180

   Before implementing this fix, this cluster setup and series of steps would eventually get to a scenario where data
   could be added to a table on cluster with read-only mode disabled.
"""
import argparse

from python.src.utils import (assert_correct_active_cluster_suffix, add_common_skip_container_stop_or_restart_arg,
                              reset_cluster_setup, load_env_and_set_up_clients)

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
    replica_cluster.refresh_meta_and_hfiles()
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


def create_table_and_test_clusters_then_flip_read_only_flag(cluster1, cluster2, data_store_root):
    create_table_and_test_active_and_replica_clusters(active_cluster=cluster1, replica_cluster=cluster2)
    flip_read_only_flag(new_active_cluster=cluster2, new_replica_cluster=cluster1)
    assert_correct_active_cluster_suffix(cluster2, data_store_root)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser = add_common_skip_container_stop_or_restart_arg(parser)
    args = parser.parse_args()

    skip_container_restart = args.skip_container_start_or_restart

    if skip_container_restart:
        logger.info("Docker containers will NOT be started/restarted at the beginning of this test run")
    else:
        logger.info("Docker containers will be started/restarted at the beginning of this test run")

    cluster1, cluster2 = load_env_and_set_up_clients()
    data_store_root = get_env("HBASE_DATA_STORE_ROOT")
    docker_compose_file = get_env("DOCKER_COMPOSE_FILE")

    reset_cluster_setup(active_cluster=cluster1, replica_cluster=cluster2,
                        skip_container_restart=skip_container_restart, docker_compose_file=docker_compose_file,
                        data_store_root=data_store_root)

    if not args.skip_container_start_or_restart:
        HBaseDockerClient.start_or_restart_containers(docker_compose_file=docker_compose_file,
                                                      data_store_root=f'{data_store_root}')
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

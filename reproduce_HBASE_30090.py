#!/usr/bin/env python3
"""
Reproduces HBASE-30090: Table on replica cluster not refreshing after flipping read-only flag twice
https://issues.apache.org/jira/browse/HBASE-30090

Tables on a replica cluster will have an incorrect number of rows after the read-only flag has been changed twice.
This script creates a table 't1', and adds one row of data before changing the read-only flag. After two changes to
the flag on each cluster, added data to the active cluster will not show up on the replica cluster, even after flushing
the table on the active cluster and refreshing meta and HFiles on the replica.

This script expects an invalid row count with table 't1' on the replica cluster after two read-only flag changes.
An exception is thrown otherwise. The 'iterations' variable can be changed to reproduce this issue multiple times
in one run.

Usage: python3 reproduce_HBASE_30090.py
"""
from dotenv import load_dotenv
from environment_loader import get_env
from hbase_docker_client import HBaseDockerClient, HBaseShellCommandError
from logger_config import get_logger

logger = get_logger(__name__)


if __name__ == '__main__':
    # Load settings from .env file
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")
    table_name = "t1"
    column_family = "cf"

    cluster1 = HBaseDockerClient(container_name=container_name,
                                 local_conf=get_env('ACTIVE_CLUSTER_CONF'),
                                 hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                 cluster_name="Cluster 1")
    cluster2 = HBaseDockerClient(container_name=f'{container_name}-2',
                                 local_conf=get_env('REPLICA_CLUSTER_CONF'),
                                 hbase_ui_port=get_env('REPLICA_CLUSTER_PORT'),
                                 cluster_name="Cluster 2")

    iterations = 10
    for i in range(1, iterations+1):
        logger.info(f"----- Iteration {i} -----")
        try:
            # Create table on active cluster
            cluster1.disable_read_only_mode()
            cluster2.enable_read_only_mode()
            HBaseDockerClient.clean_up_tables(cluster1, cluster2)
            cluster1.create_table('t1', column_family)
            cluster1.put('t1', 'r1', column_family, '1')
            cluster1.flush('t1')

            # Refresh replica cluster to get this table
            assert not cluster2.verify_table_exists('t1'), \
                f"Expected 't1' to not exist on {cluster2.name}"
            cluster2.refresh_meta()
            cluster2.refresh_hfiles()
            assert cluster2.verify_table_exists('t1'), \
                f"Expected table 't1' to exist on {cluster2.name}"
            cluster2.verify_table_row_count('t1', 1)

            # Make active cluster read-only adn verify it cannot create a table or put data
            cluster1.enable_read_only_mode()
            cluster1.verify_read_only_error_occurs('create', 't1', column_family)
            cluster1.verify_read_only_error_occurs(
                'put', 't1', column_family, row='r2', data='2')

            # Make replica cluster active and add a new table and new data
            cluster2.disable_read_only_mode()
            cluster2.put('t1', 'r2', column_family, '2')
            cluster2.create_table('t2', column_family)
            cluster2.put('t2', 'r1', column_family, '1')
            cluster2.flush('t1')
            cluster2.flush('t2')

            # Refresh the new replica cluster and verify it has this data
            cluster1.refresh_meta()
            cluster1.refresh_hfiles()
            cluster1.verify_table_row_count('t1', 2)
            cluster1.verify_table_row_count('t2', 1)

            # Make the original replica cluster read-only again
            cluster2.enable_read_only_mode()
            cluster2.verify_read_only_error_occurs('create', 't3', column_family)
            cluster2.verify_read_only_error_occurs(
                'put', 't1', column_family, row='r3', data='3')

            # Make the original active cluster able to perform writes again
            cluster1.disable_read_only_mode()
            cluster1.create_table('t3', column_family)
            cluster1.put('t3', 'r1', column_family, '1')
            cluster1.put('t2', 'r2', column_family, '2')
            cluster1.put('t1', 'r3', column_family, '3')
            cluster1.flush('t1')
            cluster1.flush('t2')
            cluster1.flush('t3')

            # Refresh original replica cluster that is now back into read-only mode
            cluster2.refresh_meta()
            cluster2.refresh_hfiles()
            cluster2.verify_read_only_error_occurs('create', 't4', column_family)
            cluster2.verify_read_only_error_occurs(
                'put', 't1', column_family, 'r4', '4')
            cluster2.verify_read_only_error_occurs(
                'put', 't2', column_family, 'r3', '3')
            cluster2.verify_read_only_error_occurs(
                'put', 't3', column_family, 'r2', '2')
            cluster2.verify_table_row_count('t1', 3)
            cluster2.verify_table_row_count('t2', 2)
            cluster2.verify_table_row_count('t3', 1)
            raise RuntimeError(f"Expected an AssertionError due to bad row count on "
                               f"{cluster2.name} to have occurred by now")
        except AssertionError as e:
            expected_msg = ("Expected table 't1' on Cluster 2 to have 3 row(s). "
                            "Instead got 2 row(s)")
            if expected_msg in str(e):
                logger.info(f"*** Got invalid row count for table 't1' on "
                            f"{cluster2.name} as expected ***")
            else:
                raise RuntimeError(f"Expected an AssertionError to occur with "
                                   f"the following message:\n{expected_msg}\n"
                                   f"Got the following instead:\n{str(e)}")
        logger.info(f"Finished iteration {i} of {iterations}")

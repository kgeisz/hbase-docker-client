#!/usr/bin/env python3
"""
This script tests bulk-loading data with Read-Replica HBase clusters.
"""
import argparse

from python.src import get_logger
from python.src.environment_loader import get_env
from python.src.utils import (add_common_skip_container_stop_or_restart_arg, reset_cluster_setup,
                              load_env_and_set_up_clients)

logger = get_logger(__name__)

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
    container_utils_dir = get_env("CONTAINER_UTILS_DIR")

    table1 = 'blt1'
    table2 = 'blt2'
    table3 = 'blt3'
    column_family = 'cf'
    bulkload_script = f"{container_utils_dir}/bulkload.sh"

    reset_cluster_setup(active_cluster=cluster1, replica_cluster=cluster2,
                        skip_container_restart=skip_container_restart, docker_compose_file=docker_compose_file,
                        data_store_root=data_store_root)

    # Bulkload data to active cluster and verify the data is there
    cluster1.run_docker_exec_command(f"{bulkload_script} {table1} {column_family}")
    cluster1.assert_table_exists(table1)
    cluster1.assert_table_row_count(table1, expected_row_count=500)

    # Replica cluster should not see bulkloaded data until meta and HFiles have been refreshed
    cluster2.assert_table_does_not_exist(table1)
    cluster2.refresh_meta()
    cluster2.refresh_hfiles()
    cluster2.assert_table_exists(table1)
    cluster2.assert_table_row_count(table1, expected_row_count=500)

    # Cluster 1 is now a replica and Cluster 2 is not the active cluster
    cluster1.enable_read_only_mode()
    cluster2.disable_read_only_mode()

    # Bulkload more data into the existing table on Cluster 2
    cluster2.run_docker_exec_command(f"{bulkload_script} {table1} {column_family} -n 300 -i 500")
    cluster2.assert_table_row_count(table1, expected_row_count=800)

    # Cluster 1 should not see the newly bulkloaded data until its meta and HFiles have been refreshed
    cluster1.assert_table_row_count(table1, expected_row_count=500)
    cluster1.refresh_meta()
    cluster1.refresh_hfiles()
    cluster1.assert_table_row_count(table1, expected_row_count=800)

    # Bulkload data into a new table on Cluster 2
    cluster2.run_docker_exec_command(f"{bulkload_script} {table2} {column_family} -n 600")
    cluster2.assert_table_exists(table2)
    cluster2.assert_table_row_count(table2, expected_row_count=600)

    # Cluster 1 should not see this new table until after refreshing meta and HFiles
    cluster1.assert_table_does_not_exist(table2)
    cluster1.refresh_meta()
    cluster1.refresh_hfiles()
    cluster1.assert_table_exists(table2)
    cluster1.assert_table_row_count(table2, expected_row_count=600)
    cluster1.assert_table_row_count(table1, expected_row_count=800)

    # Cluster 1 is back to being the active cluster and Cluster 2 is once again the replica cluster
    cluster2.enable_read_only_mode()
    cluster1.disable_read_only_mode()

    # Bulkload data onto both existing tables, and a new third table
    cluster1.run_docker_exec_command(f"{bulkload_script} {table1} {column_family} -n 400 -i 800")
    cluster1.run_docker_exec_command(f"{bulkload_script} {table2} {column_family} -n 600 -i 600")
    cluster1.run_docker_exec_command(f"{bulkload_script} {table3} {column_family} -n 1200")
    cluster1.assert_table_row_count(table1, expected_row_count=1200)
    cluster1.assert_table_row_count(table2, expected_row_count=1200)
    cluster1.assert_table_row_count(table3, expected_row_count=1200)

    # Cluster 2 should see the old row counts for the existing tables. It won't see the new table
    # or the updated row counts until after its meta and HFiles have been refreshed.
    cluster2.assert_table_row_count(table1, expected_row_count=800)
    cluster2.assert_table_row_count(table2, expected_row_count=600)
    cluster2.assert_table_does_not_exist(table3)
    cluster2.refresh_meta()
    cluster2.refresh_hfiles()
    cluster2.assert_table_row_count(table1, expected_row_count=1200)
    cluster2.assert_table_row_count(table2, expected_row_count=1200)
    cluster2.assert_table_row_count(table3, expected_row_count=1200)

#!/usr/bin/env python3

import argparse
import os
import time

from dotenv import load_dotenv

import python.proto.generated.ActiveClusterSuffix_pb2 as acs

from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger

logger = get_logger(__name__)


def add_common_skip_table_cleanup_arg(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument('-t', '--skip-table-cleanup-on-start', action='store_true',
                        help='Skip cleaning up tables at the start of the test')
    return parser


def add_common_skip_container_stop_or_restart_arg(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument('-s', '--skip-container-start-or-restart', action='store_true',
                        help='Skip stopping, starting, and waiting for the Docker containers to be ready')
    return parser


def load_env_and_set_up_clients(cluster1_name: str = "Cluster 1",
                                cluster2_name: str = "Cluster 2") -> tuple[HBaseDockerClient, HBaseDockerClient]:
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")

    active_cluster = HBaseDockerClient(container_name=container_name,
                                       local_conf=f"{get_env('ACTIVE_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                       hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                       cluster_name=cluster1_name)
    replica_cluster = HBaseDockerClient(container_name=f'{container_name}-2',
                                        local_conf=f"{get_env('REPLICA_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                        hbase_ui_port=get_env('REPLICA_CLUSTER_PORT'),
                                        cluster_name=cluster2_name)
    return active_cluster, replica_cluster


def run_put_and_get(cluster: HBaseDockerClient, table: str, row: str, cf: str, data: str):
    cluster.put(table, row, cf, data)
    cluster.assert_table_row_count(table, expected_row_count=1)
    return assert_get_output(cluster, table, row, cf, expected_data=data)


def assert_get_output(cluster: HBaseDockerClient, table: str, row: str, cf: str, expected_data: str):
    output = cluster.get(table, row, cf)
    assert f"value={expected_data}" in output, \
        f"Expected get command to retrieve a row with value={expected_data}. Output instead was:\n{output}"
    return output


def assert_crud_operations_work_on_active_cluster(cluster: HBaseDockerClient):
    table = 'crud-test-table1'
    cf = 'cf'
    row = 'r1'
    data = '1'

    # Create
    cluster.create_table(table, cf)
    cluster.assert_table_exists(table)

    # Retrieve
    run_put_and_get(cluster, table, row, cf, data)

    # "Update" (there are no true updates in HBase)
    data = '2'
    run_put_and_get(cluster, table, row, cf, data)

    # Delete
    # This row has two versions. This only deletes the first version
    cluster.delete(table, row, column=f"{cf}:")
    cluster.assert_table_row_count(table, expected_row_count=1)
    assert_get_output(cluster, table, row, cf, expected_data='1')

    # Delete the final version
    cluster.delete(table, row, column=f"{cf}:")
    cluster.assert_table_row_count(table, expected_row_count=0)

    # Drop table
    cluster.disable_table(table)
    cluster.drop_table(table)
    cluster.assert_table_does_not_exist(table)


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


def reset_cluster_setup(active_cluster: HBaseDockerClient, replica_cluster: HBaseDockerClient,
                        skip_container_restart: bool, docker_compose_file: str, data_store_root: str):
    """
    Resets the Read-Replica cluster setup where one cluster is the active cluster (read-write mode) and the other
    cluster is the replica cluster (read-only mode).
    """
    if not skip_container_restart:
        HBaseDockerClient.stop_containers(docker_compose_file=docker_compose_file, data_dir=data_store_root)

    active_cluster.disable_read_only_mode(run_update_all_config=False)
    replica_cluster.enable_read_only_mode(run_update_all_config=False)

    if not skip_container_restart:
        HBaseDockerClient.start_or_restart_containers(docker_compose_file=docker_compose_file,
                                                      data_store_root=f'{data_store_root}')
        HBaseDockerClient.wait_for_clusters_to_start([active_cluster, replica_cluster])

#!/usr/bin/env python3

import argparse
import os
import time

import python.proto.generated.ActiveClusterSuffix_pb2 as acs

from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger

logger = get_logger(__name__)


def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument('-t', '--skip-table-cleanup-on-start', action='store_true',
                        help='Skip cleaning up tables at the start of the test')
    return parser


def run_put_and_get(cluster: HBaseDockerClient, table: str, row: str, cf: str, data: str):
    cluster.put(table, row, cf, data)
    cluster.assert_table_row_count(table, expected_row_count=1)
    return assert_get_output(cluster, table, row, cf, data)


def assert_get_output(cluster: HBaseDockerClient, table: str, row: str, cf: str, expected_data: str):
    output = cluster.get(table, row, cf)
    assert f"value={expected_data}" in output, \
        f"Expected get command to retrieve a row with value={expected_data}. Output instead was:\n{output}"
    return output


def assert_crud_operations_work_on_active_cluster(cluster: HBaseDockerClient):
    table = 'test1'
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

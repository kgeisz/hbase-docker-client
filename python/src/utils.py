#!/usr/bin/env python3

import argparse

from python.src.hbase_docker_client import HBaseDockerClient


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
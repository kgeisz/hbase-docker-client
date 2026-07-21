#!/usr/bin/env python3
import argparse

from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger
from python.src.utils import add_common_skip_table_cleanup_arg, load_env_and_set_up_clients

logger = get_logger(__name__)


def test_put_delete_behavior(active_cluster, replica_cluster, table_name, column):
    """
    Verifies data can be added to/deleted from the active cluster, and that the read-replica cluster
    does not see this data until refresh_hfiles has been run. It also verifies put and delete
    operations on the read-replica result in an error.
    """
    # Add data to the table on the active cluster
    logger.info(f"Adding data to '{table_name}' on {active_cluster.name} and verifying it exists")
    active_cluster.put(table_name, "row1", column, "value1")
    active_cluster.assert_table_row_count(table_name, 1)

    # Verify the read-replica cluster does not see this new data
    logger.info(f"Verifying '{table_name}' on {replica_cluster.name} still has 0 rows")
    replica_cluster.assert_table_row_count(table_name, 0)

    # Flush the table's data on the active cluster
    logger.info(f"Flushing '{table_name}' on {active_cluster.name} and refreshing meta and "
                f"HFiles on {replica_cluster.name}")
    active_cluster.flush(table_name)

    # Refresh meta and HFiles, and verify the read-replica cluster now sees the data
    logger.info(f"Refreshing meta and HFiles on {replica_cluster.name}")
    replica_cluster.refresh_meta()
    replica_cluster.refresh_hfiles()
    logger.info(f"Verifying '{table_name}' on {replica_cluster.name} has data after refreshing HFiles")
    replica_cluster.assert_table_row_count(table_name, 1)

    # Verify data cannot be added to the table on the read-replica cluster
    logger.info(f"Verifying data cannot be added to '{table_name}' on {replica_cluster.name}")
    replica_cluster.assert_read_only_error_occurs('put', table_name, column, 'row2', 'value2')

    # Verify data cannot be deleted from the table on the read-replica cluster
    logger.info(f"Verifying data cannot be deleted from '{table_name}' on {replica_cluster.name}")
    replica_cluster.assert_read_only_error_occurs('delete', table_name, column, 'row2')

    # Delete data from the active cluster
    logger.info(f"Deleting row from '{table_name}' on {active_cluster.name} "
                f"and verifying it is gone")
    active_cluster.delete(table_name, "row1", column)
    active_cluster.flush(table_name)
    active_cluster.assert_table_row_count(table_name, 0)

    # Verify deleted data still exists on the read-replica cluster
    logger.info(f"Verifying deleted row still exists on {replica_cluster.name}")
    replica_cluster.assert_table_row_count(table_name, 1)

    # Verify the read-replica cluster no longer has the data after refreshing HFiles
    replica_cluster.refresh_hfiles()
    replica_cluster.assert_table_row_count(table_name, 0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser = add_common_skip_table_cleanup_arg(parser)
    args = parser.parse_args()

    active_cluster, replica_cluster = load_env_and_set_up_clients(cluster1_name="Active Cluster",
                                                                  cluster2_name="Read-Replica Cluster")
    table_name = "t1"
    column_family = "cf"
    column = f"{column_family}:c1"

    if not args.skip_table_cleanup_on_start:
        HBaseDockerClient.clean_up_tables(active_cluster, replica_cluster)

    # Create a table on the active cluster and have it appear on the read-replica cluster
    active_cluster.create_table(table_name, column_family)
    replica_cluster.refresh_meta()

    test_put_delete_behavior(active_cluster, replica_cluster, table_name, column)

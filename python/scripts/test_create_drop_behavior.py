#!/usr/bin/env python3
from dotenv import load_dotenv
from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient, HBaseShellCommandError
from python.src.logger_config import get_logger

logger = get_logger(__name__)


def test_table_creation_behavior(active_cluster, replica_cluster, table_name, column_family):
    """
    Tests table creation behavior for read-replica clusters. It creates a table on the active
    cluster, and then runs refresh_meta on the replica cluster and verifies the table's existence.
    It does a similar process when dropping the table on the active cluster. It also verifies
    tables cannot be created/dropped on the replica cluster.
    """
    # We should not be able to create a new table on the read-replica cluster
    replica_cluster.assert_read_only_error_occurs('create', table_name, column_family)

    active_cluster.create_table(table_name, column_family)

    # Read-Replica cluster should not see the newly created table yet
    logger.info(f"Verifying {active_cluster.name} now has table '{table_name}', "
                f"while {replica_cluster.name} cluster does not")
    active_cluster.assert_table_exists(table_name)
    replica_cluster.assert_table_does_not_exist(table_name)

    # Read-Replica cluster should now see the newly created table
    replica_cluster.refresh_meta()
    logger.info(f"Verifying {replica_cluster.name} has table '{table_name}' after refreshing meta")
    replica_cluster.assert_table_exists(table_name)
    active_cluster.assert_table_exists(table_name)

    # Cannot drop the table on the Read-Replica cluster. A WriteAttemptedOnReadOnlyClusterException should occur
    replica_cluster.disable_table(table_name)
    replica_cluster.assert_read_only_error_occurs('drop', table_name, column_family)
    # The table should still exist on the read-replica cluster since drops are not allowed
    replica_cluster.assert_table_exists(table_name)

    # Drop the table on the active cluster
    active_cluster.disable_table(table_name)
    active_cluster.drop_table(table_name)

    # The read-replica cluster should still have the table that was dropped on the active
    # cluster since 'refresh_meta' has not been run yet.
    logger.info(f"Verifying {replica_cluster.name} still has table '{table_name}'")
    active_cluster.assert_table_does_not_exist(table_name)
    replica_cluster.assert_table_exists(table_name)

    # The read-replica cluster no longer has the dropped table after running 'refresh_meta'.
    logger.info(f"Verifying {replica_cluster.name} no longer has table '{table_name}' after "
                f"refreshing meta")
    replica_cluster.refresh_meta()
    replica_cluster.assert_table_does_not_exist(table_name)


if __name__ == "__main__":
    # Load settings from .env file
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")
    table_name = "t1"
    column_family = "cf"

    active_cluster = HBaseDockerClient(container_name=container_name,
                                       local_conf=f"{get_env('ACTIVE_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                       hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                       cluster_name="Active Cluster")
    replica_cluster = HBaseDockerClient(container_name=f"{container_name}-2",
                                        local_conf=f"{get_env('REPLICA_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                        hbase_ui_port=get_env('REPLICA_CLUSTER_PORT'),
                                        cluster_name="Read-Replica Cluster")
    try:
        # Delete any lingering tables
        logger.info(f"Checking if table '{table_name}' already exists on {active_cluster.name} "
                    f"and dropping it if necessary")
        HBaseDockerClient.clean_up_tables(active_cluster, replica_cluster)

        test_table_creation_behavior(active_cluster, replica_cluster, table_name, column_family)
    except (RuntimeError, HBaseShellCommandError, KeyboardInterrupt) as e:
        logger.error(f"An error occurred:\n{e}")
        logger.info("Cleaning up any tables that may be remaining")
        HBaseDockerClient.clean_up_tables(active_cluster, replica_cluster)

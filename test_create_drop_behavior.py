#!/usr/bin/env python3
from dotenv import load_dotenv
from environment_loader import get_env
from hbase_docker_client import HBaseDockerClient, HBaseShellCommandError
from logger_config import get_logger

logger = get_logger(__name__)


def test_table_creation_behavior(active_cluster, replica_cluster, table_name, column_family):
    """
    Tests table creation behavior for read-replica clusters. It creates a table on the active
    cluster, and then runs refresh_meta on the replica cluster and verifies the table's existence.
    It does a similar process when dropping the table on the active cluster. It also verifies
    tables cannot be created/dropped on the replica cluster.
    """
    # We should not be able to create a new table on the read-replica cluster
    replica_cluster.verify_read_only_error_occurs('create', table_name, column_family)

    active_cluster.create_table(table_name, column_family)

    # Read-Replica cluster should not see the newly created table yet
    logger.info(f"Verifying {active_cluster.name} now has table '{table_name}', "
                f"while {replica_cluster.name} cluster does not")
    assert active_cluster.verify_table_exists(table_name), \
        f"Expected table '{table_name}' to exist on {active_cluster.name}"
    assert not replica_cluster.verify_table_exists(table_name), \
        f"Table '{table_name}' should not exist on {replica_cluster.name}"

    # Read-Replica cluster should now see the newly created table
    replica_cluster.refresh_meta()
    logger.info(f"Verifying {replica_cluster.name} has table '{table_name}' after refreshing meta")
    assert replica_cluster.verify_table_exists(table_name), \
        (f"Expected table '{table_name}' to exist on {replica_cluster.name} "
         f"after running refresh_meta")
    assert active_cluster.verify_table_exists(table_name), \
        (f"Expected table '{table_name}' to exist on {active_cluster.name} "
         f"after running refresh_meta")

    # Cannot drop the table on the Read-Replica cluster. A DoNotRetryIOException should occur
    replica_cluster.disable_table(table_name)
    replica_cluster.verify_read_only_error_occurs('drop', table_name, column_family)
    # The table should still exist on the read-replica cluster since drops are not allowed
    assert replica_cluster.verify_table_exists(table_name), \
        (f"Expected table '{table_name}' to still exist on {replica_cluster.name} "
         f"after drop attempt")

    # Drop the table on the active cluster
    active_cluster.disable_table(table_name)
    active_cluster.drop_table(table_name)

    # The read-replica cluster should still have the table that was dropped on the active
    # cluster since 'refresh_meta' has not been run yet.
    logger.info(f"Verifying {replica_cluster.name} still has table '{table_name}'")
    assert not active_cluster.verify_table_exists(table_name), \
        f"Expected table '{table_name}' to have been dropped on {active_cluster.name}"
    assert replica_cluster.verify_table_exists(table_name), \
        (f"Table '{table_name}' should still exist on {replica_cluster.name} "
         f"since 'refresh_meta' has not been run again")

    # The read-replica cluster no longer has the dropped table after running 'refresh_meta'.
    logger.info(f"Verifying {replica_cluster.name} no longer has table '{table_name}' after "
                f"refreshing meta")
    replica_cluster.refresh_meta()
    assert not replica_cluster.verify_table_exists(table_name), \
        (f"Expected table '{table_name}' no longer exist on {replica_cluster.name} "
         f"after running 'refresh_meta'")


def verify_invalid_read_only_command_occurs(replica_cluster, cmd_type, table_name, column_family):
    """
    Runs a 'create' or 'drop' command on the read-replica cluster and expects an error to occur
    as a result.
    """
    logger.info(f"Verifying {replica_cluster.name} cannot {cmd_type} '{table_name}' since it is in "
                f"read-only mode")
    try:
        # This should throw an exception
        if cmd_type.lower() == 'create':
            replica_cluster.create_table(table_name, column_family)
        elif cmd_type.lower() == 'drop':
            replica_cluster.drop_table(table_name)

        # If we get here, then the table was dropped on the read-replica cluster, which should
        # not have happened.
        raise RuntimeError(f"Expected {cmd_type} table attempt '{table_name}' on "
                           f"{replica_cluster.name} to result in an error")
    except HBaseShellCommandError as e:
        expected_error = ("org.apache.hadoop.hbase.DoNotRetryIOException: "
                          "Operation not allowed in Read-Only Mode")
        assert expected_error in str(e), (f"Expected exception to contain the following: "
                                          f"{expected_error}\n"
                                          f"The actual exception was:\n{e}")
    logger.info(f"{cmd_type.capitalize()} table attempt on {replica_cluster.name} "
                f"failed as expected")


if __name__ == "__main__":
    # Load settings from .env file
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")
    table_name = "t1"
    column_family = "cf"

    active_cluster = HBaseDockerClient(container_name=container_name,
                                       local_conf=get_env('ACTIVE_CLUSTER_CONF'),
                                       hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                       cluster_name="Active Cluster")
    replica_cluster = HBaseDockerClient(container_name=f"{container_name}-2",
                                        local_conf=get_env('REPLICA_CLUSTER_CONF'),
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

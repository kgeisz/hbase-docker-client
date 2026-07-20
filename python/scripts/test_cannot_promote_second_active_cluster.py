#!/usr/bin/env python3
"""
Verifies a cluster cannot be promoted to an active cluster when another active cluster already exists.

The test starts with two Read-Replica HBase clusters, where one cluster is the active cluster and the other cluster is
the replica cluster. The test tries to promote the replica cluster to a second active cluster and expects an error to
occur. It then verifies this "second active cluster" is still in read-only mode and that data can still be added to the
actual active cluster.

This test script verifies the fix for:

HBASE-30220: A replica cluster can have read-only mode disabled even when another active cluster already exists
https://issues.apache.org/jira/browse/HBASE-30220

Before implementing the fix for HBASE-30220, a cluster could be promoted to from a replica cluster to an active cluster
even when another active cluster already existed.
"""
from dotenv import load_dotenv
from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient, DockerExecCommandError
from python.src.logger_config import get_logger
from python.scripts.test_read_only_flag_flipping import create_table_and_test_active_and_replica_clusters
from python.src.utils import assert_crud_operations_work_on_active_cluster, assert_correct_active_cluster_suffix
from time import sleep

logger = get_logger(__name__)


def assert_error_when_trying_to_have_second_active_cluster(replica_cluster: HBaseDockerClient, expected_error: str):
    try:
        replica_cluster.disable_read_only_mode()
        raise RuntimeError(f"Expected an DockerExecCommandError with the following error message:\n\n"
                           f"{expected_error}")
    except DockerExecCommandError as e:
        assert expected_error in str(e), (f"Expected DockerExecCommandError to contain the following message:\n\n"
                                          f"{str(expected_error)}\n\n"
                                          f"Got the following message instead:\n\n{str(e)}")
        logger.info(f"Successfully prevented {replica_cluster.name} from becoming a second active cluster")


def run_test_iteration(active_cluster: HBaseDockerClient, replica_cluster: HBaseDockerClient, data_root: str):
    create_table_and_test_active_and_replica_clusters(active_cluster, replica_cluster)
    assert_error_when_trying_to_have_second_active_cluster(replica_cluster, expected_error_msg)

    # Cluster should still be in read-only mode after failed transition from read-only to read-write mode
    replica_cluster.assert_read_only_error_occurs('create', 'test_table', column_family)

    assert_crud_operations_work_on_active_cluster(active_cluster)

    # Demote active cluster to replica and promote original replica to be the new active cluster
    active_cluster.enable_read_only_mode()
    replica_cluster.disable_read_only_mode()
    active_cluster = replica_cluster

    # Wait for active cluster file to be updated and verify its contents
    sleep(3)
    assert_correct_active_cluster_suffix(active_cluster, data_root)


if __name__ == '__main__':
    # Load settings from .env file
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")
    data_store_root = get_env("HBASE_DATA_STORE_ROOT")
    docker_compose_file = get_env("DOCKER_COMPOSE_FILE")
    column_family = "cf"

    expected_error_msg = ("ReadOnlyTransitionException: Cannot disable read-only mode because another active cluster "
                          "already exists on this storage location. The read-only coprocessors have not been removed.")

    cluster1 = HBaseDockerClient(container_name=container_name,
                                 local_conf=f"{get_env('ACTIVE_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                 hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                 cluster_name="Cluster 1")
    cluster2 = HBaseDockerClient(container_name=f'{container_name}-2',
                                 local_conf=f"{get_env('REPLICA_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                 hbase_ui_port=get_env('REPLICA_CLUSTER_PORT'),
                                 cluster_name="Cluster 2")

    HBaseDockerClient.stop_containers(docker_compose_file=docker_compose_file, data_dir=f'{data_store_root}/*', sudo=True)
    cluster1.disable_read_only_mode(run_update_all_config=False)
    cluster2.enable_read_only_mode(run_update_all_config=False)
    HBaseDockerClient.start_or_restart_containers(docker_compose_file=docker_compose_file,
                                                  data_store_root=f'{data_store_root}')
    HBaseDockerClient.wait_for_clusters_to_start([cluster1, cluster2])
    assert_correct_active_cluster_suffix(cluster1, data_store_root)
    HBaseDockerClient.clean_up_tables(active_cluster=cluster1, replica_cluster=cluster2)

    test_iterations = 5
    for i in range(1, test_iterations+1):
        logger.info(f"---------- Iteration {i} ----------")
        if i % 2 == 1:
            run_test_iteration(active_cluster=cluster1, replica_cluster=cluster2, data_root=data_store_root)
        else:
            run_test_iteration(active_cluster=cluster2, replica_cluster=cluster1, data_root=data_store_root)
        logger.info(f"Finished iteration {i} of {test_iterations}")

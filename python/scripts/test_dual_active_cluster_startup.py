#!/usr/bin/env python3
"""
Verifies that two clusters cannot both start with read-only mode disabled (both as active clusters)
on the same shared data store. One cluster must fail to start, with the HMaster process not
running, and an error logged to the master log.

Usage: python3 ./python/scripts/test_dual_active_cluster_startup.py
"""
import subprocess
import time

from dotenv import load_dotenv
from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger

logger = get_logger(__name__)

STARTUP_WAIT_SECONDS = 60
EXPECTED_ERROR_MSG = "Another cluster is running in active (read-write) mode on this storage location"


def set_readonly_disabled(cluster: HBaseDockerClient):
    logger.info(f"Setting hbase.global.readonly.enabled=false on {cluster.name}")
    cluster.set_hbase_conf_property_value('hbase.global.readonly.enabled', 'false')
    actual = cluster.get_hbase_conf_property_value('hbase.global.readonly.enabled')
    assert actual == 'false', (
        f"Expected hbase.global.readonly.enabled=false on {cluster.name}, got '{actual}'"
    )


def are_containers_running() -> bool:
    result = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "-q"],
        capture_output=True,
        text=True
    )
    return bool(result.stdout.strip())


def start_or_restart_containers():
    if are_containers_running():
        command = ["docker", "compose", "restart"]
        action = "restart"
    else:
        command = ["docker", "compose", "up", "-d"]
        action = "start"

    logger.info(f"Running 'docker compose {action}' for both containers")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"docker compose {action} failed (exit {result.returncode}):\n"
            f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )
    logger.info(f"docker compose {action} completed successfully")


def is_process_running(cluster: HBaseDockerClient, process_name: str) -> bool:
    output = cluster.run_docker_exec_command("jps")
    return process_name in output


def check_cluster_processes(cluster: HBaseDockerClient) -> bool:
    hmaster_running = is_process_running(cluster, "HMaster")
    logger.info(f"  {cluster.name}: HMaster={'running' if hmaster_running else 'down'}")
    return hmaster_running


def assert_error_in_master_log(cluster: HBaseDockerClient):
    logger.info(f"Checking {cluster.name} master log for expected error message")
    log_output = cluster.run_docker_exec_command(
        "cat /opt/hbase/logs/hbase-*-master-*.log || true"
    )
    assert EXPECTED_ERROR_MSG in log_output, (
        f"Expected {cluster.name}'s master log to contain:\n"
        f"  '{EXPECTED_ERROR_MSG}'\n"
        f"but it was not found.\nLog tail:\n{log_output[-2000:]}"
    )
    logger.info(f"  [PASS] Found expected error message in {cluster.name}'s master log")


if __name__ == '__main__':
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")

    cluster1 = HBaseDockerClient(container_name=container_name,
                                 local_conf=f"{get_env('ACTIVE_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                 hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                 cluster_name="Cluster 1")
    cluster2 = HBaseDockerClient(container_name=f'{container_name}-2',
                                 local_conf=f"{get_env('REPLICA_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                 hbase_ui_port=get_env('REPLICA_CLUSTER_PORT'),
                                 cluster_name="Cluster 2")

    # Set both clusters to active mode (read-only disabled)
    set_readonly_disabled(cluster1)
    set_readonly_disabled(cluster2)

    # Start or restart containers so both attempt to start as active
    start_or_restart_containers()

    # Wait for HBase to attempt startup on both containers
    logger.info(f"Waiting {STARTUP_WAIT_SECONDS}s for clusters to attempt startup...")
    time.sleep(STARTUP_WAIT_SECONDS)

    # Determine which cluster failed
    logger.info("Checking HBase processes on both clusters")
    cluster1_running = check_cluster_processes(cluster1)
    cluster2_running = check_cluster_processes(cluster2)

    if cluster1_running and not cluster2_running:
        failed_cluster = cluster2
        running_cluster = cluster1
    elif cluster2_running and not cluster1_running:
        failed_cluster = cluster1
        running_cluster = cluster2
    elif not cluster1_running and not cluster2_running:
        raise RuntimeError("Both clusters appear to be down — this is unexpected")
    else:
        raise RuntimeError(
            "Both clusters appear to be running — the test expects exactly one to have failed. "
            "This may indicate the clusters are using separate data stores or the feature is not working."
        )

    logger.info(f"[PASS] {running_cluster.name} is running as the active cluster")
    logger.info(f"[PASS] {failed_cluster.name} failed to start (HMaster is down)")

    # Verify the failed cluster's master log contains the expected error
    assert_error_in_master_log(failed_cluster)

    logger.info("=" * 60)
    logger.info("TEST PASSED: Dual active cluster startup correctly rejected")
    logger.info("=" * 60)
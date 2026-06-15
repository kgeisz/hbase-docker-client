#!/usr/bin/env python3
from dotenv import load_dotenv
from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger

logger = get_logger(__name__)


if __name__ == "__main__":
    # Load settings from .env file
    load_dotenv()
    active_port = get_env('ACTIVE_CLUSTER_PORT')
    replica_port = get_env('REPLICA_CLUSTER_PORT')
    container_base = get_env('HBASE_CONTAINER_NAME')

    active_cluster = HBaseDockerClient(container_name=container_base,
                                       local_conf=f"{get_env('ACTIVE_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                       hbase_ui_port=active_port,
                                       cluster_name="Active Cluster")
    replica_cluster = HBaseDockerClient(container_name=f"{container_base}-2",
                                        local_conf=f"{get_env('REPLICA_CLUSTER_CONF_DIR')}/hbase-site.xml",
                                        hbase_ui_port=replica_port,
                                        cluster_name="Read-Replica Cluster")

    HBaseDockerClient.wait_for_clusters_to_start([active_cluster, replica_cluster])

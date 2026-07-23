#!/usr/bin/env python3
"""
Verifies the hbase-docker containers have started properly. For each cluster, the script first
curls the HBase UI until it receives a 200 response and then gets the server status to verify
there are no dead clusters
"""
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger
from python.src.utils import load_env_and_set_up_clients

logger = get_logger(__name__)


def main():
    active_cluster, replica_cluster = load_env_and_set_up_clients(cluster1_name="Active Cluster",
                                                                  cluster2_name="Read-Replica Cluster")
    HBaseDockerClient.wait_for_clusters_to_start([active_cluster, replica_cluster])


if __name__ == "__main__":
    main()

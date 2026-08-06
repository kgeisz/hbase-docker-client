"""
Use this script to quickly set up HBaseDockerClient objects in your Python console.

To set up the environment, start the Python console from the hbase-docker-client directory and run:

from python.scripts.python_console_env_setup import *
"""
from python.src.environment_loader import get_env
from python.src.utils import load_env_and_set_up_clients

cluster1, cluster2 = load_env_and_set_up_clients()

hbase_image = get_env("HBASE_IMAGE")
hbase_container_name = get_env("HBASE_CONTAINER_NAME")
hbase_conf_dir = get_env("HBASE_CONF_DIR")
data_store_root = get_env("HBASE_DATA_STORE_ROOT")
active_cluster_port = get_env("ACTIVE_CLUSTER_PORT")
replica_cluster_port = get_env("REPLICA_CLUSTER_PORT")
active_cluster_conf_dir = get_env("ACTIVE_CLUSTER_CONF_DIR")
replica_cluster_conf_dir = get_env("REPLICA_CLUSTER_CONF_DIR")
docker_compose_file = get_env("DOCKER_COMPOSE_FILE")
log_level = get_env("LOG_LEVEL")

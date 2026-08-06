# hbase-docker-client

A Python test suite for validating the HBase Read-Replica feature ([HBASE-29081](https://issues.apache.org/jira/browse/HBASE-29081)). It uses Docker containers running HBase to verify that a read-only replica cluster correctly mirrors an active cluster's data while rejecting write operations.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- Python 3.12+
- [Apache Maven](https://maven.apache.org/install.html) (for building HBase from source)
- Git

## Setup

1. Clone this repository:
```bash
git clone https://github.com/kgeisz/hbase-docker-client.git
cd hbase-docker-client
```

2. Create and activate a Python virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4. Update the `.env` file. The paths in `.env` default to `$HOME/projects/hbase-docker-client`. If you cloned the repository to a different location, update the paths accordingly.

## Building the HBase Docker Image

The Docker image is built from the Apache HBase source. The HBase source must be cloned into the root of this project so that an `./hbase` directory exists alongside the `Dockerfile` and `build-images.sh`.

1. Clone the Apache HBase repository into this project's root directory:
```bash
git clone https://github.com/apache/hbase.git
```

2. Check out the branch that contains the Read-Replica feature (typically `master`):
```bash
cd hbase
git checkout master
cd ..
```

3. Run the build script:
```bash
./build-images.sh
```

This script reads the `HBASE_IMAGE` variable from `.env`, runs `mvn clean` in the `./hbase` directory to remove previous build artifacts, and then runs `docker build` to compile HBase from source and package it into a Docker image.

The first build compiles HBase from source inside Docker and can take significant time. Subsequent builds benefit from Docker's layer caching.

## Running the Tests

1. Clean any previous data and start the containers:
```bash
rm -rf ./tmp && docker compose up -d
```

This starts two HBase containers: an active cluster and a read-replica cluster.

2. Set the `PYTHONPATH`:
```bash
export PYTHONPATH="$(pwd)"
```

3. Compile `ActiveClusterSuffix.proto`
```bash
# Optional: Copy file from HBase repo to ensure you have the latest version
cp hbase/hbase-protocol-shaded/src/main/protobuf/server/ActiveClusterSuffix.proto python/proto

# Make sure Python environment is active
python3 python/proto/proto_compiler.py
```

5. Run a test script:
```bash
# Make sure Python environment is active
python3 ./python/scripts/verify_hbase_start.py
```

## Test Scripts

All scripts are in `python/scripts/` and run with:

```bash
# Make sure Python environment is active
python3 ./python/scripts/<script_name>.py [options]
```

### `verify_hbase_start.py`

Polls the HBase Web UIs on both clusters until they return HTTP 200, then confirms there are no dead servers.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--skip-container-start-or-restart` | `-s` | `false` | Skip stopping, starting, and waiting for containers |

### `test_put_get_delete_behavior.py`

Verifies put/delete on the active cluster, data visibility on the replica after flush and refresh, and that writes on the replica are rejected. Also checks that `flush` on the replica does not hang (HBASE-30301).

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--skip-table-cleanup-on-start` | `-t` | `false` | Skip cleaning up tables at the start of the test |

### `test_create_drop_behavior.py`

Tests table create/drop on the active cluster and verifies these operations are rejected on the replica.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--skip-table-cleanup-on-start` | `-t` | `false` | Skip cleaning up tables at the start of the test |

### `test_read_only_flag_flipping.py`

Repeatedly flips the read-only flag between two clusters and validates that the `active-cluster` suffix file is updated correctly each time.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--skip-container-start-or-restart` | `-s` | `false` | Skip stopping, starting, and waiting for containers |

### `test_bulkloaded_data_and_region_splits.py`

Tests bulk loading and region splits across clusters, including role swaps and visibility after refresh.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--skip-container-start-or-restart` | `-s` | `false` | Skip stopping, starting, and waiting for containers |

### `test_cannot_promote_second_active_cluster.py`

Verifies that a replica cluster cannot be promoted to active when another active cluster already exists on the same shared data store (HBASE-30220).

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--skip-container-start-or-restart` | `-s` | `false` | Skip stopping, starting, and waiting for containers |

### `test_dual_active_cluster_startup.py`

Starts two clusters both configured as active against the same data store and verifies exactly one fails to start.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--clean-up-containers` | `-c` | `false` | Stop containers and revert cluster configurations after the test finishes |

## HBaseDockerClient

The core class in `python/src/hbase_docker_client.py` wraps `docker exec` to run HBase shell commands against a named container from outside it. Each instance represents one cluster.

### Constructor

```python
HBaseDockerClient(
    container_name="hbase-docker",
    local_conf="conf1/hbase-site.xml",
    hbase_ui_port=16010,
    cluster_name="Active Cluster",
)
```

- `container_name` — Docker container to run commands against.
- `local_conf` — Path to the local `hbase-site.xml` (mounted as a Docker volume, so edits propagate into the container).
- `hbase_ui_port` — Port for the HBase Master UI health check.
- `cluster_name` — Human-readable label used in log messages.

### Command Execution

| Method | Description |
|--------|-------------|
| `run_docker_exec_command(bash_cmd)` | Runs an arbitrary bash command inside the container via `docker exec`. |
| `run_hbase_shell_command(hbase_cmd)` | Runs an HBase shell command inside the container. |

### Cluster Health

| Method | Description |
|--------|-------------|
| `wait_for_hbase_ui()` | Polls the Master UI until it returns HTTP 200. |
| `check_server_status()` | Validates the cluster has the expected number of masters, region servers, and no dead servers. |
| `wait_for_cluster_to_start()` | Combines `wait_for_hbase_ui` and `check_server_status`. |

### Table Operations

| Method | Description |
|--------|-------------|
| `create_table(table_name, column_family)` | Creates a table with one column family. |
| `disable_table(table_name)` | Disables a table. |
| `drop_table(table_name)` | Drops a table. |
| `list_tables()` | Returns the list of tables as a Python list. |
| `list_regions(table_name)` | Returns region info for a table. |

### Data Operations

| Method | Description |
|--------|-------------|
| `put(table_name, row, column, data)` | Inserts a cell value. |
| `get(table_name, row, column)` | Reads a cell value. |
| `delete(table_name, row, column)` | Deletes a cell. |
| `scan(table_name)` | Scans an entire table. |
| `count(table_name)` | Counts rows in a table. |

### Maintenance

| Method | Description |
|--------|-------------|
| `flush(table_name)` | Flushes the memstore to HFiles. |
| `split(thing_to_split, split_key)` | Triggers an async region split. |
| `flush_and_split(thing_to_split, split_key)` | Flushes then splits. |
| `major_compact(table_or_region)` | Triggers major compaction. |
| `major_compact_and_wait(table_or_region)` | Triggers major compaction and blocks until complete. |
| `catalogjanitor_run()` | Forces cleanup of split parent regions in `hbase:meta`. |

### Replica Sync

| Method | Description |
|--------|-------------|
| `refresh_meta()` | Refreshes table metadata on the replica. |
| `refresh_hfiles()` | Refreshes data files on the replica. |
| `refresh_meta_and_hfiles()` | Runs both in sequence. |

### Read-Only Mode

| Method | Description |
|--------|-------------|
| `enable_read_only_mode()` | Sets `hbase.global.readonly.enabled=true` in the local `hbase-site.xml` and runs `update_all_config` to apply dynamically. |
| `disable_read_only_mode()` | Sets `hbase.global.readonly.enabled=false` and applies dynamically. |

### Assertion Helpers

Used by test scripts to validate expected state:

| Method | Description |
|--------|-------------|
| `assert_read_only_error_occurs(cmd_type, table_name, ...)` | Runs a write command and asserts it raises `WriteAttemptedOnReadOnlyClusterException`. |
| `assert_table_exists(table_name)` | Asserts the table is in the table list. |
| `assert_table_does_not_exist(table_name)` | Asserts the table is not in the table list. |
| `assert_table_row_count(table_name, expected)` | Asserts the table has the expected row count. |
| `assert_get_output(table, row, cf, expected_data)` | Asserts a get returns the expected value. |
| `assert_region_count_for_table(table_name, expected)` | Asserts the table has the expected number of regions. |

### Static Container Lifecycle Methods

| Method | Description |
|--------|-------------|
| `start_or_restart_containers(docker_compose_file)` | Starts or restarts containers depending on current state. |
| `stop_containers(docker_compose_file, data_dir)` | Stops containers and optionally removes the data directory. |
| `are_containers_running(docker_compose_file)` | Returns whether containers are currently running. |
| `set_up_data_store_dir(data_store_root)` | Creates the data store directory structure with appropriate permissions. |

## Architecture

The `docker-compose.yml` starts two HBase containers that share the same data store (`tmp/data-store/hbase`) via mounted volumes:

- **Active Cluster** (`hbase-docker`): Configured via `conf1/hbase-site.xml` with `hbase.global.readonly.enabled=false`. Master UI on port 16010.
- **Replica Cluster** (`hbase-docker-2`): Configured via `conf2/hbase-site.xml` with `hbase.global.readonly.enabled=true`. Master UI on port 26010.

The replica sees changes from the active cluster only after an explicit sync: flush the table on the active cluster, then run `refresh_meta` and `refresh_hfiles` on the replica.

## Interactive Python Console

The `python/scripts/python_console_env_setup.py` script lets you quickly set up `HBaseDockerClient` objects in an interactive Python shell for ad-hoc cluster interaction.

### Prerequisites

- `.env` is configured for your environment
- Containers are running (`docker compose up -d`)
- Python virtual environment is activated

### Usage

Start a Python console from the project root and run:

```python
from python.scripts.python_console_env_setup import *
```

This creates two client objects:

- `cluster1` — the active cluster
- `cluster2` — the replica cluster

All `.env` variables are also loaded into scope (e.g. `hbase_image`, `active_cluster_port`).

### Examples

Put data on the active cluster and sync it to the replica:

```python
cluster1.put("my_table", "row1", "cf:col1", "value1")
cluster1.flush("my_table")
cluster2.refresh_meta_and_hfiles()
cluster2.get("my_table", "row1", "cf:col1")
```

Make the active cluster read-only:

```python
cluster1.enable_read_only_mode()
```

Make the replica cluster writable:

```python
cluster2.disable_read_only_mode()
```

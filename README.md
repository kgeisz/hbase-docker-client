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

3. Run a test script:
```bash
python3 ./python/scripts/verify_hbase_start.py
```

Other available test scripts in `python/scripts/`:
- `test_put_get_delete_behavior.py` — Verifies put/delete on the active cluster and data visibility on the replica.
- `test_create_drop_behavior.py` — Tests table create/drop on the active cluster and rejection on the replica.
- `test_read_only_flag_flipping.py` — Flips the read-only flag between clusters and validates behavior.

## Architecture

The `docker-compose.yml` starts two HBase containers that share the same data store (`tmp/data-store/hbase`) via mounted volumes:

- **Active Cluster** (`hbase-docker`): Configured via `conf1/hbase-site.xml` with `hbase.global.readonly.enabled=false`. Master UI on port 16010.
- **Replica Cluster** (`hbase-docker-2`): Configured via `conf2/hbase-site.xml` with `hbase.global.readonly.enabled=true`. Master UI on port 26010.

The replica sees changes from the active cluster only after an explicit sync: flush the table on the active cluster, then run `refresh_meta` and `refresh_hfiles` on the replica.

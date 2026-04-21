# hbase-docker-client
A client written in Python that can run various HBase shell commands from outside an hbase-docker container.

## Setup

Python 3.12 is the recommended version for running these scripts, but it may not be necessary.

1. Create a Python environment:
```
% python3 -m venv .venv
```
2. Activate the environment:
```
% source .venv/bin/activate
```
3. Install dependencies:
```
% pip install --upgrade pip
% pip install -r requirements.txt
```

## Usage
The scripts and `docker-compose.yml` file in this repository assume you have an `hbase-docker` image.
Information for building an `hbase-docker` image can be found here:
https://github.com/kgeisz/hbase-docker/tree/read-replica

When you use this `docker-compose.yml` file to create containers, a `tmp/data-store` directory will be created
in you `hbase-docker-client` directory. This will hold the `hbase` directory and HBase's metadata. It is recommended to
clean up this directory before starting the containers.

1. Modify the `.env` file as needed.
2. Activate your environment if you have not already done so:
```
% source .venv/bin/activate
```
3. Start the containers. One container will start as the active cluster. The other will start as the read-replica cluster.
```
% rm -rf ./tmp && docker compose -f $HOME/hbase-docker-client/docker-compose.yml up -d
```
4. Run a script:
```
python3 ./verify_hbase_start.py
```

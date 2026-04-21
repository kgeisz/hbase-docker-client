#!/usr/bin/env python3
import ast
import logging
import re
import subprocess
import time
import xml.etree.ElementTree as ET
import requests

from logger_config import get_logger

logger = get_logger(__name__)


class DockerExecCommandError(Exception):
    pass


class HBaseShellCommandError(Exception):
    pass


class HBaseDockerClient:
    def __init__(self, container_name, local_conf, hbase_ui_port=16010, cluster_name="HBase Cluster",
                 max_retries=12, sleep_time=5):
        self._container_name = container_name
        self._local_conf = local_conf
        self._hbase_ui_port = hbase_ui_port
        self._cluster_name = cluster_name
        self._max_retries = max_retries
        self._sleep_time = sleep_time

    @property
    def name(self):
        return self._cluster_name

    def wait_for_hbase_ui(self):
        """Checks for a 200 OK on the HBase Master UI."""
        url = f"http://localhost:{self._hbase_ui_port}"
        logger.info(f"Waiting for HBase UI: {self._cluster_name} on {url}")
        last_exception = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    logger.info(f"SUCCESS: {self._cluster_name} UI is up.")
                    return True
            except requests.exceptions.ConnectionError as e:
                last_exception = e
            logging.info(f"Waiting {self._sleep_time} seconds before requesting HBase UI again")
            time.sleep(self._sleep_time)

        raise RuntimeError(f"\nTIMEOUT: {self._cluster_name} UI failed to respond after "
                           f"{self._max_retries} attempts. "
                           f"Last raised exception was: {last_exception}")

    def check_server_status(self):
        """Runs 'status' inside the HBase shell and validates the output."""
        logger.info(f"Validating Cluster Status: {self._cluster_name} ({self._container_name})")
        for attempt in range(1, self._max_retries + 1):
            try:
                output = self.get_hbase_status()

                # The cluster's status should have 1 active master, 1 region server,
                # and no dead servers
                validations = {
                    "Active Master": "1 active master" in output,
                    "Region Server": "1 servers" in output,
                    "No Dead Servers": "0 dead" in output
                }

                if all(validations.values()):
                    for check, status in validations.items():
                        logger.info(f"    [PASS] {check}")
                    logger.info(f"SUCCESS: {self._cluster_name} is fully operational.")
                    return True
                else:
                    logger.warning(f"{self._cluster_name} is responding, but not all "
                                   f"components are ready...")
                    logger.info(f"HBase 'status' command output:\n{output}")

            except subprocess.CalledProcessError:
                pass

            logging.info(f"Waiting {self._sleep_time} seconds before getting status on "
                         f"{self.name} again")
            time.sleep(self._sleep_time)

        raise RuntimeError(
            f"\nTIMEOUT: {self._cluster_name} shell check failed after {self._max_retries} attempts.")

    def __run_command(self, bash_cmd):
        cmd = ["docker", "exec", self._container_name, "bash", "-c", f'''{bash_cmd}''']
        cmd_str = ' '.join(cmd)
        logger.debug(f"Running command on {self._cluster_name}: {cmd_str}")
        process = subprocess.run(cmd, capture_output=True)
        stdout = process.stdout.decode('utf-8')
        if process.returncode != 0:
            raise DockerExecCommandError(f"The following docker exec command failed on "
                                         f"{self._cluster_name} ({self._container_name}): "
                                         f"{bash_cmd}\nThe docker command used to run this was: "
                                         f"{cmd_str}\nThe command's STDERR was:"
                                         f"\n{process.stderr.decode('utf-8')}\n"
                                         f"The command's STDOUT was:\n{stdout}\n")
        return stdout

    def __run_hbase_command(self, hbase_cmd):
        # In the Terminal, we usually put double quotes around everything after "-c", but doing that
        # with subprocess.run() results in a failure.
        cmd = ["docker", "exec", self._container_name, "bash", "-c",
               f'''hbase shell -n <<< "{hbase_cmd}"''']
        cmd_str = ' '.join(cmd)

        logger.debug(f"Running command on {self._cluster_name}: {cmd_str}")
        process = subprocess.run(cmd, capture_output=True)
        stdout = process.stdout.decode('utf-8')
        if process.returncode != 0:
            raise HBaseShellCommandError(f"The following HBase shell command failed on the "
                                         f"{self._cluster_name} ({self._container_name}): "
                                         f"{hbase_cmd}\nThe docker command used to run this was: "
                                         f"{cmd_str}\nThe shell command's STDERR was:"
                                         f"\n{process.stderr.decode('utf-8')}\n"
                                         f"The shell command's STDOUT was:\n{stdout}\n")
        return stdout

    def create_table(self, table_name, column_family):
        logger.info(f"Creating table '{table_name}' on {self._cluster_name}")
        create_cmd = f"create '{table_name}', '{column_family}'"
        output = self.__run_hbase_command(create_cmd)

        if f"Created table {table_name}" not in output:
            logger.error(f"Could not create table '{table_name}' on {self._cluster_name}")
            return False
        return True

    def list_tables(self):
        """Gets the list of HBase tables and returns it as a Python list"""
        logger.debug(f"Getting the list of tables in HBase on {self.name}")
        pattern = r'\[(.*?)\]'
        output = self.__run_hbase_command("list")
        match = re.search(pattern, output)
        return ast.literal_eval(match.group(0))

    def verify_table_exists(self, table_name):
        logger.debug(f"Verifying '{table_name}' is in the list of tables on {self.name}")
        return table_name in self.list_tables()

    def get_hbase_status(self):
        logger.debug(f"Getting status of {self.name}")
        return self.__run_hbase_command("status")

    def disable_table(self, table_name):
        logger.debug(f"Disabling table '{table_name}' on {self.name}")
        self.__run_hbase_command(f"disable '{table_name}'")

    def drop_table(self, table_name):
        logger.info(f"Dropping table '{table_name}' on {self.name}")
        self.__run_hbase_command(f"drop '{table_name}'")

    def put(self, table_name, row, column, data, spec_map=None):
        """
        Performs an HBase put command.
        :param table_name: the table we are inserting data into
        :param row: row of the table we are inserting data into
        :param column: column of the table we are inserting data into
        :param data: the actual data we are inserting (as a string)
        :param spec_map: additional attributes input as a string
                         (e.g. "{ATTRIBUTES=>{'my-key'=>'my-value'}}")
        """
        logger.info(f"Adding data to table '{table_name}' on {self.name}")
        put_cmd = f"put '{table_name}', '{row}', '{column}', '{data}'"
        if spec_map:
            put_cmd += f", {spec_map}"
        self.__run_hbase_command(put_cmd)

    def get(self, table_name, row, column=None, spec_map=None):
        logger.info(f"Getting data from table '{table_name}' on {self.name}")
        get_cmd = f"get '{table_name}', '{row}'"
        if column:
            get_cmd += f", '{column}'"
        if spec_map:
            get_cmd += f", {spec_map}"
        output = self.__run_hbase_command(get_cmd)
        logger.debug(f"Got data:\n{output}")
        return output

    def delete(self, table_name, row, column, timestamp=None, spec_map=None):
        logger.info(f"Deleting data from table '{table_name}' on {self.name}")
        delete_cmd = f"delete '{table_name}', '{row}', '{column}'"
        if timestamp:
            delete_cmd += f", {table_name}"
        if spec_map:
            delete_cmd += f", {spec_map}"
        self.__run_hbase_command(delete_cmd)

    def scan(self, table_name, spec_map=None):
        log_msg = f"Scanning table '{table_name}' on {self.name}"
        scan_cmd = f"scan '{table_name}'"
        if spec_map:
            scan_cmd += f", {spec_map}"
            log_msg += f" with spec_map {spec_map}"
        logging.info(log_msg)
        return self.__run_hbase_command(scan_cmd)

    def count(self, table_name, spec=None):
        logger.info(f"Counting rows for table '{table_name}' on {self.name}")
        count_cmd = f"count '{table_name}'"
        if spec:
            count_cmd += f"{spec}"
        return self.__run_hbase_command(count_cmd)

    def verify_table_row_count(self, table_name, expected_row_count):
        logger.info(f"Verifying table '{table_name}' on {self.name} has {expected_row_count} row(s)")
        output = self.count(table_name)
        split_output = output.split('\n')
        actual_row_count = split_output[1]
        assert actual_row_count == f"{expected_row_count} row(s)" in output, \
            (f"Expected table '{table_name}' on {self.name} to have {expected_row_count} row(s). "
             f"Instead got {actual_row_count}")

    def flush(self, table_name):
        logger.debug(f"Flushing table '{table_name}'")
        self.__run_hbase_command(f"flush '{table_name}'")

    def refresh_meta(self):
        logger.debug(f"Refreshing meta on {self.name}")
        self.__run_hbase_command("refresh_meta")

    def refresh_hfiles(self):
        logger.debug(f"Refreshing HFiles on {self.name}")
        self.__run_hbase_command("refresh_hfiles")

    def update_all_config(self):
        logger.debug(f"Running update_all_config on {self.name} to dynamically update the configuration")
        self.__run_hbase_command("update_all_config")

    def __set_read_only_mode_in_local_conf(self, value):
        """Sets hbase.global.readonly.enabled to a new value in a local hbase-site.xml file"""
        tree = ET.parse(self._local_conf)
        root = tree.getroot()
        for prop in root.findall('property'):
            name_elem = prop.find('name')
            if name_elem is not None and name_elem.text == 'hbase.global.readonly.enabled':
                value_elem = prop.find('value')
                if value_elem is not None:
                    value_elem.text = str(value)
                    break
        tree.write(self._local_conf, encoding='utf-8', xml_declaration=True)

    def enable_read_only_mode(self):
        """
        Sets hbase.global.readonly.enabled to 'true' in the local hbase-site.xml file and runs update_all_config
        to dynamically update the configuration. This method assumes the hbase-site.xml file is a mounted volume
        in the docker-compose file, which allows the config file within the docker container to be updated as well.
        """
        logger.info(f"Enabling read-only mode on {self.name}")
        self.__set_read_only_mode_in_local_conf('true')
        self.update_all_config()

    def disable_read_only_mode(self):
        """
        Sets hbase.global.readonly.enabled to 'false' in the local hbase-site.xml file and runs update_all_config
        to dynamically update the configuration. This method assumes the hbase-site.xml file is a mounted volume
        in the docker-compose file, which allows the config file within the docker container to be updated as well.
        """
        logger.info(f"Disabling read-only mode on {self.name}")
        self.__set_read_only_mode_in_local_conf('false')
        self.update_all_config()

    def verify_read_only_error_occurs(self, cmd_type, table_name, column,
                                      row=None, data=None):
        """
        Runs a command on read-only cluster and expects an error to occur as a result.
        """
        logger.info(f"Verifying we cannot perform a '{cmd_type}' on {self.name} "
                    f"since it is in read-only mode")
        try:
            # This should throw an exception
            match cmd_type.lower():
                case 'create':
                    self.create_table(table_name, column)
                case 'drop':
                    self.drop_table(table_name)
                case 'put':
                    self.put(table_name, column, row, data)
                case 'delete':
                    self.delete(table_name, row, column)
                case _:
                    raise RuntimeError(f"Unexpected command type: {cmd_type}")

            # If we get here, then the command succeeded on the read-replica cluster, which should
            # not have happened.
            raise RuntimeError(f"Expected {cmd_type} attempt on {self.name} "
                               f"to result in an error")
        except HBaseShellCommandError as e:
            # Verify the command we ran on the read-replica cluster produced the expected exception
            expected_error = ("org.apache.hadoop.hbase.WriteAttemptedOnReadOnlyClusterException: "
                              "Operation not allowed in Read-Only Mode")
            assert expected_error in str(e), (f"Expected exception to contain the following: "
                                              f"{expected_error}\n"
                                              f"The actual exception was:\n{e}")
        logger.info(f"{cmd_type.capitalize()} attempt on {self.name} failed as expected")

    @staticmethod
    def clean_up_tables(active_cluster, replica_cluster):
        """
        Drops all tables on the active cluster and then runs 'refresh_meta' on the
        read-replica cluster to remove those tables
        """
        tables = active_cluster.list_tables()
        if tables:
            logger.info(f"Removing all existing tables on {active_cluster.name}: {tables}")
            for table in tables:
                active_cluster.disable_table(table)
                active_cluster.drop_table(table)
            logger.info(f"Running 'refresh_meta' and 'refresh_hfiles' on {replica_cluster.name} to sync it with "
                        f"{active_cluster.name}")
            replica_cluster.refresh_meta()
            replica_cluster.refresh_hfiles()

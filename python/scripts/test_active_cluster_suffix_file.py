#!/usr/bin/env python3
import python.proto.generated.ActiveClusterSuffix_pb2 as acs


if __name__ == '__main__':
    active_cluster_file = '/Users/kgeiszler/projects/hbase-docker-client/tmp/data-store/hbase/active.cluster.suffix.id'
    active_cluster_suffix = acs.ActiveClusterSuffix()
    with open(active_cluster_file, 'rb') as f:
        data = f.read()
        header = b'PBUF'
        if data.startswith(header):
            active_cluster_suffix.ParseFromString(data[len(header):])
        else:
            active_cluster_suffix.ParseFromString(data)
        print(f"message.cluster_id = {active_cluster_suffix.cluster_id}")
        print(f"message.suffix = {active_cluster_suffix.suffix}")

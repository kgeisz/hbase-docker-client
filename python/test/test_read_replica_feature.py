import pytest

import python.test.test_dual_active_cluster_startup as test_dual_active_cluster_startup
import python.test.test_create_drop_behavior as test_create_drop_behavior
import python.test.test_put_get_delete_behavior as test_put_get_delete_behavior
import python.test.test_read_only_flag_flipping as test_read_only_flag_flipping
import python.test.test_cannot_promote_second_active_cluster as test_cannot_promote_second_active_cluster
import python.test.test_bulkloaded_data_and_region_splits as test_bulkloaded_data_and_region_splits


# There are rare occasions where HBase fails to initialize within the Docker container.
# We will re-run the test when this occurs.
@pytest.mark.flaky(reruns=2, only_rerun="HBaseInitializationError")
class TestReadReplica:
    def test_dual_active_cluster_startup(self):
        test_dual_active_cluster_startup.run_test(clean_up_containers=True)

    def test_create_drop_behavior(self):
        test_create_drop_behavior.run_test(drop_existing_tables=True, new_containers=True)

    def test_put_get_delete_behavior(self):
        test_put_get_delete_behavior.run_test(drop_existing_tables=True, new_containers=True)

    def test_read_only_flag_flipping(self):
        test_read_only_flag_flipping.run_test(new_containers=True)

    def test_cannot_promote_second_active_cluster(self):
        test_cannot_promote_second_active_cluster.run_test(new_containers=True)

    def test_bulkloaded_data_and_region_splits(self):
        test_bulkloaded_data_and_region_splits.run_test(new_containers=True)

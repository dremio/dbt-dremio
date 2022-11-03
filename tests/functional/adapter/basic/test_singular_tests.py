from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
)


class TestSingularTestsDremio(BaseSingularTests):
    def test_singular_tests(self, project):
        # test command
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 2

        # We have the right result nodes
        check_result_nodes_by_name(results, ["passing", "failing"])

        # Check result status
        for result in results:
            if result.node.name == "passing":
                assert result.status == "pass"
            elif result.node.name == "failing":
                assert result.status == "fail"

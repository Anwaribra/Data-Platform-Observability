from great_expectations.core import ExpectationSuite, ExpectationConfiguration


def get_dag_runs_expectation_suite() -> ExpectationSuite:

    suite = ExpectationSuite(
        expectation_suite_name="dag_runs_suite"
    )
    

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "dag_id",
                "mostly": 1.0
            },
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "dag_id is a required field and must not be null"
                }
            }
        )
    )

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "execution_date",
                "mostly": 1.0
            },
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "execution_date is required for tracking when DAGs run"
                }
            }
        )
    )
    

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "state",
                "value_set": [
                    "success",
                    "failed",
                    "running",
                    "queued",
                    "scheduled",
                    "skipped",
                    "up_for_retry",
                    "up_for_reschedule",
                    "upstream_failed",
                    "none"
                ],
                "mostly": 1.0
            },
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "state must be one of the valid Airflow DAG run states"
                }
            }
        )
    )
    
    # this is a custom expectation that expects duration to be >= 0 when not null
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "duration",
                "min_value": 0,
                "mostly": 1.0,
                "parse_strings_as_datetimes": False
            },
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "duration (in seconds) must be non-negative when present"
                }
            }
        )
    )
    
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_pair_values_A_to_be_greater_than_B",
            kwargs={
                "column_A": "end_date",
                "column_B": "start_date",
                "or_equal": True,
                "mostly": 1.0,
                "parse_strings_as_datetimes": True
            },
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "end_date should be after or equal to start_date"
                }
            }
        )
    )
    
    return suite


def get_dag_runs_expectations_dict() -> dict:
    suite = get_dag_runs_expectation_suite()
    return suite.to_json_dict()


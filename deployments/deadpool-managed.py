from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/broepke/prefect-dka.git",
        entrypoint="deadpool/deadpool.py:dead_pool_status_check",
    ).deploy(
        name="deadpool-managed",
        work_pool_name="my-managed-pool",
        job_variables={
            "pip_packages": [
                "prefect",
                "prefect-snowflake",
                "prefect-slack",
                "prefect-github",
                "prefect-aws",
                "twilio",
                "snowflake-connector-python[pandas]",
            ]
        },
    )

from __future__ import annotations

import os
import pendulum
from pathlib import Path

from airflow.models import DAG
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior

DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR
DBT_BIN = Path("/home/airflow/.local/bin/dbt")
DBT_ENV = {
    "DBT_PROFILES_DIR": str(DBT_PROFILES_DIR),
    "PATH": f"/home/airflow/.local/bin:{os.getenv('PATH', '')}",
}

default_args = {"owner": "data-eng"}

with DAG(
    dag_id="dbt_log_beaver_hourly",
    description="dbt transform pipeline for ClickHouse",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "clickhouse"],
    default_args=default_args,
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd {{ params.project_dir }} && {{ params.dbt_bin }} deps",
        params={"project_dir": str(DBT_PROJECT_DIR), "dbt_bin": str(DBT_BIN)},
        env=DBT_ENV,
    )

    dbt_transforms = DbtTaskGroup(
        group_id="dbt_transforms",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="log_beaver",
            target_name="dev",
            profiles_yml_filepath=DBT_PROFILES_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path=str(DBT_BIN)),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_ALL,
            emit_datasets=False,
        ),
        operator_args={"env": DBT_ENV},
    )

    dbt_deps >> dbt_transforms

# plugins/operators/dbt_operator.py
import os
import subprocess
import logging
from typing import Optional

from airflow.models import BaseOperator

logger = logging.getLogger(__name__)


class DbtOperator(BaseOperator):
    """
    Runs a dbt command as a subprocess.
    Equivalent to: dbt <command> --profiles-dir <profiles_dir> --project-dir <project_dir> [--select <select>] [--full-refresh]
    """
    template_fields = ("command", "select")
    ui_color = "#FF694B"

    def __init__(
        self,
        command: str,
        profiles_dir: str,
        project_dir: str,
        select: Optional[str] = None,
        full_refresh: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.command      = command
        self.profiles_dir = profiles_dir
        self.project_dir  = project_dir
        self.select       = select
        self.full_refresh = full_refresh

    def execute(self, context):
        cmd = [
            "dbt", self.command,
            "--profiles-dir", self.profiles_dir,
            "--project-dir",  self.project_dir,
        ]

        if self.select:
            cmd += ["--select", self.select]

        if self.full_refresh:
            cmd.append("--full-refresh")

        logger.info("Running: %s", " ".join(cmd))

        run_env = os.environ.copy()

        result = subprocess.run(
            cmd, text=True, env=run_env, cwd=self.project_dir
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"dbt {self.command} failed (exit {result.returncode})"
            )

        logger.info("dbt %s completed successfully", self.command)
        return result.returncode
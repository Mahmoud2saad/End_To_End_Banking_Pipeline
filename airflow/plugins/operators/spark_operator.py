# plugins/operators/spark_operator.py
import os
import sys
import subprocess
import logging
from typing import Optional

from airflow.models import BaseOperator

logger = logging.getLogger(__name__)


class SparkSubmitLocalOperator(BaseOperator):
    """
    Runs a PySpark script as subprocess in local mode.
    Equivalent to: python <script_path> [args...]
    """
    template_fields = ("script_path", "script_args")
    ui_color = "#4B8BBE"

    def __init__(
        self,
        script_path: str,
        script_args: Optional[list] = None,
        python_executable: Optional[str] = None,
        env: Optional[dict] = None,
        working_dir: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.script_path       = script_path
        self.script_args       = script_args or []
        self.python_executable = python_executable or sys.executable
        self.env               = env or {}
        self.working_dir       = working_dir

    def execute(self, context):
        cmd = [self.python_executable, self.script_path] + [
            str(a) for a in self.script_args
        ]
        logger.info("Running: %s", " ".join(cmd))
        run_env = os.environ.copy()
        run_env.update(self.env)
        result = subprocess.run(
            cmd, text=True, env=run_env, cwd=self.working_dir
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Script failed (exit {result.returncode}): {self.script_path}"
            )
        logger.info("Script completed: %s", self.script_path)
        return result.returncode
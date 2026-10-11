"""Machine-readable submission plans kept separate from terminal progress output."""

import json
import os
from pathlib import Path

import click


def emit_container_submission_plan(app_name: str, plan: object) -> None:
    """Write a TTY submission plan to its exchange file, or otherwise to stdout."""
    contents = json.dumps(plan)
    path = os.environ.get(f"{app_name.upper()}_CONTAINER_PLAN_FILE")
    if path:
        try:
            Path(path).write_text(contents, encoding="utf-8", newline="\n")
        except OSError as exc:
            raise click.ClickException(
                f"Cannot write container submission plan: {exc}"
            ) from exc
    else:
        click.echo(contents)

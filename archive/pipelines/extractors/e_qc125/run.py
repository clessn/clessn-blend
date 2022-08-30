import os
import subprocess
from celery import Celery

app = Celery()

app.config_from_object("celeryconfig")


def execute(command, *args, **kwargs):
    # configure environment
    env = os.environ.copy()
    path = kwargs.get("PATH", None)
    kwargs.pop("PATH", None)
    if path is not None:
        env["PATH"] = path + ":" + env["PATH"]
    for key, value in kwargs.items():
        env[key] = value

    # run subprocess
    result = subprocess.call(
        command,
        env=env,
        shell=True,
        universal_newlines=True,
    )

    if result != 0:
        raise Exception("command failed")


# Task definitions
@app.task
# TODO: change task1 to a better fitting name if necessary
def task1(*args, **kwargs):
    # TODO: adjust command if necessary
    command = "Rscript code/code.R"
    execute(command, *args, **kwargs)

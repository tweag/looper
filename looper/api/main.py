from argparse import ArgumentParser, Namespace
import secrets
import io
from typing import Dict, TypeAlias

import fastapi
import pydantic
import uvicorn

from fastapi import FastAPI

from looper.cli_pydantic import run_looper
from looper.command_models.commands import SUPPORTED_COMMANDS, TopLevelParser

from looper.api import stdout_redirects

stdout_redirects.enable_proxy()

JobId: TypeAlias = str


class Job(pydantic.BaseModel):
    id: JobId = pydantic.Field(
        default_factory=lambda: secrets.token_urlsafe(4),
        description="The unique identifier of the job",
    )
    status: str = pydantic.Field(
        default="in_progress",
        description="The current status of the job. Can be either `in_progress` or `completed`.",
    )
    console_output: str | None = pydantic.Field(
        default=None,
        description="Console output produced by `looper` while performing the requested action",
    )


app = FastAPI(validate_model=True)
jobs: Dict[str, Job] = {}


def background_async(top_level_model: TopLevelParser, job_id: JobId) -> None:
    argparse_namespace = create_argparse_namespace(top_level_model)
    output_stream = stdout_redirects.redirect()

    run_looper(argparse_namespace, None, True)

    # Here, we should call `stdout_redirects.stop_redirect()`, but that fails for reasons discussed
    # in the following issue: https://github.com/python/cpython/issues/80374
    # But this *seems* not to pose any problems.
    jobs[job_id].status = "completed"
    jobs[job_id].console_output = output_stream.getvalue()


def create_argparse_namespace(top_level_model: TopLevelParser) -> Namespace:
    """
    Converts a TopLevelParser instance into an argparse.Namespace object.

    This function takes a TopLevelParser instance, and converts it into an
    argparse.Namespace object. It includes handling for supported commands
    specified in SUPPORTED_COMMANDS.

    :param TopLevelParser top_level_model: An instance of the TopLevelParser
        model
    :return argparse.Namespace: An argparse.Namespace object representing
        the parsed command-line arguments.
    """
    namespace = Namespace()

    for argname, value in vars(top_level_model).items():
        if argname not in [cmd.name for cmd in SUPPORTED_COMMANDS]:
            setattr(namespace, argname, value)
        else:
            command_namespace = Namespace()
            command_namespace_args = value
            for command_argname, command_arg_value in vars(
                command_namespace_args
            ).items():
                setattr(
                    command_namespace,
                    command_argname,
                    command_arg_value,
                )
            setattr(namespace, argname, command_namespace)
    return namespace


@app.post(
    "/",
    status_code=202,
    summary="Run Looper in Background",
    description="Create a new job, process data with the specified "
    "`top_level_model`, and initiate a background asynchronous task to run "
    "looper.",
)
async def main_endpoint(
    top_level_model: TopLevelParser, background_tasks: fastapi.BackgroundTasks
) -> Dict:
    job = Job()
    jobs[job.id] = job
    background_tasks.add_task(background_async, top_level_model, job.id)
    return {"job_id": job.id}


@app.get(
    "/status/{job_id}",
    summary="Get Job Status",
    description="Retrieve the status of a job based on its unique identifier.",
)
async def get_status(job_id: JobId) -> Job:
    return jobs[job_id]


def main() -> None:
    parser = ArgumentParser("looper-serve", description="Run looper HTTP API server")
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host IP address to use (127.0.0.1 for local access only)",
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="Port the server listens on"
    )
    args = parser.parse_args()

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()

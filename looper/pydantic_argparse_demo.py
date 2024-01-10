import argparse
import os
import sys
from typing import Optional

import pydantic
import pydantic_argparse
import yaml
from pephubclient import PEPHubClient

from divvy import select_divvy_config

from . import __version__
from .const import *
from .divvy import DEFAULT_COMPUTE_RESOURCES_NAME, select_divvy_config
from .exceptions import *
from .looper import *
from .parser_types import *
from .project import Project, ProjectContext
from .utils import (
    dotfile_path,
    enrich_args_via_cfg,
    is_registry_path,
    read_looper_dotfile,
    read_yaml_file,
)


## pydantic model for `looper run` command
class RunParser(pydantic.BaseModel):
    # arguments
    looper_config: str = pydantic.Field(description="Looper project configuration file")


## pydantic model for `looper run` command
class CheckParser(pydantic.BaseModel):
    # arguments
    looper_config: str = pydantic.Field(description="Looper project configuration file")


## pydantic model for base command
class TopLevelParser(pydantic.BaseModel):
    # Commands
    run: Optional[RunParser] = pydantic.Field(description="Run a looper project")
    check: Optional[CheckParser] = pydantic.Field(description="Check a looper project")
    ## these are required to make progress with the `hello_looper` example, but probably shouldn't be here
    config_file: Optional[str] = pydantic.Field(
        description="Project configuration file"
    )
    pep_config: Optional[str] = pydantic.Field(description="PEP configuration file")
    output_dir: Optional[str] = pydantic.Field(description="Output directory")
    sample_pipeline_interfaces: Optional[str] = pydantic.Field(
        description="Sample pipeline interfaces definition"
    )
    project_pipeline_interfaces: Optional[str] = pydantic.Field(
        description="Project pipeline interfaces definition"
    )
    amend: Optional[bool] = pydantic.Field(description="Amend stuff?")
    sel_flag: Optional[bool] = pydantic.Field(description="Selection flag")
    exc_flag: Optional[bool] = pydantic.Field(description="Exclusion flag")

    time_delay: Optional[int] = pydantic.Field(
        0, description="Time delay in seconds between job submissions"
    )
    command_extra: Optional[str] = pydantic.Field(
        description="String to append to every command"
    )
    command_extra_override: Optional[str] = pydantic.Field(
        description="Same as command-extra, but overrides values in PEP"
    )
    ignore_flags: Optional[bool] = pydantic.Field(
        description="Ignore run status flags? Default=False"
    )
    lumpn: Optional[int] = pydantic.Field(
        description="Number of commands to batch into one job"
    )
    lump: Optional[int] = pydantic.Field(
        description="Total input file size (GB) to batch into one job"
    )
    limit: Optional[int] = pydantic.Field(description="Limit to n samples")
    skip: Optional[int] = pydantic.Field(description="Skip samples by numerical index")
    divvy: Optional[str] = pydantic.Field(
        None,
        description="Path to divvy configuration file. Default=$DIVCFG env "
        "variable. Currently: {}".format(os.getenv("DIVCFG", None) or "not set"),
    )
    dry_run: Optional[bool] = pydantic.Field(
        False, description="Don't actually submit the jobs.  Default=False"
    )


def _proc_resources_spec(args):
    """
    Process CLI-sources compute setting specification. There are two sources
    of compute settings in the CLI alone:
        * YAML file (--settings argument)
        * itemized compute settings (--compute argument)

    The itemized compute specification is given priority

    :param argparse.Namespace: arguments namespace
    :return Mapping[str, str]: binding between resource setting name and value
    :raise ValueError: if interpretation of the given specification as encoding
        of key-value pairs fails
    """
    spec = getattr(args, "compute", None)
    try:
        settings_data = (
            read_yaml_file(args.settings) if hasattr(args, "settings") else {}
        )
    except yaml.YAMLError:
        _LOGGER.warning(
            "Settings file ({}) does not follow YAML format,"
            " disregarding".format(args.settings)
        )
        settings_data = {}
    if not spec:
        return settings_data
    pairs = [(kv, kv.split("=")) for kv in spec]
    bads = []
    for orig, pair in pairs:
        try:
            k, v = pair
        except ValueError:
            bads.append(orig)
        else:
            settings_data[k] = v
    if bads:
        raise ValueError(
            "Could not correctly parse itemized compute specification. "
            "Correct format: " + EXAMPLE_COMPUTE_SPEC_FMT
        )
    return settings_data


def main() -> None:
    parser = pydantic_argparse.ArgumentParser(
        model=TopLevelParser,
        prog="looper",
        description="pydantic-argparse demo",
        add_help=True,
    )
    args = parser.parse_typed_args()
    print(args)

    # here comes adapted `cli_looper.py` code
    looper_cfg_path = os.path.relpath(dotfile_path(), start=os.curdir)
    try:
        looper_config_dict = read_looper_dotfile()

        for looper_config_key, looper_config_item in looper_config_dict.items():
            setattr(args, looper_config_key, looper_config_item)

    except OSError:
        parser.print_help(sys.stderr)
        raise ValueError(
            f"Looper config file does not exist. Use looper init to create one at {looper_cfg_path}."
        )

    args = enrich_args_via_cfg(args, parser, False)
    divcfg = (
        select_divvy_config(filepath=args.divvy) if hasattr(args, "divvy") else None
    )
    # Ignore flags if user is selecting or excluding on flags:
    if args.sel_flag or args.exc_flag:
        args.ignore_flags = True

    # Initialize project
    if is_registry_path(args.config_file):
        if vars(args)[SAMPLE_PL_ARG]:
            p = Project(
                amendments=args.amend,
                divcfg_path=divcfg,
                runp=args.command == "runp",
                project_dict=PEPHubClient()._load_raw_pep(
                    registry_path=args.config_file
                ),
                **{
                    attr: getattr(args, attr) for attr in CLI_PROJ_ATTRS if attr in args
                },
            )
        else:
            raise MisconfigurationException(
                f"`sample_pipeline_interface` is missing. Provide it in the parameters."
            )
    else:
        try:
            p = Project(
                cfg=args.config_file,
                amendments=args.amend,
                divcfg_path=divcfg,
                runp=False,
                **{
                    attr: getattr(args, attr) for attr in CLI_PROJ_ATTRS if attr in args
                },
            )
        except yaml.parser.ParserError as e:
            _LOGGER.error(f"Project config parse failed -- {e}")
            sys.exit(1)

    selected_compute_pkg = p.selected_compute_package or DEFAULT_COMPUTE_RESOURCES_NAME
    if p.dcc is not None and not p.dcc.activate_package(selected_compute_pkg):
        _LOGGER.info(
            "Failed to activate '{}' computing package. "
            "Using the default one".format(selected_compute_pkg)
        )

    with ProjectContext(
        prj=p,
        selector_attribute="toggle",
        selector_include=None,
        selector_exclude=None,
        selector_flag=None,
        exclusion_flag=None,
    ) as prj:
        command = "run"
        if command == "run":
            run = Runner(prj)
            try:
                compute_kwargs = _proc_resources_spec(args)
                return run(args, rerun=False, **compute_kwargs)
            except SampleFailedException:
                sys.exit(1)
            except IOError:
                _LOGGER.error(
                    "{} pipeline_interfaces: '{}'".format(
                        prj.__class__.__name__, prj.pipeline_interface_sources
                    )
                )
                raise


if __name__ == "__main__":
    main()

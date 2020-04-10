#!/usr/bin/env python
"""
Looper: a pipeline submission engine. https://github.com/pepkit/looper
"""

import abc
import csv
import glob
import logging
import os
import subprocess
import sys
if sys.version_info < (3, 3):
    from collections import Mapping
else:
    from collections.abc import Mapping
import yaml
import pandas as _pd

from collections import defaultdict
# Need specific sequence of actions for colorama imports?
from colorama import init
init()
from colorama import Fore, Style
from shutil import rmtree

from . import __version__, build_parser, _LEVEL_BY_VERBOSITY
from .conductor import SubmissionConductor
from .const import *
from .exceptions import JobSubmissionException, MisconfigurationException
from .html_reports import HTMLReportBuilder
from .project import Project, ProjectContext
from .utils import determine_config_path, fetch_flag_files, sample_folder, \
    get_file_for_project
from .looper_config import *

from divvy import DEFAULT_COMPUTE_RESOURCES_NAME, \
    NEW_COMPUTE_KEY as DIVVY_COMPUTE_KEY
from logmuse import init_logger
from peppy.const import *
from eido import validate_sample, validate_config
from ubiquerg import query_yes_no


_PKGNAME = "looper"
_LOGGER = logging.getLogger(_PKGNAME)


class Executor(object):
    """ Base class that ensures the program's Sample counter starts.

    Looper is made up of a series of child classes that each extend the base
    Executor class. Each child class does a particular task (such as run the
    project, summarize the project, destroy the project, etc). The parent
    Executor class simply holds the code that is common to all child classes,
    such as counting samples as the class does its thing."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, prj):
        """
        The Project defines the instance; establish an iteration counter.

        :param Project prj: Project with which to work/operate on
        """
        super(Executor, self).__init__()
        self.prj = prj
        self.counter = LooperCounter(len(prj.samples))

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        """ Do the work of the subcommand/program. """
        pass


class Checker(Executor):

    def __call__(self, flags=None, all_folders=False, max_file_count=30):
        """
        Check Project status, based on flag files.

        :param Iterable[str] | str flags: Names of flags to check, optional;
            if unspecified, all known flags will be checked.
        :param bool all_folders: Whether to check flags in all folders, not
            just those for samples in the config file from which the Project
            was created.
        :param int max_file_count: Maximum number of filepaths to display for a
            given flag.
        """

        # Handle single or multiple flags, and alphabetize.
        flags = sorted([flags] if isinstance(flags, str)
                       else list(flags or FLAGS))
        flag_text = ", ".join(flags)

        # Collect the files by flag and sort by flag name.
        _LOGGER.debug("Checking project folders for flags: %s", flag_text)
        if all_folders:
            files_by_flag = fetch_flag_files(
                results_folder=self.prj.results_folder, flags=flags)
        else:
            files_by_flag = fetch_flag_files(prj=self.prj, flags=flags)

        # For each flag, output occurrence count.
        for flag in flags:
            _LOGGER.info("%s: %d", flag.upper(), len(files_by_flag[flag]))

        # For each flag, output filepath(s) if not overly verbose.
        for flag in flags:
            try:
                files = files_by_flag[flag]
            except Exception as e:
                _LOGGER.debug("No files for {} flag. Caught exception: {}".
                              format(flags, getattr(e, 'message', repr(e))))
                continue
            # If checking on a specific flag, do not limit the number of
            # reported filepaths, but do not report empty file lists
            if len(flags) == 1 and len(files) > 0:
                _LOGGER.info("%s (%d):\n%s", flag.upper(),
                             len(files), "\n".join(files))
            # Regardless of whether 0-count flags are previously reported,
            # don't report an empty file list for a flag that's absent.
            # If the flag-to-files mapping is defaultdict, absent flag (key)
            # will fetch an empty collection, so check for length of 0.
            if 0 < len(files) <= max_file_count:
                _LOGGER.info("%s (%d):\n%s", flag.upper(),
                             len(files), "\n".join(files))


class Cleaner(Executor):
    """ Remove all intermediate files (defined by pypiper clean scripts). """

    def __call__(self, args, preview_flag=True):
        """
        Execute the file cleaning process.

        :param argparse.Namespace args: command-line options and arguments
        :param bool preview_flag: whether to halt before actually removing files
        """
        _LOGGER.info("Files to clean:")

        for sample in self.prj.samples:
            _LOGGER.info(self.counter.show(sample.sample_name))
            sample_output_folder = sample_folder(self.prj, sample)
            cleanup_files = glob.glob(os.path.join(sample_output_folder,
                                                   "*_cleanup.sh"))
            if preview_flag:
                # Preview: Don't actually clean, just show what will be cleaned.
                _LOGGER.info("Files to clean: %s", ", ".join(cleanup_files))
            else:
                for f in cleanup_files:
                    _LOGGER.info(f)
                    subprocess.call(["sh", f])

        if not preview_flag:
            _LOGGER.info("Clean complete.")
            return 0

        if args.dry_run:
            _LOGGER.info("Dry run. No files cleaned.")
            return 0

        if not args.force_yes and not \
                query_yes_no("Are you sure you want to permanently delete all "
                             "intermediate pipeline results for this project?"):
            _LOGGER.info("Clean action aborted by user.")
            return 1

        self.counter.reset()

        return self(args, preview_flag=False)


class Destroyer(Executor):
    """ Destroyer of files and folders associated with Project's Samples """

    def __call__(self, args, preview_flag=True):
        """
        Completely remove all output produced by any pipelines.

        :param argparse.Namespace args: command-line options and arguments
        :param bool preview_flag: whether to halt before actually removing files
        """

        _LOGGER.info("Removing results:")
        for sample in self.prj.samples:
            _LOGGER.info(self.counter.show(sample.sample_name))
            sample_output_folder = sample_folder(self.prj, sample)
            if preview_flag:
                # Preview: Don't actually delete, just show files.
                _LOGGER.info(str(sample_output_folder))
            else:
                _remove_or_dry_run(sample_output_folder, args.dry_run)

        _LOGGER.info("Removing summary:")
        destroy_summary(self.prj, args.dry_run)

        if not preview_flag:
            _LOGGER.info("Destroy complete.")
            return 0

        if args.dry_run:
            _LOGGER.info("Dry run. No files destroyed.")
            return 0

        if not args.force_yes and not query_yes_no(
                "Are you sure you want to permanently delete all pipeline "
                "results for this project?"):
            _LOGGER.info("Destroy action aborted by user.")
            return 1

        self.counter.reset()

        # Finally, run the true destroy:
        return self(args, preview_flag=False)


class Collator(Executor):
    """" Submitter for project-level pipelines """
    def __init__(self, prj):
        """
        Initializes an instance

        :param Project prj: Project with which to work/operate on
        """
        super(Executor, self).__init__()
        self.prj = prj
        if not self.prj.pipeline_interfaces:
            raise MisconfigurationException(
                "Looper requires at least one pointer to project-level in a "
                "pipeline interface file, set with the '{p}' key in the project"
                " config file and define {c} in '{c}' section in that file".
                    format(p=PIPELINE_INTERFACES_KEY, c=PROJECT_PL_KEY)
            )

    def __call__(self, args, remaining_args, **compute_kwargs):
        """
        Matches collators by protocols, creates submission scripts
        and submits them

        :param argparse.Namespace args: parsed command-line options and
            arguments, recognized by looper
        :param list remaining_args: command-line options and arguments not
            recognized by looper, germane to samples/pipelines
        """
        jobs = 0
        project_pifaces = self.prj._samples_by_interface.keys()
        _LOGGER.debug("Matched {} project pipeline interfaces"
                     .format(len(project_pifaces)))
        if PIPELINE_INTERFACES_KEY in self.prj[CONFIG_KEY][LOOPER_KEY]:
            # pipeline interfaces sources defined in the project config override
            # ones that are matched by the Samples in this Project
            project_pifaces = \
                [expandpath(src) for src in
                 self.prj[CONFIG_KEY][LOOPER_KEY][PIPELINE_INTERFACES_KEY]]
        if not project_pifaces:
            _LOGGER.warning("No pipelines found to run for this project")
            sys.exit(0)
        self.prj.populate_pipeline_outputs()
        self.counter = LooperCounter(len(project_pifaces))
        for project_piface in project_pifaces:
            try:
                project_piface_object = PipelineInterface(project_piface)
            except FileNotFoundError:
                _LOGGER.warning("Pipeline interface does not exist: {}".
                             format(project_piface))
                continue
            _LOGGER.info(self.counter.show(
                name=self.prj.name, type="project",
                pipeline_name=project_piface_object.pipeline_name))
            conductor = SubmissionConductor(
                pipeline_interface=project_piface_object,
                prj=self.prj,
                compute_variables=compute_kwargs,
                dry_run=args.dry_run,
                delay=args.time_delay,
                extra_args=remaining_args,
                ignore_flags=args.ignore_flags,
                collate=True
            )
            conductor._pool = [None]
            conductor.submit()
            jobs += conductor.num_job_submissions
        _LOGGER.info("\nLooper finished")
        _LOGGER.info("Jobs submitted: {}".format(jobs))


class Runner(Executor):
    """ The true submitter of pipelines """

    def __call__(self, args, remaining_args, rerun=False, **compute_kwargs):
        """
        Do the Sample submission.

        :param argparse.Namespace args: parsed command-line options and
            arguments, recognized by looper
        :param list remaining_args: command-line options and arguments not
            recognized by looper, germane to samples/pipelines
        :param bool rerun: whether the given sample is being rerun rather than
            run for the first time
        """
        max_cmds = sum(list(map(len, self.prj._samples_by_interface.values())))
        self.counter.total = max_cmds
        failures = defaultdict(list)  # Collect problems by sample.
        processed_samples = set()  # Enforce one-time processing.
        submission_conductors = {}
        try:
            comp_vars = self.prj.dcc[COMPUTE_KEY].to_map()
        except AttributeError:
            if not isinstance(self.prj.dcc[COMPUTE_KEY], Mapping):
                raise TypeError("Project's computing config isn't a mapping: {}"
                                " ({})".format(self.prj.dcc[COMPUTE_KEY],
                                               type(self.prj.dcc[COMPUTE_KEY])))
            from copy import deepcopy
            comp_vars = deepcopy(self.prj.dcc[COMPUTE_KEY])
        comp_vars.update(compute_kwargs or {})

        # Determine number of samples eligible for processing.
        num_samples = len(self.prj.samples)
        if args.limit is None:
            upper_sample_bound = num_samples
        elif args.limit < 0:
            raise ValueError("Invalid number of samples to run: {}".
                             format(args.limit))
        else:
            upper_sample_bound = min(args.limit, num_samples)
        _LOGGER.debug("Limiting to {} of {} samples".
                      format(upper_sample_bound, num_samples))

        num_commands_possible = 0
        failed_submission_scripts = []

        # config validation (samples excluded) against all schemas defined
        # for every pipeline matched for this project
        [validate_config(self.prj, schema_file)
         for schema_file in self.prj.get_schemas(self.prj.pipeline_interfaces)]

        for piface in self.prj.pipeline_interfaces:
            conductor = SubmissionConductor(
                pipeline_interface=piface,
                prj=self.prj,
                compute_variables=comp_vars,
                dry_run=args.dry_run,
                delay=args.time_delay,
                extra_args=remaining_args,
                ignore_flags=args.ignore_flags,
                max_cmds=args.lumpn,
                max_size=args.lump
            )
            submission_conductors[piface.pipe_iface_file] = conductor

        for sample in self.prj.samples[:upper_sample_bound]:
            pl_fails = []
            skip_reasons = []
            sample_pifaces = self.prj.get_sample_piface(sample[SAMPLE_NAME_ATTR])
            if not sample_pifaces:
                skip_reasons.append("No pipeline interfaces defined")

            if skip_reasons:
                _LOGGER.warning(NOT_SUB_MSG.format(", ".join(skip_reasons)))
                failures[sample.sample_name] = skip_reasons
                continue

            # single sample validation against a single schema
            # (from sample's piface)
            [validate_sample(self.prj, sample.sample_name, schema_file)
             for schema_file in self.prj.get_schemas(sample_pifaces)]

            processed_samples.add(sample[SAMPLE_NAME_ATTR])

            for sample_piface in sample_pifaces:
                _LOGGER.info(
                    self.counter.show(name=sample.sample_name,
                                      pipeline_name=sample_piface.pipeline_name)
                )
                num_commands_possible += 1
                cndtr = submission_conductors[sample_piface.pipe_iface_file]
                try:
                    curr_pl_fails = cndtr.add_sample(sample, rerun=rerun)
                except JobSubmissionException as e:
                    failed_submission_scripts.append(e.script)
                else:
                    pl_fails.extend(curr_pl_fails)
            if pl_fails:
                failures[sample.sample_name].extend(pl_fails)

        job_sub_total = 0
        cmd_sub_total = 0

        for piface, conductor in submission_conductors.items():
            conductor.submit(force=True)
            job_sub_total += conductor.num_job_submissions
            cmd_sub_total += conductor.num_cmd_submissions
            conductor.write_skipped_sample_scripts()

        # Report what went down.
        # max_cmds = min(len(self.prj.samples), args.limit or float("inf"))
        _LOGGER.info("\nLooper finished")
        _LOGGER.info("Samples valid for job generation: {} of {}".
                     format(len(processed_samples), num_samples))
        _LOGGER.info("Successful samples: {} of {}".
                     format(num_samples - len(failures), num_samples))
        _LOGGER.info("Commands submitted: {} of {}".
                     format(cmd_sub_total, max_cmds))
        _LOGGER.info("Jobs submitted: {}".format(job_sub_total))
        if args.dry_run:
            _LOGGER.info("Dry run. No jobs were actually submitted.")

        # Restructure sample/failure data for display.
        samples_by_reason = defaultdict(set)
        # Collect names of failed sample(s) by failure reason.
        for sample, failures in failures.items():
            for f in failures:
                samples_by_reason[f].add(sample)
        # Collect samples by pipeline with submission failure.
        for piface, conductor in submission_conductors.items():
            # Don't add failure key if there are no samples that failed for
            # that reason.
            if conductor.failed_samples:
                fails = set(conductor.failed_samples)
                samples_by_reason[SUBMISSION_FAILURE_MESSAGE] |= fails

        failed_sub_samples = samples_by_reason.get(SUBMISSION_FAILURE_MESSAGE)
        if failed_sub_samples:
            _LOGGER.info("\n{} samples with at least one failed job submission:"
                         " {}".format(len(failed_sub_samples),
                                      ", ".join(failed_sub_samples)))

        # If failure keys are only added when there's at least one sample that
        # failed for that reason, we can display information conditionally,
        # depending on whether there's actually failure(s).
        if samples_by_reason:
            _LOGGER.info("\n{} unique reasons for submission failure: {}".format(
                len(samples_by_reason), ", ".join(samples_by_reason.keys())))
            full_fail_msgs = [_create_failure_message(reason, samples)
                              for reason, samples in samples_by_reason.items()]
            _LOGGER.info("\nSummary of failures:\n{}".
                         format("\n".join(full_fail_msgs)))


class Summarizer(Executor):
    """ Project/Sample output summarizer """
    def __init__(self, prj):
        # call the inherited initialization
        super(Summarizer, self).__init__(prj)
        # pull together all the fits and stats from each sample into
        # project-combined spreadsheets.
        self.stats, self.columns = _create_stats_summary(self.prj, self.counter)
        self.objs = _create_obj_summary(self.prj, self.counter)

    def __call__(self):
        """ Do the summarization. """
        # initialize the report builder
        report_builder = HTMLReportBuilder(self.prj)
        # run the report builder. a set of HTML pages is produced
        report_path = report_builder(self.objs, self.stats,
                                     uniqify(self.columns))
        _LOGGER.info("HTML Report (n=" + str(len(self.stats)) + "): "
                     + report_path)


def _create_stats_summary(project, counter):
    """
    Create stats spreadsheet and columns to be considered in the report, save
    the spreadsheet to file

    :param looper.Project project: the project to be summarized
    :param looper.LooperCounter counter: a counter object
    """
    # Create stats_summary file
    columns = []
    stats = []
    project_samples = project.samples
    missing_files = []
    _LOGGER.info("Creating stats summary...")
    for sample in project_samples:
        _LOGGER.info(counter.show(sample.sample_name, sample.protocol))
        sample_output_folder = sample_folder(project, sample)
        # Grab the basic info from the annotation sheet for this sample.
        # This will correspond to a row in the output.
        sample_stats = sample.get_sheet_dict()
        columns.extend(sample_stats.keys())
        # Version 0.3 standardized all stats into a single file
        stats_file = os.path.join(sample_output_folder, "stats.tsv")
        if not os.path.isfile(stats_file):
            missing_files.append(stats_file)
            continue
        t = _pd.read_csv(stats_file, sep="\t", header=None,
                         names=['key', 'value', 'pl'])
        t.drop_duplicates(subset=['key', 'pl'], keep='last', inplace=True)
        t.loc[:, 'plkey'] = t['pl'] + ":" + t['key']
        dupes = t.duplicated(subset=['key'], keep=False)
        t.loc[dupes, 'key'] = t.loc[dupes, 'plkey']
        sample_stats.update(t.set_index('key')['value'].to_dict())
        stats.append(sample_stats)
        columns.extend(t.key.tolist())
    tsv_outfile_path = get_file_for_project(project, 'stats_summary.tsv')
    if missing_files:
        _LOGGER.warning("Stats files missing for {} samples: {}".
                        format(len(missing_files),missing_files))
    tsv_outfile = open(tsv_outfile_path, 'w')
    tsv_writer = csv.DictWriter(tsv_outfile, fieldnames=uniqify(columns),
                                delimiter='\t', extrasaction='ignore')
    tsv_writer.writeheader()
    for row in stats:
        tsv_writer.writerow(row)
    tsv_outfile.close()
    _LOGGER.info("Summary (n=" + str(len(stats)) + "): " + tsv_outfile_path)
    counter.reset()
    return stats, uniqify(columns)


def _create_obj_summary(project, counter):
    """
    Read sample specific objects files and save to a data frame

    :param looper.Project project: the project to be summarized
    :param looper.LooperCounter counter: a counter object
    :return pandas.DataFrame: objects spreadsheet
    """
    _LOGGER.info("Creating objects summary...")
    objs = _pd.DataFrame()
    # Create objects summary file
    missing_files = []
    for sample in project.samples:
        # Process any reported objects
        _LOGGER.info(counter.show(sample.sample_name, sample.protocol))
        sample_output_folder = sample_folder(project, sample)
        objs_file = os.path.join(sample_output_folder, "objects.tsv")
        if not os.path.isfile(objs_file):
            missing_files.append(objs_file)
            continue
        t = _pd.read_csv(objs_file, sep="\t", header=None,
                         names=['key', 'filename', 'anchor_text',
                                'anchor_image', 'annotation'])
        t['sample_name'] = sample.sample_name
        objs = objs.append(t, ignore_index=True)
    if missing_files:
        _LOGGER.warning("Object files missing for {} samples: {}".
                        format(len(missing_files), missing_files))
    # create the path to save the objects file in
    objs.to_csv(get_file_for_project(project, 'objs_summary.tsv'), sep="\t")
    return objs


def _create_failure_message(reason, samples):
    """ Explain lack of submission for a single reason, 1 or more samples. """
    color = Fore.LIGHTRED_EX
    reason_text = color + reason + Style.RESET_ALL
    samples_text = ", ".join(samples)
    return "{}: {}".format(reason_text, samples_text)


def _remove_or_dry_run(paths, dry_run=False):
    """
    Remove file or directory or just inform what would be removed in
    case of dry run

    :param list|str paths: list of paths to files/dirs to be removed
    :param bool dry_run: logical indicating whether the files should remain
        untouched and massage printed
    """
    paths = paths if isinstance(paths, list) else [paths]
    for path in paths:
        if os.path.exists(path):
            if dry_run:
                _LOGGER.info("DRY RUN. I would have removed: " + path)
            else:
                _LOGGER.info("Removing: " + path)
                if os.path.isfile(path):
                    os.remove(path)
                else:
                    rmtree(path)
        else:
            _LOGGER.info(path + " does not exist.")


def destroy_summary(prj, dry_run=False):
    """
    Delete the summary files if not in dry run mode
    """
    _remove_or_dry_run([get_file_for_project(prj, "summary.html"),
                        get_file_for_project(prj, 'stats_summary.tsv'),
                        get_file_for_project(prj, 'objs_summary.tsv'),
                        get_file_for_project(prj, "reports")], dry_run)


def uniqify(seq):
    """
    Fast way to uniqify while preserving input order.
    """
    # http://stackoverflow.com/questions/480214/
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def parse_user_input(x):
    """
    Inputs overriding config-defined options are list of lists of strings.
    To make the manipulation more convenient we need to make them
    lists of strings

    :param Iterable[Iterable[str], ...] | None x: user-provided argument
    :return Iterable[str] | None: flattened user input or None if not provided
    """
    if x is None:
        return
    flat_input = []
    for i in x:
        flat_input.extend(i)
    return flat_input


class LooperCounter(object):
    """
    Count samples as you loop through them, and create text for the
    subcommand logging status messages.

    :param int total: number of jobs to process
    """

    def __init__(self, total):
        self.count = 0
        self.total = total

    def show(self, name, type="sample", pipeline_name=None):
        """
        Display sample counts status for a particular protocol type.

        The counts are running vs. total for the protocol within the Project,
        and as a side-effect of the call, the running count is incremented.

        :param str name: name of the sample
        :param str pipeline_name: name of the pipeline
        :return str: message suitable for logging a status update
        """
        self.count += 1
        return _submission_status_text(type=type,
            curr=self.count, total=self.total, name=name,
            pipeline_name=pipeline_name, color=Fore.CYAN
        )

    def reset(self):
        self.count = 0

    def __str__(self):
        return "LooperCounter of size {}".format(self.total)


def _submission_status_text(curr, total, name, pipeline_name=None,
                            type="sample", color=Fore.CYAN):
    """ Generate submission sample text for run or collate """
    txt = color + "## [{n} of {t}] {type}: {name}".\
        format(n=curr, t=total, type=type, name=name)
    if pipeline_name:
        txt += "; pipeline: {}".format(pipeline_name)
    return txt + Style.RESET_ALL


def _proc_resources_spec(spec):
    """
    Process CLI-specified itemized compute setting specification.

    :param str | NoneType spec: itemized compute specification from CLI
    :return Mapping[str, str]: binding between resource setting name and value
    :raise ValueError: if interpretation of the given specification as encoding
        of key-value pairs fails
    """
    if not spec:
        return {}
    kvs = spec.strip().split(",")
    pairs = [(kv, kv.split("=")) for kv in kvs]
    bads, data = [], {}
    for orig, pair in pairs:
        try:
            k, v = pair
        except ValueError:
            bads.append(orig)
        else:
            data[k] = v
    if bads:
        raise ValueError(
            "Could not correctly parse itemized compute specification. "
            "Correct format: " + EXAMPLE_COMPUTE_SPEC_FMT)
    return data


def main():
    """ Primary workflow """
    global _LOGGER

    parser = build_parser()
    args, remaining_args = parser.parse_known_args()

    try:
        conf_file = args.config_file
    except AttributeError:
        parser.print_help(sys.stderr)
        sys.exit(1)

    # Set the logging level.
    if args.dbg:
        # Debug mode takes precedence and will listen for all messages.
        level = args.logging_level or logging.DEBUG
    elif args.verbosity is not None:
        # Verbosity-framed specification trumps logging_level.
        level = _LEVEL_BY_VERBOSITY[args.verbosity]
    else:
        # Normally, we're not in debug mode, and there's not verbosity.
        level = LOGGING_LEVEL

    # Establish the project-root logger and attach one for this module.
    logger_kwargs = {"level": level,
                     "logfile": args.logfile,
                     "devmode": args.dbg}
    init_logger(name="peppy", **logger_kwargs)
    init_logger(name="divvy", **logger_kwargs)
    init_logger(name="eido", **logger_kwargs)
    _LOGGER = init_logger(name=_PKGNAME, **logger_kwargs)

    if len(remaining_args) > 0:
        _LOGGER.debug("Remaining arguments passed to pipelines: {}".
                      format(" ".join([str(x) for x in remaining_args])))

    # lc = LooperConfig(select_looper_config(filename=args.looper_config))
    # _LOGGER.debug("Determined genome config: {}".format(lc))

    _LOGGER.info("Looper version: {}\nCommand: {}".
                 format(__version__, args.command))

    # Initialize project
    _LOGGER.debug("Building Project")
    try:
        prj = Project(config_file=determine_config_path(conf_file),
                      amendments=args.amendments,
                      pifaces=args.pifaces[0] if args.pifaces else None,
                      file_checks=args.file_checks,
                      compute_env_file=getattr(args, 'env', None))
    except yaml.parser.ParserError as e:
        _LOGGER.error("Project config parse failed -- {}".format(e))
        sys.exit(1)
    _LOGGER.debug("Results subdir: " + prj.results_folder)

    selected_cli_compute_pkg = getattr(args, DIVVY_COMPUTE_KEY, None)
    # compute package selection priority list: cli > project > default
    selected_compute_pkg = DEFAULT_COMPUTE_RESOURCES_NAME
    if COMPUTE_PACKAGE_KEY in prj[CONFIG_KEY][LOOPER_KEY]:
        selected_compute_pkg = prj[CONFIG_KEY][LOOPER_KEY][COMPUTE_PACKAGE_KEY]
    if selected_cli_compute_pkg:
        selected_compute_pkg = selected_cli_compute_pkg
    prj.dcc.activate_package(selected_compute_pkg)

    with ProjectContext(prj=prj,
                        selector_attribute=args.selector_attribute,
                        selector_include=args.selector_include,
                        selector_exclude=args.selector_exclude) as prj:

        if args.command in ["run", "rerun"]:
            run = Runner(prj)
            try:
                compute_kwargs = _proc_resources_spec(
                    getattr(args, "compute", ""))
                run(args, remaining_args, rerun=(args.command == "rerun"),
                    **compute_kwargs)
            except IOError:
                _LOGGER.error("{} pipeline_interfaces: '{}'".
                              format(prj.__class__.__name__,
                                     prj.pipeline_interface_sources))
                raise

        if args.command == "runp":
            compute_kwargs = _proc_resources_spec(getattr(args, "compute", ""))
            collate = Collator(prj)
            collate(args, remaining_args, **compute_kwargs)

        if args.command == "destroy":
            return Destroyer(prj)(args)

        if args.command == "summarize":
            Summarizer(prj)()

        if args.command == "check":
            Checker(prj)(flags=args.flags)

        if args.command == "clean":
            return Cleaner(prj)(args)

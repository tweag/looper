""" Looper version of NGS project model. """

import itertools
import os

from jsonschema import ValidationError
from logging import getLogger

from peppy import SAMPLE_NAME_ATTR, OUTDIR_KEY, CONFIG_KEY, \
    Project as peppyProject
from eido import read_schema, PathAttrNotFoundError
from divvy import DEFAULT_COMPUTE_RESOURCES_NAME, ComputingConfiguration
from ubiquerg import is_command_callable

from .processed_project import populate_sample_paths, populate_project_paths
from .const import *
from .exceptions import *
from .looper_config import *

__all__ = ["Project"]

_LOGGER = getLogger(__name__)


class ProjectContext(object):
    """ Wrap a Project to provide protocol-specific Sample selection. """

    def __init__(self, prj, selector_attribute=None,
                 selector_include=None, selector_exclude=None):
        """ Project and what to include/exclude defines the context. """
        if not isinstance(selector_attribute, str):
            raise TypeError(
                "Name of attribute for sample selection isn't a string: {} "
                "({})".format(selector_attribute, type(selector_attribute)))
        self.prj = prj
        self.include = selector_include
        self.exclude = selector_exclude
        self.attribute = selector_attribute

    def __getattr__(self, item):
        """ Samples are context-specific; other requests are handled
        locally or dispatched to Project. """
        if item == "samples":
            return fetch_samples(prj=self.prj,
                                 selector_attribute=self.attribute,
                                 selector_include=self.include,
                                 selector_exclude=self.exclude)
        if item in ["prj", "include", "exclude"]:
            # Attributes requests that this context/wrapper handles
            return self.__dict__[item]
        else:
            # Dispatch attribute request to Project.
            return getattr(self.prj, item)

    def __getitem__(self, item):
        """ Provide the Mapping-like item access to the instance's Project. """
        return self.prj[item]

    def __enter__(self):
        """ References pass through this instance as needed, so the context
         provided is the instance itself. """
        return self

    def __repr__(self):
        return self.prj.__repr__()

    def __exit__(self, *args):
        """ Context teardown. """
        pass


class Project(peppyProject):
    """
    Looper-specific NGS Project.

    :param str config_file: path to configuration file with data from
        which Project is to be built
    :param Iterable[str] amendments: name indicating amendment to use, optional
    :param str compute_env_file: Environment configuration YAML file specifying
        compute settings.
    :param Iterable[str] pifaces: list of path to pipeline interfaces.
        Overrides the config-defined ones.
    :param bool dry: If dry mode is activated, no directories
        will be created upon project instantiation.
    :param bool permissive: Whether a error should be thrown if
        a sample input file(s) do not exist or cannot be open.
    :param bool file_checks: Whether sample input files should be checked
        for their  attributes (read type, read length)
        if this is not set in sample metadata.
    :param str compute_env_file: Environment configuration YAML file specifying
        compute settings.
    :param type no_environment_exception: type of exception to raise if environment
        settings can't be established, optional; if null (the default),
        a warning message will be logged, and no exception will be raised.
    :param type no_compute_exception: type of exception to raise if compute
        settings can't be established, optional; if null (the default),
        a warning message will be logged, and no exception will be raised.
    """
    def __init__(self, config_file, amendments=None, dry=False,
                 compute_env_file=None, no_environment_exception=RuntimeError,
                 no_compute_exception=RuntimeError, file_checks=False,
                 permissive=True, **kwargs):
        super(Project, self).__init__(config_file, amendments=amendments)
        setattr(self, EXTRA_KEY, dict())
        for attr_name in CLI_PROJ_ATTRS:
            if attr_name in kwargs:
                setattr(self[EXTRA_KEY], attr_name, kwargs[attr_name])
        self._apply_user_pifaces(self.cli_pifaces[0] if self.cli_pifaces else None)
        self._samples_by_interface = self._samples_by_piface(self.piface_key)
        self._interfaces_by_sample = self._piface_by_samples()
        self.file_checks = file_checks
        self.permissive = permissive
        self.dcc = ComputingConfiguration(
            config_file=compute_env_file, no_env_error=no_environment_exception,
            no_compute_exception=no_compute_exception
        )
        # Set project's directory structure
        if not dry:
            _LOGGER.debug("Ensuring project directories exist")
            self.make_project_dirs()

    @property
    def piface_key(self):
        """
        Name of the pipeline interface attribute for this project

        :return str: name of the pipeline interface attribute
        """
        return self._extra_cli_or_cfg(PIFACE_KEY_SELECTOR) or PIPELINE_INTERFACES_KEY

    @property
    def toggle_key(self):
        """
        Name of the toggle attribute for this project

        :return str: name of the toggle attribute
        """
        return self._extra_cli_or_cfg(TOGGLE_KEY_SELECTOR) or SAMPLE_TOGGLE_ATTR

    @property
    def selected_compute_package(self):
        return self._extra_cli_or_cfg(COMPUTE_PACKAGE_KEY)

    @property
    def cli_pifaces(self):
        return self._extra_cli_or_cfg(self.piface_key)

    @property
    def output_dir(self):
        return self._extra_cli_or_cfg(OUTDIR_KEY, strict=True)

    def _extra_cli_or_cfg(self, attr_name, strict=False):
        try:
            result = getattr(self[EXTRA_KEY], attr_name)
        except (AttributeError, KeyError):
            return
        if result is not None:
            return result
        if CONFIG_KEY in self and LOOPER_KEY in self[CONFIG_KEY] \
                and attr_name in self[CONFIG_KEY][LOOPER_KEY]:
            return self[CONFIG_KEY][LOOPER_KEY][attr_name]
        else:
            if strict:
                from .utils import dotfile_path
                raise MisconfigurationException(
                    "'{}' is missing. Provide it as CLI argument, in '{}' "
                    "file or in '{}' section of the config".
                        format(attr_name, dotfile_path(), LOOPER_KEY))
            return

    @property
    def results_folder(self):
        return self._out_relpath(RESULTS_SUBDIR_KEY, default="results_pipeline")

    @property
    def submission_folder(self):
        return self._out_relpath(SUBMISSION_SUBDIR_KEY, default="submission")

    def _out_relpath(self, key, default):
        return os.path.join(getattr(self, OUTDIR_KEY),
                            getattr(self[EXTRA_KEY], key) or default)

    def make_project_dirs(self):
        """
        Creates project directory structure if it doesn't exist.
        """
        for folder_key in ["results_folder", "submission_folder"]:
            folder_path = getattr(self, folder_key)
            _LOGGER.debug("Ensuring project dir exists: '{}'".format(folder_path))
            if not os.path.exists(folder_path):
                _LOGGER.debug("Attempting to create project folder: '{}'".
                              format(folder_path))
                try:
                    os.makedirs(folder_path)
                except OSError as e:
                    _LOGGER.warning("Could not create project folder: '{}'".
                                    format(str(e)))

    @property
    def project_pipeline_interface_sources(self):
        """
        Get a list of all valid project-level pipeline interface sources
        associated with this project. Sources that are file paths are expanded

        :return list[str]: collection of valid pipeline interface sources:
        """
        project_pifaces = self._samples_by_interface.keys()
        if self.piface_key in self[CONFIG_KEY][LOOPER_KEY]:
            # pipeline interfaces sources defined in the project config override
            # ones that are matched by the Samples in this Project
            cfg_pifaces = self[CONFIG_KEY][LOOPER_KEY][self.piface_key]
            _LOGGER.debug("Overwriting sample-matched pifaces with a "
                          "config-specified ones: {}".format(cfg_pifaces))
            # make sure it's a list, so both lists and strings can be
            # specified in the config
            if isinstance(cfg_pifaces, str):
                cfg_pifaces = [cfg_pifaces]
            project_pifaces = [expandpath(src) for src in cfg_pifaces]
        return project_pifaces

    @property
    def project_pipeline_interfaces(self):
        """
        Flat list of all valid project-level interface objects associated
        with this Project

        Note that only valid pipeline interfaces will show up in the
        result (ones that exist on disk/remotely and validate successfully
        against the schema)

        :return list[looper.PipelineInterface]: list of pipeline interfaces
        """
        return [PipelineInterface(pi)
                for pi in self.project_pipeline_interface_sources]

    @property
    def pipeline_interfaces(self):
        """
        Flat list of all valid interface objects associated with this Project

        Note that only valid pipeline interfaces will show up in the
        result (ones that exist on disk/remotely and validate successfully
        against the schema)

        :return list[looper.PipelineInterface]: list of pipeline interfaces
        """
        return [i for s in self._interfaces_by_sample.values() for i in s]

    @property
    def pipeline_interface_sources(self):
        """
        Get a list of all valid pipeline interface sources associated
        with this project. Sources that are file paths are expanded

        :return list[str]: collection of valid pipeline interface sources
        """
        return self._samples_by_interface.keys()

    def _apply_user_pifaces(self, pifaces):
        """
        Overwrite sample pipeline interface sources with the provided ones

        :param Iterable[str] | str | NoneType pifaces: collection of pipeline
            interface sources
        """
        _LOGGER.debug("CLI-specified pifaces: {}".format(pifaces))
        valid_pi = []
        if not pifaces:
            # No CLI-specified pipeline interface sources
            return
        if isinstance(pifaces, str):
            pifaces = [pifaces]
        for piface in pifaces:
            pi = expandpath(piface)
            try:
                PipelineInterface(pi)
            except Exception as e:
                _LOGGER.warning("Provided pipeline interface source ({}) is "
                                "invalid. Caught exception: {}".
                                format(pi, getattr(e, 'message', repr(e))))
            else:
                valid_pi.append(pi)
        [setattr(s, self.piface_key, valid_pi) for s in self.samples]
        if valid_pi:
            _LOGGER.info("Provided valid pipeline interface sources ({}) "
                         "set in all samples".format(", ".join(valid_pi)))

    def get_sample_piface(self, sample_name):
        """
        Get a list of pipeline interfaces associated with the specified sample.

        Note that only valid pipeline interfaces will show up in the
        result (ones that exist on disk/remotely and validate successfully
        against the schema)

        :param str sample_name: name of the sample to retrieve list of
            pipeline interfaces for
        :return list[looper.PipelineInterface]: collection of valid
            pipeline interfaces associated with selected sample
        """
        try:
            return self._interfaces_by_sample[sample_name]
        except KeyError:
            return None

    def build_submission_bundles(self, protocol, priority=True):
        """
        Create pipelines to submit for each sample of a particular protocol.

        With the argument (flag) to the priority parameter, there's control
        over whether to submit pipeline(s) from only one of the project's
        known pipeline locations with a match for the protocol, or whether to
        submit pipelines created from all locations with a match for the
        protocol.

        :param str protocol: name of the protocol/library for which to
            create pipeline(s)
        :param bool priority: to only submit pipeline(s) from the first of the
            pipelines location(s) (indicated in the project config file) that
            has a match for the given protocol; optional, default True
        :return Iterable[(PipelineInterface, type, str, str)]:
        :raises AssertionError: if there's a failure in the attempt to
            partition an interface's pipeline scripts into disjoint subsets of
            those already mapped and those not yet mapped
        """

        if not priority:
            raise NotImplementedError(
                "Currently, only prioritized protocol mapping is supported "
                "(i.e., pipeline interfaces collection is a prioritized list, "
                "so only the first interface with a protocol match is used.)")

        # Pull out the collection of interfaces (potentially one from each of
        # the locations indicated in the project configuration file) as a
        # sort of pool of information about possible ways in which to submit
        # pipeline(s) for sample(s) of the indicated protocol.
        pifaces = self.interfaces.get_pipeline_interface(protocol)
        if not pifaces:
            raise PipelineInterfaceConfigError(
                "No interfaces for protocol: {}".format(protocol))

        # coonvert to a list, in the future we might allow to match multiple
        pifaces = pifaces if isinstance(pifaces, str) else [pifaces]

        job_submission_bundles = []
        new_jobs = []

        _LOGGER.debug("Building pipelines matched by protocol: {}".
                      format(protocol))

        for pipe_iface in pifaces:
            # Determine how to reference the pipeline and where it is.
            path = pipe_iface[SAMPLE_PL_KEY]["path"]
            if not (os.path.exists(path) or
                    is_command_callable(path)):
                _LOGGER.warning("Missing pipeline script: {}".
                                format(path))
                continue

            # Add this bundle to the collection of ones relevant for the
            # current PipelineInterface.
            new_jobs.append(pipe_iface)

            job_submission_bundles.append(new_jobs)
        return list(itertools.chain(*job_submission_bundles))

    @staticmethod
    def get_schemas(pifaces, schema_key=INPUT_SCHEMA_KEY):
        """
        Get the list of unique schema paths for a list of pipeline interfaces

        :param str | Iterable[str] pifaces: pipeline interfaces to search
            schemas for
        :param str schema_key: where to look for schemas in the piface
        :return Iterable[str]: unique list of schema file paths
        """
        if isinstance(pifaces, str):
            pifaces = [pifaces]
        schema_set = set()
        for piface in pifaces:
            schema_file = piface.get_pipeline_schemas(SAMPLE_PL_KEY, schema_key)
            if schema_file:
                schema_set.update([schema_file])
        return list(schema_set)

    def populate_pipeline_outputs(self, check_exist=False):
        """
        Populate project and sample output attributes based on output schemas
        that pipeline interfaces point to. Additionally, if requested,  check
        for the constructed paths existence on disk
        """
        for sample in self.samples:
            sample_piface = self.get_sample_piface(sample[SAMPLE_NAME_ATTR])
            if sample_piface:
                paths = self.get_schemas(sample_piface, OUTPUT_SCHEMA_KEY)
                for path in paths:
                    schema = read_schema(path)[-1]
                    try:
                        populate_project_paths(self, schema, check_exist)
                        populate_sample_paths(sample, schema, check_exist)
                    except PathAttrNotFoundError:
                        _LOGGER.error(
                            "Missing outputs of pipelines matched by protocol: "
                            "{}".format(sample.protocol)
                        )
                        raise

    def _piface_by_samples(self):
        """
        Create a mapping of all defined interfaces in this Project by samples.

        :return list[str]: a collection of pipeline interfaces keyed by
        sample name
        """
        pifaces_by_sample = {}
        for source, sample_names in self._samples_by_interface.items():
            for sample_name in sample_names:
                pifaces_by_sample.setdefault(sample_name, [])
                pifaces_by_sample[sample_name].\
                    append(PipelineInterface(source))
        return pifaces_by_sample

    def _omit_from_repr(self, k, cls):
        """
        Exclude the interfaces from representation.

        :param str k: key of item to consider for omission
        :param type cls: placeholder to comply with superclass signature
        """
        return super(Project, self)._omit_from_repr(k, cls) or k == "interfaces"

    def _samples_by_piface(self, piface_key):
        """
        Create a collection of all samples with valid pipeline interfaces

        :param str piface_key: name of the attribute that holds pipeline interfaces
        :return list[str]: a collection of samples keyed by pipeline interface
            source
        """
        samples_by_piface = {}
        msgs = set()
        for sample in self.samples:
            if piface_key in sample and sample[piface_key]:
                piface_srcs = sample[piface_key]
                if isinstance(piface_srcs, str):
                    piface_srcs = [piface_srcs]
                for source in piface_srcs:
                    source = expandpath(source)
                    if not os.path.isabs(source):
                        source = os.path.join(
                            os.path.dirname(self.config_file), source)
                    try:
                        PipelineInterface(source)
                    except (ValidationError, IOError) as e:
                        msg = "Ignoring invalid pipeline interface source: " \
                              "{}. Caught exception: {}".\
                            format(source, getattr(e, 'message', repr(e)))
                        msgs.add(msg)
                        continue
                    else:
                        samples_by_piface.setdefault(source, set())
                        samples_by_piface[source].add(sample[SAMPLE_NAME_ATTR])
        for msg in msgs:
            _LOGGER.warning(msg)
        return samples_by_piface


def fetch_samples(prj, selector_attribute=None, selector_include=None,
                  selector_exclude=None):
    """
    Collect samples of particular protocol(s).

    Protocols can't be both positively selected for and negatively
    selected against. That is, it makes no sense and is not allowed to
    specify both selector_include and selector_exclude protocols. On the
    other hand, if
    neither is provided, all of the Project's Samples are returned.
    If selector_include is specified, Samples without a protocol will be
    excluded,
    but if selector_exclude is specified, protocol-less Samples will be
    included.

    :param Project prj: the Project with Samples to fetch
    :param str selector_attribute: name of attribute on which to base the
    fetch
    :param Iterable[str] | str selector_include: protocol(s) of interest;
        if specified, a Sample must
    :param Iterable[str] | str selector_exclude: protocol(s) to include
    :return list[Sample]: Collection of this Project's samples with
        protocol that either matches one of those in selector_include,
        or either
        lacks a protocol or does not match one of those in selector_exclude
    :raise TypeError: if both selector_include and selector_exclude
    protocols are
        specified; TypeError since it's basically providing two arguments
        when only one is accepted, so remain consistent with vanilla
        Python2;
        also possible if name of attribute for selection isn't a string
    """
    if selector_attribute is None or \
            (not selector_include and not selector_exclude):
        # Simple; keep all samples.  In this case, this function simply
        # offers a list rather than an iterator.
        return list(prj.samples)

    if not isinstance(selector_attribute, str):
        raise TypeError(
            "Name for attribute on which to base selection isn't string: "
            "{} "
            "({})".format(selector_attribute, type(selector_attribute)))

    # At least one of the samples has to have the specified attribute
    if prj.samples and not any(
            [hasattr(s, selector_attribute) for s in prj.samples]):
        raise AttributeError(
            "The Project samples do not have the attribute '{attr}'".
                format(attr=selector_attribute))

    # Intersection between selector_include and selector_exclude is
    # nonsense user error.
    if selector_include and selector_exclude:
        raise TypeError(
            "Specify only selector_include or selector_exclude parameter, "
            "not both.")

    # Ensure that we're working with sets.
    def make_set(items):
        if isinstance(items, str):
            items = [items]
        return items

    # Use the attr check here rather than exception block in case the
    # hypothetical AttributeError would occur; we want such
    # an exception to arise, not to catch it as if the Sample lacks
    # "protocol"
    if not selector_include:
        # Loose; keep all samples not in the selector_exclude.
        def keep(s):
            return not hasattr(s, selector_attribute) \
                   or getattr(s, selector_attribute) \
                   not in make_set(selector_exclude)
    else:
        # Strict; keep only samples in the selector_include.
        def keep(s):
            return hasattr(s, selector_attribute) \
                   and getattr(s, selector_attribute) \
                   in make_set(selector_include)

    return list(filter(keep, prj.samples))

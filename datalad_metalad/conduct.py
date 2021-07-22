# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Conduct the execution of a processing pipeline
"""
import concurrent.futures
import logging
from importlib import import_module
from typing import List, Union, Optional

from datalad.distribution.dataset import datasetmethod
from datalad.interface.base import build_doc
from datalad.interface.base import Interface
from datalad.interface.utils import eval_results
from datalad.support.constraints import (
    EnsureNone,
    EnsureInt,
)
from datalad.support.param import Parameter
from dataladmetadatamodel import JSONObject

from .processor.base import Processor
from .utils import read_json_object

__docformat__ = 'restructuredtext'

default_metadata_backend = "git"

lgr = logging.getLogger('datalad.metadata.conduct')


class ConductProcessorException(Exception):
    pass


@build_doc
class Conduct(Interface):
    """Conduct the execution of a processing pipeline

    A processing pipeline is a metalad-specific application of
    the Unix shell philosophy, have a number of small programs
    that do one thing, but that one thing, very well.

    Processing pipelines consist of:

    - A provider, that provides data that should be processed

    - A list of processors. A processor reads data,
      either from the previous processor or the provider and performs
      computations on the data and return a result that is processed by
      the next processor. The computation may have side-effect,
      e.g. store metadata.

    The provider is usually executed in the current processes' main
    thread. Processors are usually executed in concurrent processes,
    i.e. workers. The maximum number of workers is given by the
    parameter `max_workers`.

    Which provider and which processors are used is defined in an
    "configuration", which is given as JSON-serialized dictionary.
    """

    _examples_ = [
        dict(
            text='[DOES NOT WORK YET] Perform the "old" aggregate',
            code_cmd="datalad meta-conduct dataset_traversal extract add"),
    ]

    _params_ = dict(
        max_workers=Parameter(
            args=("-m", "--max-workers",),
            metavar="MAX_WORKERS",
            doc="maximum number of workers",
            default=None,
            constraints=EnsureInt() | EnsureNone()),
        configuration=Parameter(
            args=("configuration",),
            metavar="CONFIGURATION",
            doc="""Path to a file with contains the pipeline configuration
                   as JSON-serialized object. If the path is "-", the
                   configuration is read from standard input."""),
        arguments=Parameter(
            args=("arguments",),
            metavar="ARGUMENTS",
            nargs="*",
            doc="""Additional constructor arguments for provider or processors.
            The arguments have to be prefixed with either "p:" for provider,
            or an integer for a processor. The integer is the index of the
            processor in the processor list of the configuration, e.g.
            "0:" for the first processor, "1:" for the second processor
            etc.
    
            The arguments will be appended to the respective argument list
            that is given in the configuration.""")
    )

    @staticmethod
    @datasetmethod(name='meta_conduct')
    @eval_results
    def __call__(
            configuration: Union[str, JSONObject],
            arguments: List[str],
            max_workers: Optional[int] = None):

        conduct_configuration = read_json_object(configuration)

        additional_arguments = get_additional_arguments(
            arguments,
            conduct_configuration)

        provider_instance = get_class_instance(
            conduct_configuration["provider"])(
            *(conduct_configuration["provider"]["arguments"] + additional_arguments["provider"]),
            **conduct_configuration["provider"]["keyword_arguments"])

        processor_instances = [
            get_class_instance(spec)(
                *spec["arguments"] + additional_arguments["processors"][index],
                **spec["keyword_arguments"])
            for index, spec in enumerate(conduct_configuration["processors"])]

        assert_pipeline_validity(
            provider_instance.output_type(),
            processor_instances)

        executor = concurrent.futures.ProcessPoolExecutor(max_workers)
        running = set()

        for initial_result in provider_instance.next_object():
            lgr.debug(f"Provider yielded: {initial_result}")
            lgr.debug(f"Starting instance {processor_instances[0]} on {initial_result}")
            running.add(executor.submit(processor_instances[0].execute, -1, initial_result))

            lgr.debug(f"Waiting for first completing from: {running}")
            done, running = concurrent.futures.wait(
                running,
                return_when=concurrent.futures.FIRST_COMPLETED,
                timeout=0)

            for future in done:
                try:
                    source_index, result_list = future.result()
                    this_index = source_index + 1
                    next_index = this_index + 1
                    for result in result_list:
                        lgr.debug(
                            f"Element[{source_index}] returned result "
                            f"{result} [provider not yet exhausted]")
                        if next_index >= len(processor_instances):
                            lgr.debug(
                                f"No more elements in pipeline, returning "
                                f"{result} [provider not yet exhausted]")
                            yield dict(
                                action="meta_conduct",
                                status="ok",
                                logger=lgr,
                                path=result["path"],
                                result=result)
                        else:
                            lgr.debug(
                                f"Handing result {result} to element "
                                f"{next_index} in pipeline [provider not yet "
                                f"exhausted]")
                            running.add(
                                executor.submit(
                                    processor_instances[next_index].execute,
                                    this_index,
                                    result))
                except ConductProcessorException as e:
                    lgr.error(f"Exception {e} in processor {future}")
                    yield dict(
                        action="meta_conduct",
                        status="error",
                        logger=lgr,
                        message=e.args[0])

        while running:
            lgr.debug(f"Waiting for first completing from: {running}")
            done, running = concurrent.futures.wait(
                running,
                return_when=concurrent.futures.FIRST_COMPLETED)

            for future in done:
                try:
                    source_index, result_list = future.result()
                    this_index = source_index + 1
                    next_index = this_index + 1
                    for result in result_list:
                        lgr.debug(
                            f"Element[{source_index}] returned result {result}")
                        if next_index >= len(processor_instances):
                            lgr.debug(
                                f"No more elements in pipeline, returning "
                                f"returning {result}")
                            yield dict(
                                action="meta_conduct",
                                status="ok",
                                logger=lgr,
                                path=result["path"],
                                result=result)
                        else:
                            lgr.debug(
                                f"Handing result {result} to element "
                                f"{next_index} in pipeline")
                            running.add(
                                executor.submit(
                                    processor_instances[next_index].execute,
                                    this_index,
                                    result))
                except ConductProcessorException as e:
                    lgr.error(f"Exception {e} in processor {future}")
                    yield dict(
                        action="meta_conduct",
                        status="error",
                        logger=lgr,
                        message=e.args[0])
        return


def get_class_instance(module_class_spec: dict):
    module_instance = import_module(module_class_spec["module"])
    class_instance = getattr(module_instance, module_class_spec["class"])
    return class_instance


def assert_pipeline_validity(current_output_type: str, processors: List[Processor]):
    for processor in processors:
        next_input_type = processor.input_type()
        if next_input_type != current_output_type:
            raise ValueError(
                f"Input type mismatch: {next_input_type} "
                f"!= {current_output_type}")
        current_output_type = processor.output_type()


def get_additional_arguments(arguments: List[str],
                             conduct_configuration: JSONObject) -> dict:
    result = dict(
        provider=[],
        processors={
            i: []
            for i in range(len(conduct_configuration["processors"]))})

    for argument in arguments:
        if argument.startswith("p:"):
            result["provider"].append(argument[2:])
        else:
            prefix, argument = argument.split(":", 1)
            if int(prefix) >= len(result["processors"]):
                lgr.warning(
                    f"ignoring argument {argument} for non-existing processor "
                    f"#{prefix}")
                continue
            result["processors"][int(prefix)].append(argument)

    return result

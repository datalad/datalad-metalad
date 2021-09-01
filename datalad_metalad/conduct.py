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
from collections import defaultdict
from importlib import import_module
from itertools import chain
from typing import Dict, Iterable, List, Union, Optional

from datalad.distribution.dataset import datasetmethod
from datalad.interface.base import build_doc
from datalad.interface.base import Interface
from datalad.interface.utils import eval_results
from datalad.support.constraints import (
    EnsureChoice,
    EnsureInt,
    EnsureNone
)
from datalad.support.param import Parameter
from dataladmetadatamodel import JSONObject

from .pipelineelement import PipelineElement, PipelineElementState
from .processor.base import Processor
from .provider.base import Provider

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
            text="""
                Run metalad_core_dataset extractor on the top dataset and all
                subdatasets. "pipeline-spec.json" looks like this::
        
                  {
                      "provider": {
                        "module": "datalad_metalad.provider.datasettraverse",
                        "class": "DatasetTraverser",
                        "arguments": [],
                        "keyword_arguments": {}
                      },
                      "processors": [
                        {
                          "module": "datalad_metalad.processor.extract",
                          "class": "MetadataExtractor",
                          "arguments": [],
                          "keyword_arguments": {}
                        },
                        {
                          "module": "datalad_metalad.processor.add",
                          "class": "MetadataAdder",
                          "arguments": [],
                          "keyword_arguments": {}
                        }
                      ]
                    }
            """,
            code_cmd="datalad meta-conduct pipeline2_conf.json p:<dataset path>"
                     " 0:Dataset 0:metalad_core_dataset 1:<metadata-repo path>"
        ),
        dict(
            text="""
                Run metalad_core_file extractor on all files of the root dataset
                and the subdatasets. Automatically get the content, if it is
                not present. Drop everything that was automatically fetched.
                The pipeline specification looks like this::
        
                  {
                      "provider": {
                        "module": "datalad_metalad.provider.datasettraverse",
                        "class": "DatasetTraverser",
                        "arguments": [],
                        "keyword_arguments": {}
                      },
                      "processors": [
                        {
                          "module": "datalad_metalad.processor.autoget",
                          "class": "AutoGet",
                          "arguments": [],
                          "keyword_arguments": {}
                        },
                        {
                          "module": "datalad_metalad.processor.extract",
                          "class": "MetadataExtractor",
                          "arguments": [],
                          "keyword_arguments": {}
                        },
                        {
                          "module": "datalad_metalad.processor.add",
                          "class": "MetadataAdder",
                          "arguments": [],
                          "keyword_arguments": {}
                        },
                        {
                          "module": "datalad_metalad.processor.autodrop",
                          "class": "AutoDrop",
                          "arguments": [],
                          "keyword_arguments": {}
                        }
                      ]
                    }
            """,
            code_cmd="datalad meta-conduct pipeline-spec_conf.json "
                     "p:<dataset path> 1:Dataset 1:metalad_core_dataset "
                     "2:<metadata-repo path>"
        )
    ]

    _params_ = dict(
        max_workers=Parameter(
            args=("-m", "--max-workers",),
            metavar="MAX_WORKERS",
            doc="maximum number of workers",
            default=None,
            constraints=EnsureInt() | EnsureNone()),
        processing_mode=Parameter(
            args=("-p", "--processing-mode",),
            doc="""Specify how elements are executed, either in subprocesses,
                   in threads, or sequentially in the main thread. The
                   respective values are "process", "thread", and "sequential",
                   (default: "process").""",
            constraints=EnsureChoice("process", "thread", "sequential"),
            default="process"),
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
                   The arguments have to be prefixed with the name of the
                   provider or processor followed by ":" or "=".
                   
                   Positional arguments are identified by the prefix "<name>:".
                   The argument is given directly after the prefix. The
                   arguments will be appended to the respective argument
                   list that is given in the configuration.
                   
                   Key-Value parameter are identified by the prefix "<name>=".
                   The key and value are then given as: "<name>=<key>=<value>".
                   """)
    )

    @staticmethod
    @datasetmethod(name='meta_conduct')
    @eval_results
    def __call__(
            configuration: Union[str, JSONObject],
            arguments: List[str],
            max_workers: Optional[int] = None,
            processing_mode: str = "process"):

        conduct_configuration = read_json_object(configuration)

        element_names = [
            element["name"] for element in chain(
                [conduct_configuration["provider"]],
                conduct_configuration["processors"])]

        additional_arguments = get_additional_arguments(
            arguments,
            element_names)

        provider_name = conduct_configuration["provider"]["name"]
        provider_instance = get_class_instance(conduct_configuration["provider"])(
            *(
                conduct_configuration["provider"]["arguments"] +
                additional_arguments[provider_name]["positional_arguments"]
            ),
            **{
                **conduct_configuration["provider"]["keyword_arguments"],
                **additional_arguments[provider_name]["keyword_arguments"]
            })

        processor_instances = [
            get_class_instance(spec)(
                *(spec["arguments"] + additional_arguments[spec["name"]]["positional_arguments"]),
                **{
                    **spec["keyword_arguments"],
                    **additional_arguments[spec["name"]]["keyword_arguments"]
                }
            )
            for index, spec in enumerate(conduct_configuration["processors"])]

        if processing_mode == "sequential":
            yield from process_sequential(
                provider_instance,
                processor_instances)
            return
        elif processing_mode == "thread":
            executor = concurrent.futures.ThreadPoolExecutor(max_workers)
        elif processing_mode == "process":
            executor = concurrent.futures.ProcessPoolExecutor(max_workers)
        else:
            raise ValueError(f"unsupported processing mode: {processing_mode}")

        yield from process_parallel(
            executor,
            provider_instance,
            processor_instances)
        return


def process_parallel(executor,
                     provider_instance,
                     processor_instances) -> Iterable:

    running = set()

    # This thread iterates over the provider result and
    # starts a new pipeline element to process the result
    for pipeline_element in provider_instance.next_object():

        lgr.debug(f"Starting instance {processor_instances[0]} on {pipeline_element}")
        running.add(executor.submit(processor_instances[0].execute, -1, pipeline_element))

        # During provider result fetching, check for already finished processors
        done, running = concurrent.futures.wait(
            running,
            return_when=concurrent.futures.FIRST_COMPLETED,
            timeout=0)

        for future in done:
            try:

                source_index, pipeline_element = future.result()
                this_index = source_index + 1
                next_index = this_index + 1

                lgr.debug(
                    f"Processor[{source_index}] returned {pipeline_element} "
                    f"[provider not yet exhausted]")
                if next_index >= len(processor_instances):
                    yield dict(
                        action="meta_conduct",
                        status="ended",
                        logger=lgr,
                        result=pipeline_element)
                else:
                    lgr.debug(
                        f"Starting processor[{next_index}]"
                        f"[provider not yet exhausted]")
                    running.add(
                        executor.submit(
                            processor_instances[next_index].execute,
                            this_index,
                            pipeline_element))

            except Exception as e:
                lgr.error(f"Exception {e} in processor {future}")
                yield dict(
                    action="meta_conduct",
                    status="error",
                    logger=lgr,
                    message=e.args[0])

    # Provider exhausted, process the running pipelines
    while running:
        lgr.debug(f"Waiting for next completing from {running}")
        done, running = concurrent.futures.wait(
            running,
            return_when=concurrent.futures.FIRST_COMPLETED)

        for future in done:
            try:

                source_index, pipeline_element = future.result()
                this_index = source_index + 1
                next_index = this_index + 1

                lgr.debug(
                    f"Processor[{source_index}] returned {pipeline_element}")

                if next_index >= len(processor_instances):
                    lgr.debug(
                        f"No more elements in pipeline, "
                        f"returning {pipeline_element}")
                    yield dict(
                        action="meta_conduct",
                        status="ended",
                        logger=lgr,
                        result=pipeline_element)
                else:
                    lgr.debug(
                        f"Handing pipeline element {pipeline_element} to"
                        f"processor[{next_index}]")
                    running.add(
                        executor.submit(
                            processor_instances[next_index].execute,
                            this_index,
                            pipeline_element))

            except Exception as e:
                lgr.error(f"Exception {e} in processor {future}")
                yield dict(
                    action="meta_conduct",
                    status="error",
                    logger=lgr,
                    message=e.args[0])
    return


def process_sequential(provider_instance: Provider,
                       processor_instances: List[Processor]) -> Iterable:

    for pipeline_element in provider_instance.next_object():

        lgr.debug(f"Provider yielded: {pipeline_element}")
        yield from process_downstream(pipeline_element, processor_instances)
    return


def process_downstream(pipeline_element: PipelineElement,
                       processor_instances: List[Processor]) -> Iterable:

    if pipeline_element.state == PipelineElementState.STOP:
        datalad_result = dict(
            action="meta_conduct",
            status="stopped",
            logger=lgr,
            result=pipeline_element)

        lgr.debug(
            f"Pipeline stop was requested, returning datalad result {datalad_result}")

        yield datalad_result
        return

    for processor in processor_instances:
        try:
            _, pipeline_element = processor.execute(None, pipeline_element)
        except Exception as e:
            yield dict(
                action="meta_conduct",
                status="error",
                logger=lgr,
                message=f"Exception in processor {processor}",
                base_error=str(e))
            return

    datalad_result = dict(
        action="meta_conduct",
        status="ended",
        logger=lgr,
        result=pipeline_element)

    lgr.debug(
        f"Pipeline finished, returning datalad result {datalad_result}")

    yield datalad_result
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
                             element_names: List[str]) -> dict:

    def element_dict() -> Dict:
        return dict(positional_arguments=[], keyword_arguments={})

    def sortable_index(string: str, character: str):
        index = string.find(character)
        return len(string) if index < 0 else index

    result = defaultdict(element_dict)

    for argument in arguments:
        delimiter = sorted(
            [
                (sortable_index(argument, delimiter), delimiter)
                for delimiter in (":", "=")
            ],
            key=lambda element: element[0]
        )[0][1]

        name, argument = argument.split(delimiter, 1)
        if name not in element_names:
            raise ValueError(f"No provider or processor with name: '{name}'")

        if delimiter == ":":
            result[name]["positional_arguments"].append(argument)
        else:
            key, value = argument.split("=", 1)
            result[name]["keyword_arguments"][key] = value
    return result

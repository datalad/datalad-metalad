# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Conduct the execution of a processing pipeline.

NB: Individual elements are instantiated once and reused in the individual
parallel executions.
"""
import concurrent.futures
import logging
import traceback
from collections import defaultdict
from importlib import import_module
from itertools import chain
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from datalad.distribution.dataset import datasetmethod
from datalad.interface.base import build_doc
from datalad.interface.base import Interface
from datalad.interface.utils import eval_results
from datalad.support.constraints import (
    EnsureChoice,
    EnsureInt,
    EnsureNone,
)
from datalad.support.param import Parameter

from .pipeline.pipelinedata import (
    PipelineData,
    PipelineDataState,
)
from .pipeline.pipelineelement import PipelineElement
from .pipeline.consumer.base import Consumer
from .pipeline.processor.base import Processor
from .pipeline.provider.base import Provider
from .metadatatypes import JSONType
from .utils import read_json_object


__docformat__ = 'restructuredtext'

default_metadata_backend = "git"

lgr = logging.getLogger('datalad.metadata.conduct')


def split_arguments(arguments: List[str], divider: str) -> Tuple[List, List]:
    if divider in arguments:
        index = arguments.index(divider)
        return arguments[:index], arguments[index + 1:]
    else:
        return arguments, []


def check_arguments(keyword_arguments: Dict[str, Dict[str, str]],
                    elements: List[Dict]) -> Optional[str]:

    error_messages = []
    for element in elements:
        element_kwargs = keyword_arguments[element["name"]]
        element_class = get_class_instance(element)
        error_message = element_class.check_keyword_args(element_kwargs)
        if error_message is not None:
            error_messages.append(error_message)

    if error_messages:
        return "\n".join(error_messages)
    return None


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
            text="Run 'metalad_example_dataset' extractor on the top dataset "
                 "and all subdatasets. Add the resulting metadata in aggregated"
                 "mode. This command uses the provided pipeline"
                 "definition 'extract_metadata'.",
            code_cmd="datalad meta-conduct extract_metadata "
                     "traverser.top_level_dir=<dataset path> "
                     "traverser.item_type=dataset "
                     "traverser.traverse_sub_datasets=True "
                     "extractor.extractor_type=dataset "
                     "extractor.extractor_name=metalad_example_dataset "
                     "adder.aggregate=True"
        ),
        dict(
            text="Run metalad_example_file extractor on all files of the root "
                 "dataset and the subdatasets. Automatically get the content, "
                 "if it is not present. Drop content that was automatically "
                 "fetched after its metadata has been added.",
            code_cmd="datalad meta-conduct extract_metadata_autoget_autodrop "
                     "traverser.top_level_dir=<dataset path> "
                     "traverser.item_type=file "
                     "traverser.traverse_sub_datasets=True "
                     "extractor.extractor_type=file "
                     "extractor.extractor_name=metalad_example_file "
                     "adder.aggregate=True"
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
        pipeline_help=Parameter(
            args=("--pipeline-help",),
            doc="Show documentation for the elements in the pipeline and exit.",
            action="store_true",
            default=False),
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
            doc="""Constructor arguments for pipeline elements, i.e. provider,
                   processors, and consumer. The arguments have to be prefixed
                   with the name of the pipeline element, followed by ".",
                   the keyname, a "=", and the value. The pipeline element
                   arguments are identified by the pattern
                   "<name>.<key>=<value>"."""),
    )

    @staticmethod
    @datasetmethod(name='meta_conduct')
    @eval_results
    def __call__(
            configuration: Union[str, JSONType],
            arguments: List[str],
            max_workers: Optional[int] = None,
            processing_mode: str = "process",
            pipeline_help: bool = False):

        element_arguments = arguments
        conduct_configuration = read_json_object(configuration)

        elements = [
            element
            for element in chain(
                [conduct_configuration["provider"]],
                conduct_configuration["processors"],
                [conduct_configuration.get("consumer", None)]
            )
            if element is not None
        ]

        element_names = [element["name"] for element in elements]
        if len(element_names) != len(set(element_names)):
            raise ValueError("repeated element names")

        class_instances = {
            element["name"]: get_class_instance(element)
            for element in chain(
                [conduct_configuration["provider"]],
                conduct_configuration["processors"],
                [conduct_configuration.get("consumer", None)]
            )
            if element is not None
        }

        if pipeline_help is True:
            for name, class_instance in class_instances.items():
                print(f"== Element: {name} =============================")
                print(class_instance.interface_documentation.get_description())
                print(f".. Variables: {'.' * len(name)}............................")
                print(class_instance.interface_documentation.get_entry_description(name))
                print(f"=============={'=' * len(name)}============================\n")
            return

        constructor_keyword_args = get_constructor_keyword_args(
            element_arguments=element_arguments,
            element_names=element_names)

        error_message = check_arguments(constructor_keyword_args, elements)
        if error_message:
            raise ValueError(
                "Pipeline element construction errors:\n"
                f"{error_message}\n")

        evaluated_constructor_args = evaluate_constructor_args(
            class_instance=class_instances,
            element_arguments=constructor_keyword_args)

        consumer_element = conduct_configuration.get("consumer", None)
        if consumer_element:
            consumer_name = consumer_element["name"]
            consumer_instance = get_class_instance(consumer_element)(
                **{
                    **conduct_configuration["consumer"]["arguments"],
                    **evaluated_constructor_args[consumer_name]
                })
        else:
            consumer_instance = None

        provider_name = conduct_configuration["provider"]["name"]
        provider_instance = get_class_instance(
            conduct_configuration["provider"])(
            **{
                **conduct_configuration["provider"]["arguments"],
                **evaluated_constructor_args[provider_name]
            })

        processor_instances = [
            get_class_instance(spec)(
                **{
                    **spec["arguments"],
                    **evaluated_constructor_args[spec["name"]]
                }
            )
            for index, spec in enumerate(conduct_configuration["processors"])]

        if processing_mode == "sequential":
            yield from process_sequential(
                provider_instance,
                processor_instances,
                consumer_instance)
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
                processor_instances,
                consumer_instance)


def process_parallel(executor,
                     provider_instance: Provider,
                     processor_instances: List[Processor],
                     consumer_instance: Optional[Consumer] = None
                     ) -> Iterable:

    running = set()

    # This thread iterates over the provider result,
    # starts a new processor instance to process the result,
    # and feeds the result of every pipeline into the consumer.
    for pipeline_data in provider_instance.next_object():

        if not processor_instances:
            path = pipeline_data.get_result("path")
            yield dict(
                action="meta_conduct",
                status="ok",
                path=str(path),
                logger=lgr,
                pipeline_data=pipeline_data.to_json())
            continue

        lgr.debug(f"Starting {processor_instances[0]} on {pipeline_data}")
        running.add(
            executor.submit(
                processor_instances[0].execute,
                -1,
                pipeline_data))

        # During provider result fetching, check for already finished processors
        done, running = concurrent.futures.wait(
            running,
            return_when=concurrent.futures.FIRST_COMPLETED,
            timeout=0)

        for future in done:
            try:

                source_index, pipeline_data = future.result()
                this_index = source_index + 1
                next_index = this_index + 1

                lgr.debug(
                    f"Processor[{source_index}] returned {pipeline_data} "
                    f"[provider not yet exhausted]")
                if next_index >= len(processor_instances):
                    if consumer_instance:
                        pipeline_data = consumer_instance.consume(pipeline_data)

                    path = pipeline_data.get_result("path")
                    if path is not None:
                        yield dict(
                            action="meta_conduct",
                            status="ok",
                            path=str(path),
                            logger=lgr,
                            pipeline_data=pipeline_data.to_json())
                else:
                    lgr.debug(
                        f"Starting processor[{next_index}]"
                        f"[provider not yet exhausted]")
                    running.add(
                        executor.submit(
                            processor_instances[next_index].execute,
                            this_index,
                            pipeline_data))

            except Exception as e:
                lgr.error(f"Exception {e} in processor {future}")
                yield dict(
                    action="meta_conduct",
                    status="error",
                    logger=lgr,
                    message=traceback.format_exc())

    # Provider exhausted, process the running pipelines
    while running:
        lgr.debug(f"Waiting for next completing from {running}")
        done, running = concurrent.futures.wait(
            running,
            return_when=concurrent.futures.FIRST_COMPLETED)

        for future in done:
            try:

                source_index, pipeline_data = future.result()
                this_index = source_index + 1
                next_index = this_index + 1

                lgr.debug(
                    f"Processor[{source_index}] returned {pipeline_data}")

                if next_index >= len(processor_instances):
                    if consumer_instance:
                        pipeline_data = consumer_instance.consume(pipeline_data)
                    lgr.debug(
                        f"No more elements in pipeline, returning "
                        f"{pipeline_data}")

                    path = pipeline_data.get_result("path")
                    if path is not None:
                        yield dict(
                            action="meta_conduct",
                            status="ok",
                            path=str(path),
                            logger=lgr,
                            pipeline_data=pipeline_data.to_json())
                else:
                    lgr.debug(
                        f"Handing pipeline data {pipeline_data} to"
                        f"processor[{next_index}]")
                    running.add(
                        executor.submit(
                            processor_instances[next_index].execute,
                            this_index,
                            pipeline_data))

            except Exception as e:
                lgr.error(f"Exception {e} in processor {future}")
                yield dict(
                    action="meta_conduct",
                    status="error",
                    logger=lgr,
                    message=traceback.format_exc())
    return


def process_sequential(provider_instance: Provider,
                       processor_instances: List[Processor],
                       consumer_instance: Optional[Consumer]) -> Iterable:

    for pipeline_data in provider_instance.next_object():
        lgr.debug(f"Provider yielded: {pipeline_data}")
        yield from process_downstream(
            pipeline_data=pipeline_data,
            processor_instances=processor_instances,
            consumer_instance=consumer_instance)


def process_downstream(pipeline_data: PipelineData,
                       processor_instances: List[Processor],
                       consumer_instance: Optional[Consumer]) -> Iterable:

    if pipeline_data.state == PipelineDataState.STOP:
        path = pipeline_data.get_result("path")
        if path is not None:
            datalad_result = dict(
                action="meta_conduct",
                status="stopped",
                path=str(path),
                logger=lgr,
                pipeline_data=pipeline_data)

            lgr.debug(
                f"Pipeline stop was requested, "
                f"returning datalad result {datalad_result}")

            yield datalad_result
        return

    for processor in processor_instances:
        try:
            _, pipeline_data = processor.execute(None, pipeline_data)
        except Exception as exc:
            yield dict(
                action="meta_conduct",
                status="error",
                logger=lgr,
                message=f"Exception in processor {processor}: {exc}",
                base_error=traceback.format_exc())
            return

    if consumer_instance:
        try:
            pipeline_data = consumer_instance.consume(pipeline_data)
        except Exception as exc:
            yield dict(
                action="meta_conduct",
                status="error",
                logger=lgr,
                message=f"Exception in consumer {consumer_instance}: {exc}",
                base_error=traceback.format_exc())
            return

    path = pipeline_data.get_result("path")
    if path is not None:
        datalad_result = dict(
            action="meta_conduct",
            status="ok",
            path=str(path),
            logger=lgr,
            pipeline_data=pipeline_data.to_json())

        lgr.debug(
            f"Pipeline finished, returning datalad result {datalad_result}")

        yield datalad_result
    return


def get_class_instance(module_class_spec: dict):
    module_instance = import_module(module_class_spec["module"])
    return getattr(module_instance, module_class_spec["class"])


def get_constructor_keyword_args(element_arguments: List[str],
                                 element_names: List[str]) -> dict:

    result = defaultdict(dict)
    for argument in element_arguments:
        try:
            argument_coordinate, value = argument.split("=", 1)
            element_name, keyword = argument_coordinate.split(".")
        except:
            raise ValueError(f"Badly formatted element-argument: '{argument}'")

        if element_name not in element_names:
            raise ValueError(f"No pipeline element with name: '{element_name}'")
        result[element_name][keyword] = value
    return result


def evaluate_constructor_args(class_instance: Dict[str, Type[PipelineElement]],
                              element_arguments: Dict
                              ) -> Dict:

    result = defaultdict(dict)
    for element_name, class_instance in class_instance.items():
        for keyword, value in element_arguments[element_name].items():
            value = class_instance.get_keyword_arg_value(keyword, value)
            result[element_name][keyword] = value
    return result

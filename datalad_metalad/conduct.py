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
import logging
import sys
from collections import defaultdict
from importlib import import_module
from itertools import chain
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
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
from dataladmetadatamodel import JSONObject

from .pipelineelement import PipelineElement
from .pipelinedata import PipelineData
from .concurrent.processor import (
    Processor as ConcurrentProcessor,
    ProcessorInterface,
    ProcessorResultType,
)
from .concurrent.queueprocessor import QueueProcessor
from .consumer.base import Consumer
from .provider.base import Provider

from .utils import read_json_object


__docformat__ = 'restructuredtext'

default_metadata_backend = "git"

lgr = logging.getLogger('datalad.metadata.conduct')


class ConductProcessorException(Exception):
    pass


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
        print(element)
        element_kwargs = keyword_arguments[element["name"]]
        element_class = get_class_instance(element)
        error_message = element_class.check_keyword_args(element_kwargs)
        if error_message is not None:
            error_messages.append(error_message)

    if error_messages:
        return "\n".join(error_messages)
    return None


def get_optional_element_instance(element_type: str,
                                  conduct_configuration: JSONObject,
                                  constructor_keyword_args: Dict[str, Dict[str, str]]
                                  ) -> Optional[PipelineElement]:

    element_configuration = conduct_configuration.get(element_type, None)
    if element_configuration:
        element_name = element_configuration["name"]
        return get_class_instance(element_configuration)(
            **{
                **element_configuration["arguments"],
                **constructor_keyword_args[element_name]
            })
    return None


def get_element_instance(element_type: str,
                         conduct_configuration: JSONObject,
                         constructor_keyword_args: Dict[str, Dict[str, str]]
                         ) -> PipelineElement:

    element = get_optional_element_instance(
        element_type=element_type,
        conduct_configuration=conduct_configuration,
        constructor_keyword_args=constructor_keyword_args)

    if element is None:
        raise ValueError(
            f"No element of type {element_type} in pipeline configuration")

    return element


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
            text="Run metalad_core_dataset extractor on the top dataset and "
                 "all subdatasets. Add the resulting metadata in aggregated"
                 "mode. This command uses the provided pipeline"
                 "definition 'extract_metadata'.",
            code_cmd="datalad meta-conduct extract_metadata "
                     "traverser.path=<dataset path> traverser.type=dataset"
                     "traverser.recursive=True extractor.type=dataset "
                     "extractor.extractor_name=metalad_core_dataset "
                     "adder.aggregate=True"
        ),
        dict(
            text="Run metalad_core_file extractor on all files of the root "
                 "dataset and the subdatasets. Automatically get the content, "
                 "if it is not present. Drop content that was automatically "
                 "fetched after its metadata has been added.",
            code_cmd="datalad meta-conduct extract_metadata_autoget_autodrop "
                     "traverser.path=<dataset path> traverser.type=file"
                     "traverser.recursive=True extractor.type=file "
                     "extractor.extractor_name=metalad_core_file "
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
                   "<name>.<key>=<value>". If an optional path has the same
                   structure as a pipeline element argument, the pipeline 
                   element arguments can be terminated by "++"."""),
    )

    @staticmethod
    @datasetmethod(name='meta_conduct')
    @eval_results
    def __call__(
            configuration: Union[str, JSONObject],
            arguments: List[str],
            max_workers: Optional[int] = None,
            processing_mode: str = "process"):

        element_arguments, file_path = split_arguments(arguments, "++")
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

        constructor_keyword_args = get_constructor_keyword_args(
            element_arguments=element_arguments,
            element_names=element_names)

        error_message = check_arguments(constructor_keyword_args, elements)
        if error_message:
            raise ValueError(
                "Pipeline element construction errors:\n"
                f"{error_message}\n")

        provider_instance = cast(Provider, get_element_instance(
            element_type="provider",
            conduct_configuration=conduct_configuration,
            constructor_keyword_args=constructor_keyword_args))

        consumer_instance = cast(Consumer, get_optional_element_instance(
            element_type="consumer",
            conduct_configuration=conduct_configuration,
            constructor_keyword_args=constructor_keyword_args))

        if processing_mode == "thread":
            ConcurrentProcessor.set_thread_executor(max_workers)
            sequential = False
        elif processing_mode == "process":
            ConcurrentProcessor.set_process_executor(max_workers)
            sequential = False
        elif processing_mode == "sequential":
            ConcurrentProcessor.set_thread_executor(max_workers)
            sequential = True
        else:
            raise ValueError(f"Unknown processing mode: {processing_mode}")

        yield from process_parallel(
            provider_instance=provider_instance,
            consumer_instance=consumer_instance,
            conduct_configuration=conduct_configuration,
            constructor_keyword_args=constructor_keyword_args,
            sequential=sequential)


def process_parallel(provider_instance: Provider,
                     consumer_instance: Optional[Consumer],
                     conduct_configuration: JSONObject,
                     constructor_keyword_args: Dict[str, Dict[str, str]],
                     sequential: bool
                     ) -> Iterable:
    """ Execute a configuration

    This method iterates over the provider results and starts a new processor
    queue for each result.

    If a consumer is given, the output of the queue
    is feed into the consumer and the consumer's result is yielded.

    If no consumer is given, the output of the queue is yielded

    :param provider_instance:
    :param consumer_instance:
    :param conduct_configuration:
    :param constructor_keyword_args:
    :param sequential: run everything in this thread (mainly for debugging)
    :return:
    """

    for pipeline_data in provider_instance.next_object():

        # Generate a new queue for the given pipeline data.
        # TODO: check the costs, this has to be done in threads, but not
        #  necessarily in process execution

        queue_processor_element_callables = [
            get_class_instance(spec)(
                **{
                    **spec["arguments"],
                    **constructor_keyword_args[spec["name"]]
                }
            ).process
            for index, spec in enumerate(conduct_configuration["processors"])
        ]

        if not queue_processor_element_callables:
            path = pipeline_data.get_result("path")
            yield dict(
                action="meta_conduct",
                status="ok",
                path=str(path),
                logger=lgr,
                pipeline_data=pipeline_data.to_json())
            continue

        queue_processor = _create_queue_processor_from(
            workers=queue_processor_element_callables)

        queue_processor.start(
            arguments=[pipeline_data],
            result_processor=process_worker_result,
            result_processor_args=[consumer_instance],
            sequential=sequential
        )

        for result in ConcurrentProcessor.done_all(0.0):
            yield result

    for result in ConcurrentProcessor.done_all():
        yield result


def _create_queue_processor_from(workers: List[Callable]) -> QueueProcessor:
    processors = [
        ConcurrentProcessor(worker, f"worker_{index}")
        for index, worker in enumerate(workers)
    ]
    return QueueProcessor(processors=processors, name=f"")


def process_worker_result(sender: ProcessorInterface,
                          result_type: ProcessorResultType,
                          result: PipelineData,
                          consumer: Optional[Consumer]):

    if result_type == ProcessorResultType.Result:
        if consumer is not None:
            result = consumer.consume(result)
        path = result.get_result("path")
        if path is not None:
            return dict(
                action="meta_conduct",
                status="ok",
                path=str(path),
                logger=lgr,
                pipeline_data=result.to_json())
    else:
        print(f"Exception {result}", file=sys.stderr)


def get_class_instance(module_class_spec: dict):
    module_instance = import_module(module_class_spec["module"])
    class_instance = getattr(module_instance, module_class_spec["class"])
    return class_instance


def get_constructor_keyword_args(element_arguments: List[str],
                                 element_names: List[str]) -> dict:

    result = defaultdict(dict)

    for argument in element_arguments:
        argument_coordinate, value = argument.split("=", 1)
        element_name, keyword = argument_coordinate.split(".")
        if element_name not in element_names:
            raise ValueError(f"No pipeline element with name: '{element_name}'")
        result[element_name][keyword] = value
    return result

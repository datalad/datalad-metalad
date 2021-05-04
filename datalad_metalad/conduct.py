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
import sys
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple, Union
from uuid import UUID


from datalad.distribution.dataset import Dataset, datasetmethod
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
from .provider.base import Provider
from .utils import read_json_object


__docformat__ = 'restructuredtext'

default_metadata_backend = "git"

lgr = logging.getLogger('datalad.metadata.conduct')


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
        dataset=Parameter(
            args=("-d", "--dataset"),
            metavar="DATASET",
            doc="""Dataset on which the execution should be conducted"""),
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
                   configuration is read from standard input.""")
    )

    @staticmethod
    @datasetmethod(name='meta_conduct')
    @eval_results
    def __call__(
            configuration: Union[str, JSONObject],
            dataset=None,
            max_workers=None):

        dataset_path = Path(dataset or ".")

        conduct_configuration = read_json_object(configuration)

        provider_instance = get_class_instance(
            conduct_configuration["provider"])(
                *conduct_configuration["provider"]["arguments"],
                **conduct_configuration["provider"]["keyword_arguments"])

        processor_instances = [
            get_class_instance(spec)(
                *spec["arguments"],
                **spec["keyword_arguments"])
            for spec in conduct_configuration["processors"]]

        assert_pipeline_validity(
            provider_instance.output_type(),
            processor_instances)

        executor = concurrent.futures.ProcessPoolExecutor(max_workers)
        running = set()

        for element in provider_instance.next_object():
            running.add(executor.submit(processor_instances[0].execute, -1, element))
            done, running = concurrent.futures.wait(
                running,
                return_when=concurrent.futures.FIRST_COMPLETED,
                timeout=0)

            for future in done:
                index, result = future.result()
                lgr.debug(
                    f"Element[{index}] returned result "
                    f"{result} [provider not yet exhausted]")
                next_index = index + 1
                if next_index >= len(processor_instances):
                    yield dict(
                        action="meta_conduct",
                        status="ok",
                        logger=lgr,
                        path=str(result),
                        result=result)
                else:
                    running.add(
                        executor.submit(
                            processor_instances[next_index].execute,
                            next_index,
                            result))

        while True:
            done, running = concurrent.futures.wait(
                running,
                return_when=concurrent.futures.FIRST_COMPLETED)

            for future in done:
                index, result = future.result()
                lgr.debug(
                    f"Element[{index}] returned result "
                    f"{result} [provider exhausted]")
                next_index = index + 1
                if next_index >= len(processor_instances):
                    yield dict(
                        action="meta_conduct",
                        status="ok",
                        logger=lgr,
                        path=str(result),
                        result=result)
                else:
                    running.add(
                        executor.submit(
                            processor_instances[next_index].execute,
                            next_index,
                            result))

            if not running:
                break

        return


def get_class_instance(module_class_spec: dict):
    module_instance = sys.modules[module_class_spec["module"]]
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

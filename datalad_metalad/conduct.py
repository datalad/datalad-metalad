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
import json
import logging
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple, Union
from uuid import UUID


from .provider.base import Provider
from .processor.base import Processor


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
from dataladmetadatamodel.metadata import MetadataInstance
from dataladmetadatamodel.metadatapath import MetadataPath
from dataladmetadatamodel.metadatarootrecord import MetadataRootRecord
from dataladmetadatamodel.treenode import TreeNode
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList

from .exceptions import NoMetadataStoreFound
from .metadata import get_top_level_metadata_objects
from .pathutils.metadataurlparser import (
    MetadataURLParser,
    TreeMetadataURL,
    UUIDMetadataURL
)


__docformat__ = 'restructuredtext'

default_metadata_backend = "git"

lgr = logging.getLogger('datalad.metadata.conduct')


###########################################################

class FilesystemTraverser(Provider):
    def __init__(self, file_system_object: Union[str, Path]):
        super().__init__(file_system_object)
        self.file_system_objects = [Path(file_system_object)]

    def next_object(self):
        while self.file_system_objects:
            file_system_object = self.file_system_objects.pop()
            if file_system_object.is_symlink():
                continue
            elif file_system_object.is_dir():
                self.file_system_objects.extend(list(file_system_object.glob("*")))
            else:
                yield file_system_object

    @staticmethod
    def output_type() -> str:
        return "pathlib.Path"


class StringEater(Processor):
    def __init__(self):
        super().__init__()

    def process(self, path: Path) -> Path:
        if path.parts:
            return Path().joinpath(*(path.parts[1:]))
        return path

    @staticmethod
    def input_type() -> str:
        return "pathlib.Path"

    @staticmethod
    def output_type() -> str:
        return "pathlib.Path"



###########################################################


@build_doc
class Conduct(Interface):
    """Conduct the execution of a processing pipeline

    Processing pipelines consist of:

    - A provider, that provides entities that should be processed

    - A list of processors. A processor read entities,
      either from the previous processor or the providerm and performs
      a computation on the entity. The computation may have side-effect,
      e.g. store metadata, and yields a result that is processed by
      the next processor.

    Processors are usually executed in concurrent processes, i.e. workers.
    The maximum number of workers is given by the parameter `max_workers`.
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
        provider=Parameter(
            args=("provider",),
            metavar="PROVIDER",
            doc="provider instance",
            nargs=1),
        processors=Parameter(
            #args=("processors",),
            metavar="PROCESSORS",
            doc="processor classes",
            nargs='+'),
    )

    @staticmethod
    @datasetmethod(name='meta_conduct')
    @eval_results
    def __call__(
            provider,
            processors,
            dataset=None,
            max_workers=None):

        dataset_path = Path(dataset or ".")

        provider = FilesystemTraverser(dataset_path)
        processors = [StringEater() for i in range(5)]

        executor = concurrent.futures.ProcessPoolExecutor(max_workers)
        running = set()

        for element in provider.next_object():
            running.add(executor.submit(processors[0].execute, -1, element))
            done, running = concurrent.futures.wait(
                running,
                return_when=concurrent.futures.FIRST_COMPLETED,
                timeout=0)

            for future in done:
                index, result = future.result()
                print(f"E[{index}]: {result}")
                next_index = index + 1
                if next_index >= len(processors):
                    yield dict(
                        action="meta_conduct",
                        status="ok",
                        logger=lgr,
                        result=result)
                else:
                    running.add(
                        executor.submit(processors[next_index].execute, next_index, result))

        while True:
            done, running = concurrent.futures.wait(
                running,
                return_when=concurrent.futures.FIRST_COMPLETED)

            for future in done:
                index, result = future.result()
                print(f"L[{index}]: {result}")
                next_index = index + 1
                if next_index >= len(processors):
                    yield dict(
                        action="meta_conduct",
                        status="ok",
                        logger=lgr,
                        result=result)
                else:
                    running.add(
                        executor.submit(processors[next_index].execute, next_index, result))

            if not running:
                break

        return

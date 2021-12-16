import abc
import queue
from typing import (
    Any,
    Optional,
)

from ..pipelineelement import PipelineElement


__docformat__ = "restructuredtext"


class Consumer(metaclass=abc.ABCMeta):
    """ A sub-process consumer for conduct.

    Consumer provide an access point, usually a queue or similar, on which
    they consume data. An example is a metadata-adder that receives metadata
    from a queue and adds it to a datastore.
    """

    @abc.abstractmethod
    def consume(self, pipeline_element: PipelineElement) -> bool:
        """ Consume the pipeline element.

        Consume the pipeline element and return `True` if it was successfully
        consumed, `False` otherwise.

        Overwrite this method in derived classes to implement the functionality
        of the consumer.

        :param PipelineElement pipeline_element: The pipeline element that
            shall be consumed.
        :return: Return `True` if the element was consumed, `False` otherwise
        :rtype: bool
        """
        raise NotImplementedError

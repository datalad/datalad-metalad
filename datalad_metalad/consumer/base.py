import abc

from ..pipelinedata import PipelineData
from ..pipelineelement import PipelineElement


__docformat__ = "restructuredtext"


class Consumer(PipelineElement, metaclass=abc.ABCMeta):
    """ A sub-process consumer for conduct.

    Consumer provide an access point, usually a queue or similar, on which
    they consume data. An example is a metadata-adder that receives metadata
    from a queue and adds it to a datastore.
    """

    @abc.abstractmethod
    def consume(self, pipeline_data: PipelineData) -> bool:
        """ Consume the pipeline data.

        Consume the pipeline data and return `True` if it was successfully
        consumed, `False` otherwise.

        Overwrite this method in derived classes to implement the functionality
        of the consumer.

        :param PipelineData pipeline_data: The pipeline data that
            shall be consumed.
        :return: Return `True` if the element was consumed, `False` otherwise
        :rtype: bool
        """
        raise NotImplementedError

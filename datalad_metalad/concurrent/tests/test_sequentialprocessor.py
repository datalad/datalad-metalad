import concurrent.futures
import os
from pprint import pprint
from typing import Dict

from ..processor import FutureSet
from ..sequentialprocessor import ProcessorSequence


def add_component(pe: Dict) -> Dict:
    if "name_list" not in pe:
        pe["name_list"] = "x"
    else:
        pe["name_list"] += " x"
    print(os.getpid(), pe)
    return pe


def test_basics():
    executor = concurrent.futures.ProcessPoolExecutor(10)
    result = dict()
    future_set = FutureSet()
    processors = [add_component] * 4

    seq = ProcessorSequence(
        processors=processors,
        initial_result_object=result,
        executor=executor,
        future_set=future_set
    )

    print("main:", os.getpid())
    seq.start()

    for done_future, done_processor in future_set.done:
        result = done_processor.process_done(done_future)
        if result is not None:
            pprint(result)

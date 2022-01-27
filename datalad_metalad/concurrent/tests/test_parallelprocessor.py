import concurrent.futures
import os
import time
from pprint import pprint
from typing import Dict

from ..processor import FutureSet
from ..parallelprocessor import ProcessorParallel


def add_component(pe: Dict) -> Dict:
    if "name_list" not in pe:
        pe["name_list"] = f"{os.getpid()} {time.time()}"
    else:
        pe["name_list"] += " x"
    print(os.getpid(), pe)
    return pe


def test_basics():
    executor = concurrent.futures.ProcessPoolExecutor(10)
    result = dict()
    future_set = FutureSet()
    processors = [add_component] * 20

    par = ProcessorParallel(
        processors=processors,
        initial_result_object=result,
        executor=executor,
        future_set=future_set
    )

    print("main:", os.getpid())
    par.start()

    for done_future, done_processor in future_set.done:
        result = done_processor.process_done(done_future)
        if result is not None:
            pprint(result)

import logging
from functools import partialmethod
from typing import (
    Callable,
    Optional,
)

import datalad.runner.gitrunner as git_runner
from datalad.runner.protocol import GeneratorMixIn
from .commandserviceclient import execute


logger = logging.getLogger("datalad.runner.cached")
original_run: Optional[Callable] = None


def _cached_run(self,
                port,
                cmd,
                protocol=None,
                stdin=None,
                cwd=None,
                env=None,
                timeout=None,
                exception_on_error=True,
                **kwargs):

    # The stdin check should actually be:
    # `stdin not isinstance(stdin, (str, bytes, type(None))):`
    # But then we should implement sending str or byte stdin to the server
    if (
            issubclass(protocol, GeneratorMixIn)
            or not isinstance(stdin, (str, bytes, type(None)))):

        logger.debug(
            "calling original run due to a generator protocol or a non static "
            "stdin value"
        )
        return original_run(
            self, cmd, protocol, stdin, cwd, env, timeout, exception_on_error,
            **kwargs
        )

    cwd = cwd or self.cwd
    env = self._get_adjusted_env(env or self.env, cwd=cwd)
    logger.debug(f"calling http://localhost:{port}/command")
    return execute(
        port,
        cmd=cmd,
        stdin=stdin,
        workdir=cwd,
        environment=env
    )


def patch_git_runner(port: int):
    global original_run
    if original_run is None:
        original_run = git_runner.GitWitlessRunner.run
        git_runner.GitWitlessRunner.run = partialmethod(_cached_run, port)


def unpatch_git_runner():
    global original_run
    if original_run is not None:
        git_runner.GitWitlessRunner._get_chunked_results = original_run
        original_run = None

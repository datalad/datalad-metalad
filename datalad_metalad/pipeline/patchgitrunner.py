import logging
from functools import partialmethod
from typing import (
    Callable,
    Optional,
)

import datalad.runner.gitrunner as git_runner
from datalad.runner.nonasyncrunner import (
    STDERR_FILENO,
    STDOUT_FILENO,
)
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
                enforce_local=False,
                **kwargs):

    if enforce_local or not isinstance(stdin, (str, bytes, type(None))):
        logger.debug(
            "calling original run because `enforce_local` is True"
            if enforce_local
            else "calling original run due to a non-static stdin value"
        )
        return original_run(
            self, cmd, protocol, stdin, cwd, env, timeout, exception_on_error,
            **kwargs
        )

    cwd = cwd or self.cwd
    env = self._get_adjusted_env(env or self.env, cwd=cwd)
    logger.debug(f"calling http://localhost:{port}/command")
    result = execute(
        port,
        cmd=cmd,
        stdin=stdin,
        workdir=cwd,
        environment=env
    )
    if issubclass(protocol, GeneratorMixIn):
        return [
            (file_no, result[key])
            for key, file_no in (("stderr", STDERR_FILENO), ("stdout", STDOUT_FILENO))
            if key in result
        ]
    return result


def patch_git_runner(port: int):
    global original_run
    if original_run is None:
        original_run = git_runner.GitWitlessRunner.run
        git_runner.GitWitlessRunner.run = partialmethod(_cached_run, port)


def unpatch_git_runner():
    global original_run
    if original_run is not None:
        git_runner.GitWitlessRunner.run = original_run
        original_run = None

import json
import os
import sys
from typing import (
    Dict,
    List,
    Optional,
    Union,
)

import requests
from datalad.runner.exception import CommandError

from .commandservice import (
    header_name,
    token_variable_name,
)


caching_command_service_url = 'http://localhost:{port}/command'


class RemoteCommandError(CommandError):
    def __init__(self,
                 port: int,
                 status: int,
                 msg: str,
                 cmd: str,
                 environment: Optional[Dict]
                 ) -> None:

        CommandError.__init__(self, cmd=cmd, msg=msg)
        self.port = port
        self.status = status
        self.token_info = ""
        if environment and token_variable_name in environment:
            self.token_info = f"{token_variable_name} in provided environment"
        else:
            self.token_info = f"{token_variable_name} not in provided environment"
        if token_variable_name in os.environ:
            self.token_info += f" {token_variable_name} in os.environ"
        else:
            self.token_info = f"{token_variable_name} not in os.environ"

    def to_str(self, include_output=True):
        real_url = caching_command_service_url.format(port=self.port)
        return \
            f"remote call to {real_url} failed with {self.status} {self.msg}" \
            f", command was {self.cmd}, {self.token_info}"

    def __str__(self):
        return self.to_str()


def execute(port: int,
            cmd: List[str],
            stdin: Optional[Union[str, bytes]],
            workdir: str,
            environment: Optional[Dict] = None
            ) -> Dict:

    token = os.environ.get(token_variable_name, None)
    if token is None:
        token = (environment or {}).get(token_variable_name, None)

    request_result = requests.post(
        caching_command_service_url.format(port=port),
        headers={header_name: token} if token else {},
        json={
            "cmd": cmd,
            "stdin": stdin,
            "cwd": workdir,
            "env": environment
        })
    if request_result.status_code < 200 or request_result.status_code >= 300:
        raise RemoteCommandError(
            port=port,
            status=request_result.status_code,
            msg=request_result.reason,
            cmd=str(cmd),
            environment=environment
        )
    result = json.loads(request_result.content.decode("utf-8"))
    code, stdout, stderr = result["code"], result["stdout"], result["stderr"]
    if code != 0:
        raise CommandError(
            cmd=cmd,
            code=code,
            stdout=stdout,
            stderr=stderr,
            cwd=workdir
        )
    if "--json" in cmd and "annex" in cmd:
        # Assume annex was called with --json flag. In this case `stdout` should
        # contain lines with one serialized json object per line.
        result["stdout_json"] = [
            json.loads(line)
            for line in stdout.splitlines()
        ]
        result["stdout"] = ""
    return result


if __name__ == "__main__":
    print(execute(int(sys.argv[1]), sys.argv[2:], None, os.getcwd()))

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


caching_command_service_url = 'http://localhost:{port}/command'


def execute(port: int,
            cmd: List[str],
            stdin: Optional[Union[str, bytes]],
            workdir: str,
            environment: Optional[Dict] = None
            ) -> Dict:
    request_result = requests.post(
        caching_command_service_url.format(port=port),
        json={
            "cmd": cmd,
            "stdin": stdin,
            "cwd": workdir,
            "env": environment
        })
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

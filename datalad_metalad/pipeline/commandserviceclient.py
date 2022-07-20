import json
import os
from typing import (
    Dict,
    List,
    Optional,
)

import requests
from datalad.runner.exception import CommandError


caching_command_service_url = 'http://localhost:{port}/command'


def execute(port: int,
            cmd: List[str],
            workdir: str,
            environment: Optional[Dict] = None
            ) -> Dict:
    request_result = requests.post(
        caching_command_service_url.format(port=port),
        json={
            "cmd": cmd,
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
        # contain a serialized json object.
        result["stdout_json"] = [json.loads(stdout)]
        result["stdout"] = ""
    return result


if __name__ == "__main__":
    import sys
    execute(int(sys.argv[1]), sys.argv[2:], os.getcwd())

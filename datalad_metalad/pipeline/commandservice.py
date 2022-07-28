""" This an experimental caching command server

 This server aims at consolidating identical command line calls, by
 executing them once and caching the result.
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import subprocess
import sys
import threading
from http.server import (
    BaseHTTPRequestHandler,
    ThreadingHTTPServer,
)

import requests
from progress.bar import IncrementalBar

from ..metadatatypes import JSONType
from .meter import Meter


port = None

logger = logging.getLogger("datalad.commandservice")

executor = concurrent.futures.ProcessPoolExecutor(max_workers=1)

token_variable_name = "DATALAD_CCS_TOKEN"
header_name = "x-datalad-authorization"


def authorize_request(function):
    def _inner_function(request_handler):
        if request_handler.client_address[0] != '127.0.0.1':
            request_handler.send_response(401)
            request_handler.end_headers()
            return
        if CommandRequestHandler.verify_token:
            if request_handler.headers.get(header_name, True) \
                    != os.environ.get(token_variable_name, False):
                logger.debug("No token provided or token did not match")
                request_handler.send_response(401)
                request_handler.end_headers()
                return
        return function(request_handler)
    return _inner_function


class CommandRequestHandler(BaseHTTPRequestHandler):

    cache = dict()
    cache_clearing_requested = False
    active_connections = 0
    lock = threading.Lock()
    verify_token = True

    def __init__(self, *args, **kwargs):
        self.route = {
            "/exit": self.handle_exit,
            "/clear": self.handle_clear_cache,
            "/command": self.handle_command
        }
        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def setup(self) -> None:
        with CommandRequestHandler.lock:
            CommandRequestHandler.active_connections += 1
            meter.goto(CommandRequestHandler.active_connections)
        BaseHTTPRequestHandler.setup(self)

    def finish(self) -> None:
        with CommandRequestHandler.lock:
            CommandRequestHandler.active_connections -= 1
            meter.goto(CommandRequestHandler.active_connections)
        BaseHTTPRequestHandler.finish(self)

    @authorize_request
    def do_GET(self):
        if self.path in self.route:
            self.route[self.path]()

    @authorize_request
    def do_POST(self):
        if self.path in self.route:
            if self.headers.get("clear-cache", None):
                with self.lock:
                    CommandRequestHandler.cache = dict()
            content_len = int(self.headers.get("content-length", 0))
            content = self.rfile.read(content_len).decode("utf-8")
            results = self.route[self.path](content)
            self.send_result(*results)

    def send_result(self, code: int, message: str, content: bytes):
        self.send_response(code, message)
        self.send_header("Host", f"localhost:{self.server.server_port}")
        self.send_header("Content-length", str(len(content)))
        self.send_header("Content-type", "text/json;charset=utf-8")
        self.end_headers()
        self.wfile.write(content)

    def handle_exit(self):
        self.server.shutdown()

    def handle_clear_cache(self):
        CommandRequestHandler.cache = dict()

    def handle_command(self, json_string: str) -> tuple[int, str, bytes]:
        with self.lock:
            if json_string in self.cache:
                command_spec, result = self.cache[json_string]
                logging.debug(
                    f"return cached result for command: {command_spec['cmd']}")
                return 200, "OK (cached)", result

        command_spec = json.loads(json_string)
        logging.debug(f"running command: {command_spec['cmd']}")
        result_dict = self.run_command(command_spec)
        result_content = json.dumps(result_dict).encode("utf-8")
        with self.lock:
            self.cache[json_string] = command_spec, result_content
        return 200, "OK", result_content

    def run_command(self, command_spec: JSONType) -> JSONType:
        completed_result = subprocess.run(
            command_spec["cmd"],
            cwd=command_spec["cwd"],
            env=command_spec["env"],
            stdin=command_spec.get("stdin", None),
            capture_output=True
        )
        return {
            "cmd": command_spec["cmd"],
            "code": completed_result.returncode,
            "stdout": completed_result.stdout.decode("utf-8", "backslashreplace"),
            "stderr": completed_result.stderr.decode("utf-8", "backslashreplace")
        }


def get_server(requested_port: int = 0) -> ThreadingHTTPServer:
    return ThreadingHTTPServer(
        ("127.0.0.1", requested_port),
        CommandRequestHandler
    )


def start_server(requested_port: int = 0) -> int:
    popen = subprocess.Popen(
        args=["python", __file__, str(requested_port)],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1
    )
    assigned_port = popen.stdout.readline()
    return int(assigned_port)


def stop_server(port: int):
    request_result = requests.get(f"http://localhost:{port}/exit"),


if __name__ == "__main__":
    if "--dont-verify-token" in sys.argv:
        sys.argv.remove("--dont-verify-token")
        CommandRequestHandler.verify_token = False
    requested_port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    http_server = ThreadingHTTPServer(
        ("127.0.0.1", requested_port),
        CommandRequestHandler
    )

    if True:
        meter = Meter(label="Current clients: ")
    else:
        meter = IncrementalBar(
            "Current clients_ ",
            suffix='%(index)d',
            max=10,
            file=sys.stdout
        )

    port = http_server.server_port
    print(port)

    try:
        logging.basicConfig(level=logging.DEBUG)
        meter.goto(0)
        http_server.serve_forever()
    finally:
        port = None

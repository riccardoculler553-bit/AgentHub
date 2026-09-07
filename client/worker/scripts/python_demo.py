"""Pre-registered demo script for the `python.demo` command.

Contract for all pre-registered worker scripts:
- receive task params as:  --params-json '<json>'
- print ONE JSON object line to stdout as the task result
- non-zero exit code -> EXECUTOR_FAILED on the server side
"""

import argparse
import json
import platform
import socket
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--params-json", default="{}")
    args = parser.parse_args()
    try:
        params = json.loads(args.params_json)
    except ValueError:
        params = {}

    print(
        json.dumps(
            {
                "hello": params.get("hello", "world"),
                "host": socket.gethostname(),
                "platform": platform.platform(),
                "python": sys.version.split()[0],
                "received_params": params,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import subprocess
import time
from adagio.backend.agent.protocol import RPCHandlerContext, serve_rpc_loop


def ping(*, ctx: RPCHandlerContext, message: str = "pong") -> dict[str, str]:
    ctx.emit("countdown", message=f"3!")
    time.sleep(1)
    ctx.emit("countdown", message=f"2!!")
    time.sleep(1)
    ctx.emit("countdown", message=f"1!!!")
    return {"pong": message}


def run_shell(*, ctx: RPCHandlerContext, cmd: str) -> dict[str, object]:
    ctx.emit("progress", message=f"run_shell starting: {cmd}")
    proc = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=False)
    ctx.emit("progress", message=f"run_shell returncode={proc.returncode}")
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def main():
    serve_rpc_loop({"ping": ping, "run_shell": run_shell})

from __future__ import annotations

import subprocess

from adagio.backend import RPCHandlerContext, serve_rpc_forever


def ping(*, ctx: RPCHandlerContext, message: str = "pong") -> dict[str, str]:
    ctx.emit("progress", message=f"ping called with: {message}")
    return {"message": message}


def run_shell(*, ctx: RPCHandlerContext, cmd: str) -> dict[str, object]:
    ctx.emit("progress", message=f"run_shell starting: {cmd}")
    proc = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=False)
    ctx.emit("progress", message=f"run_shell returncode={proc.returncode}")
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


if __name__ == "__main__":
    serve_rpc_forever(
        {
            "ping": ping,
            "run_shell": run_shell,
        }
    )

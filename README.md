# Adagio

## Installation

```bash
pip install .
```

## Usage

```bash
adagio --version
adagio install
adagio install --apply
adagio dispatch --command "flux run hostname"
```

```python
from adagio.backend import FluxRPCSession
from pathlib import Path

with FluxRPCSession(
    workdir=Path("."),
) as session:
    session.subscribe(lambda ev: print("EVENT", ev.event_type, ev.payload))
    session.subscribe(
        lambda ev: print("PROGRESS", ev.payload.get("message")),
        event_type="progress",
    )
    future = session.call("ping", message="hello from host")
    result = future.result(timeout=30)
    print(result)
```

`FluxRPCSession` now launches a bundled `rpc-agent.pyz` by default, so `adagio` does not need to be installed inside the container/VM (the image only needs `python3`).

In the agent handler, accept `ctx` and emit events:

```python
def ping(*, ctx, message: str = "pong"):
    ctx.emit("progress", message=f"ping: {message}")
    return {"message": message}
```

## Development

```bash
uv sync
source .venv/bin/activate
```

"""Start the API and dashboard, moving to the next free port when the asked one is taken.

uv run python -m app.serve --port 8000
"""

import argparse
import socket

import uvicorn

PORTS_TO_TRY = 20


def _answers(host: str, port: int) -> bool:
    """True when something already accepts connections on `port`."""
    try:
        with socket.create_connection((host, port), timeout=0.3):
            return True
    except ConnectionRefusedError:
        return False
    except OSError:
        return True  # no clear "refused": treat the port as taken


def open_listener(host: str, first_port: int, attempts: int = PORTS_TO_TRY) -> socket.socket:
    """Bind the first free port from `first_port` on and return the bound socket.

    Checking for a listener first matters on macOS, where a bind can succeed next to a server that
    listens on all addresses. Handing the bound socket to uvicorn leaves no gap for another
    process to take the port in between.

    Raises:
        SystemExit: every port in the range is taken.
    """
    for port in range(first_port, first_port + attempts):
        if _answers(host, port):
            continue
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((host, port))
        except OSError:
            listener.close()
            continue
        return listener
    raise SystemExit(
        f"Ports {first_port} to {first_port + attempts - 1} are all in use. Pick another: make run PORT=9000"
    )


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m app.serve", description=__doc__.split("\n\n")[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000, help="first port to try (default 8000)")
    args = parser.parse_args()

    listener = open_listener(args.host, args.port)
    port = listener.getsockname()[1]
    if port != args.port:
        print(f"Port {args.port} is in use, so the server uses port {port} instead.", flush=True)
    print(
        f"Dashboard: http://{args.host}:{port}  Trade tracker: http://{args.host}:{port}/tracker", flush=True
    )
    server = uvicorn.Server(uvicorn.Config("app.main:app", host=args.host, port=port))
    server.run(sockets=[listener])


if __name__ == "__main__":
    main()

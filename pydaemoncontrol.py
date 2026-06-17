#!/usr/bin/env python3
"""PyDaemonControl: a small directory-scoped process controller.

One daemon owns one root directory. Clients are one-shot commands that talk to
the daemon over a Unix domain socket.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - this tool is intended for Linux hosts.
    fcntl = None


DEFAULT_MAX_LOG_BYTES = 32 * 1024 * 1024
DEFAULT_ROTATE_COUNT = 3
DEFAULT_TAIL_CHARS = 256 * 1024


class RpcError(Exception):
    pass


def now_stamp() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def sanitize_name(name: str) -> str:
    if not name:
        raise RpcError("process name is empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    if any(ch not in allowed for ch in name):
        raise RpcError("process name may only contain letters, digits, dot, dash and underscore")
    return name


def state_paths(root: Path) -> dict[str, Path]:
    state = root / ".pydaemoncontrol"
    return {
        "state": state,
        "logs": state / "logs",
        "socket": state / "daemon.sock",
        "pid": state / "daemon.pid",
        "lock": state / "daemon.lock",
        "daemon_log": state / "daemon.log",
    }


def encode_response(data: dict[str, Any]) -> bytes:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode_request(raw: bytes) -> dict[str, Any]:
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RpcError(f"invalid request json: {exc}") from exc
    if not isinstance(data, dict):
        raise RpcError("request must be a json object")
    return data


def write_daemon_log(paths: dict[str, Path], message: str) -> None:
    line = f"[{now_stamp()} daemon] {message}\n"
    with paths["daemon_log"].open("a", encoding="utf-8") as fh:
        fh.write(line)


@dataclass
class HostedProcess:
    name: str
    argv: list[str]
    cwd: Path
    log_path: Path
    max_log_bytes: int = DEFAULT_MAX_LOG_BYTES
    rotate_count: int = DEFAULT_ROTATE_COUNT
    tail_chars: int = DEFAULT_TAIL_CHARS
    proc: subprocess.Popen[bytes] | None = None
    started_at: float = field(default_factory=time.time)
    exited_at: float | None = None
    exit_code: int | None = None
    bytes_written: int = 0
    current_base_offset: int = 0
    _fd: int | None = None
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _cond: threading.Condition = field(init=False)
    _tail: deque[str] = field(default_factory=deque)
    _tail_len: int = 0

    def __post_init__(self) -> None:
        self._cond = threading.Condition(self._lock)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.log_path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
        self.bytes_written = self.log_path.stat().st_size if self.log_path.exists() else 0

    def close_log(self) -> None:
        with self._lock:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def status(self) -> dict[str, Any]:
        pid = self.proc.pid if self.proc is not None else None
        return {
            "name": self.name,
            "pid": pid,
            "running": self.running,
            "exitCode": self.proc.poll() if self.proc is not None else self.exit_code,
            "startedAt": self.started_at,
            "exitedAt": self.exited_at,
            "cwd": str(self.cwd),
            "argv": self.argv,
            "logPath": str(self.log_path),
            "bytesWritten": self.bytes_written,
            "currentBaseOffset": self.current_base_offset,
        }

    def start(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            raise RpcError(f"{self.name} is already running")
        self.started_at = time.time()
        self.exited_at = None
        self.exit_code = None
        self.write_event("system", "starting: " + " ".join(self.argv))
        self.proc = subprocess.Popen(
            self.argv,
            cwd=str(self.cwd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            start_new_session=True,
        )
        threading.Thread(target=self._reader, args=("stdout", self.proc.stdout), daemon=True).start()
        threading.Thread(target=self._reader, args=("stderr", self.proc.stderr), daemon=True).start()
        threading.Thread(target=self._monitor, daemon=True).start()

    def _reader(self, stream_name: str, stream: Any) -> None:
        try:
            while True:
                chunk = stream.read(65536)
                if not chunk:
                    break
                self.write_chunk(stream_name, chunk)
        except Exception as exc:
            self.write_event("system", f"{stream_name} reader failed: {exc}")

    def _monitor(self) -> None:
        assert self.proc is not None
        code = self.proc.wait()
        self.exit_code = code
        self.exited_at = time.time()
        self.write_event("system", f"process exited with code {code}")

    def _rotate_if_needed(self, incoming_len: int) -> None:
        if self.max_log_bytes <= 0:
            return
        try:
            current_size = self.log_path.stat().st_size if self.log_path.exists() else 0
        except FileNotFoundError:
            current_size = 0
        if current_size + incoming_len <= self.max_log_bytes:
            return
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        for index in range(self.rotate_count, 0, -1):
            src = self.log_path.with_name(self.log_path.name + ("" if index == 1 else f".{index - 1}"))
            dst = self.log_path.with_name(self.log_path.name + f".{index}")
            if index == 1:
                src = self.log_path
            if src.exists():
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
        self.current_base_offset = self.bytes_written
        self._fd = os.open(self.log_path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)

    def _add_tail(self, text: str) -> None:
        if not text:
            return
        self._tail.append(text)
        self._tail_len += len(text)
        while self._tail_len > self.tail_chars and self._tail:
            removed = self._tail.popleft()
            self._tail_len -= len(removed)

    def write_event(self, stream_name: str, text: str) -> None:
        self.write_chunk(stream_name, (text.rstrip("\n") + "\n").encode("utf-8", errors="replace"))

    def write_chunk(self, stream_name: str, chunk: bytes) -> None:
        prefix = f"[{now_stamp()} {stream_name}] ".encode("utf-8")
        data = prefix + chunk.replace(b"\n", b"\n" + prefix)
        if data.endswith(prefix):
            data = data[: -len(prefix)]
        with self._cond:
            self._rotate_if_needed(len(data))
            if self._fd is None:
                self._fd = os.open(self.log_path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
            os.write(self._fd, data)
            self.bytes_written += len(data)
            self._add_tail(data.decode("utf-8", errors="replace"))
            self._cond.notify_all()

    def read_tail(self, max_bytes: int) -> str:
        max_bytes = max(1, max_bytes)
        with self._lock:
            joined = "".join(self._tail)
        raw = joined.encode("utf-8", errors="replace")
        return raw[-max_bytes:].decode("utf-8", errors="replace")

    def read_since(self, offset: int, max_bytes: int) -> str:
        max_bytes = max(1, max_bytes)
        with self._lock:
            base = self.current_base_offset
            written = self.bytes_written
        if offset < base:
            return "[pydaemoncontrol] requested offset was rotated; showing recent output\n" + self.read_tail(max_bytes)
        if offset >= written:
            return ""
        local_offset = offset - base
        try:
            with self.log_path.open("rb") as fh:
                fh.seek(local_offset)
                raw = fh.read(max_bytes)
        except FileNotFoundError:
            return ""
        return raw.decode("utf-8", errors="replace")

    def wait_for_output_after(self, offset: int, timeout: float, quiet: float = 0.2) -> None:
        end = time.time() + max(0.0, timeout)
        seen = False
        last_written = offset
        with self._cond:
            while time.time() < end:
                remaining = end - time.time()
                if remaining <= 0:
                    break
                self._cond.wait(min(quiet, remaining))
                if self.bytes_written > last_written:
                    seen = True
                    last_written = self.bytes_written
                    continue
                if seen:
                    break

    def send(self, text: str, append_newline: bool, wait: float, max_bytes: int) -> dict[str, Any]:
        if not self.running or self.proc is None or self.proc.stdin is None:
            raise RpcError(f"{self.name} is not running")
        payload = text + ("\n" if append_newline else "")
        with self._lock:
            offset = self.bytes_written
        self.proc.stdin.write(payload.encode("utf-8"))
        self.proc.stdin.flush()
        self.write_event("stdin", payload.rstrip("\n"))
        if wait > 0:
            self.wait_for_output_after(offset, wait)
        return {"offset": offset, "output": self.read_since(offset, max_bytes)}

    def stop(self, grace: float) -> dict[str, Any]:
        if not self.running or self.proc is None:
            return {"stopped": False, "reason": "not running", "status": self.status()}
        pid = self.proc.pid
        self.write_event("system", f"stopping process group {pid}")
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.time() + max(0.0, grace)
        while time.time() < deadline:
            if self.proc.poll() is not None:
                break
            time.sleep(0.05)
        if self.proc.poll() is None:
            self.write_event("system", f"killing process group {pid}")
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.proc.wait(timeout=3)
        return {"stopped": True, "status": self.status()}


class ProcHostDaemon:
    def __init__(self, root: Path, max_log_bytes: int, rotate_count: int) -> None:
        self.root = root.resolve()
        self.paths = state_paths(self.root)
        self.max_log_bytes = max_log_bytes
        self.rotate_count = rotate_count
        self.processes: dict[str, HostedProcess] = {}
        self._shutdown = threading.Event()
        self._lock_fh: Any = None

    def acquire_lock(self) -> None:
        if fcntl is None:
            raise RpcError("fcntl is required for daemon locking")
        self.paths["state"].mkdir(parents=True, exist_ok=True)
        self.paths["logs"].mkdir(parents=True, exist_ok=True)
        self._lock_fh = self.paths["lock"].open("w")
        try:
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RpcError(f"daemon lock is already held for {self.root}") from exc
        self.paths["pid"].write_text(str(os.getpid()) + "\n", encoding="utf-8")

    def serve(self) -> int:
        self.acquire_lock()
        sock_path = self.paths["socket"]
        if sock_path.exists():
            sock_path.unlink()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(sock_path))
        os.chmod(sock_path, 0o600)
        server.listen(16)
        server.settimeout(0.5)
        write_daemon_log(self.paths, f"daemon started pid={os.getpid()} root={self.root}")
        try:
            while not self._shutdown.is_set():
                try:
                    conn, _addr = server.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()
        finally:
            write_daemon_log(self.paths, "daemon stopping")
            for entry in list(self.processes.values()):
                entry.close_log()
            server.close()
            if sock_path.exists():
                sock_path.unlink()
        return 0

    def _handle_conn(self, conn: socket.socket) -> None:
        with conn:
            try:
                raw = b""
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    raw += chunk
                    if len(raw) > 8 * 1024 * 1024:
                        raise RpcError("request too large")
                response = {"ok": True, "data": self.handle(decode_request(raw))}
            except Exception as exc:
                response = {"ok": False, "error": str(exc)}
            conn.sendall(encode_response(response))

    def get_process(self, name: str) -> HostedProcess:
        name = sanitize_name(name)
        entry = self.processes.get(name)
        if entry is None:
            raise RpcError(f"unknown process: {name}")
        return entry

    def handle(self, req: dict[str, Any]) -> dict[str, Any]:
        action = req.get("action")
        if action == "ping":
            return {"pid": os.getpid(), "root": str(self.root)}
        if action == "status":
            return {
                "daemonPid": os.getpid(),
                "root": str(self.root),
                "processes": {name: proc.status() for name, proc in self.processes.items()},
            }
        if action == "start":
            return self._start(req, replace=False)
        if action == "restart":
            name = sanitize_name(str(req.get("name", "")))
            if name in self.processes:
                self.processes[name].stop(float(req.get("grace", 5.0)))
            return self._start(req, replace=True)
        if action == "send":
            entry = self.get_process(str(req.get("name", "")))
            return entry.send(
                str(req.get("text", "")),
                bool(req.get("newline", True)),
                float(req.get("wait", 1.0)),
                int(req.get("maxBytes", 65536)),
            )
        if action == "tail":
            entry = self.get_process(str(req.get("name", "")))
            return {"output": entry.read_tail(int(req.get("maxBytes", 65536))), "status": entry.status()}
        if action == "stop":
            entry = self.get_process(str(req.get("name", "")))
            return entry.stop(float(req.get("grace", 5.0)))
        if action == "daemon-stop":
            if req.get("stopChildren", False):
                for entry in list(self.processes.values()):
                    entry.stop(float(req.get("grace", 5.0)))
            self._shutdown.set()
            return {"stopping": True, "daemonPid": os.getpid()}
        raise RpcError(f"unknown action: {action}")

    def _start(self, req: dict[str, Any], replace: bool) -> dict[str, Any]:
        name = sanitize_name(str(req.get("name", "")))
        argv = req.get("argv")
        if not isinstance(argv, list) or not argv or not all(isinstance(x, str) for x in argv):
            raise RpcError("argv must be a non-empty list of strings")
        cwd_raw = req.get("cwd")
        cwd = Path(cwd_raw).resolve() if cwd_raw else self.root
        if name in self.processes and self.processes[name].running and not replace:
            raise RpcError(f"{name} is already running")
        log_path = self.paths["logs"] / f"{name}.log"
        entry = HostedProcess(
            name=name,
            argv=argv,
            cwd=cwd,
            log_path=log_path,
            max_log_bytes=int(req.get("maxLogBytes", self.max_log_bytes)),
            rotate_count=int(req.get("rotateCount", self.rotate_count)),
        )
        self.processes[name] = entry
        entry.start()
        return {"started": True, "status": entry.status()}


class ProcHostClient:
    def __init__(self, root: Path, script: Path, timeout: float) -> None:
        self.root = root.resolve()
        self.script = script.resolve()
        self.paths = state_paths(self.root)
        self.timeout = timeout

    def daemon_running(self) -> bool:
        try:
            self.request({"action": "ping"}, timeout=0.5)
            return True
        except Exception:
            return False

    def start_daemon(self, max_log_bytes: int, rotate_count: int) -> dict[str, Any]:
        self.paths["state"].mkdir(parents=True, exist_ok=True)
        if self.daemon_running():
            return self.request({"action": "ping"})
        args = [
            sys.executable,
            str(self.script),
            "--root",
            str(self.root),
            "--max-log-bytes",
            str(max_log_bytes),
            "--rotate-count",
            str(rotate_count),
            "daemon-run",
        ]
        subprocess.Popen(
            args,
            cwd=str(self.root),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        deadline = time.time() + self.timeout
        last_error = "daemon did not start"
        while time.time() < deadline:
            try:
                return self.request({"action": "ping"}, timeout=0.5)
            except Exception as exc:
                last_error = str(exc)
                time.sleep(0.1)
        raise RpcError(last_error)

    def request(self, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        sock_path = self.paths["socket"]
        if not sock_path.exists():
            raise RpcError(f"daemon socket does not exist: {sock_path}")
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(self.timeout if timeout is None else timeout)
        try:
            client.connect(str(sock_path))
            client.sendall(encode_response(payload))
            client.shutdown(socket.SHUT_WR)
            raw = b""
            while True:
                chunk = client.recv(65536)
                if not chunk:
                    break
                raw += chunk
        finally:
            client.close()
        data = decode_request(raw)
        if not data.get("ok"):
            raise RpcError(str(data.get("error", "unknown daemon error")))
        result = data.get("data")
        if not isinstance(result, dict):
            raise RpcError("daemon returned invalid data")
        return result


def print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PyDaemonControl directory-scoped daemon/client")
    parser.add_argument("--root", default=".", help="Directory that owns exactly one daemon")
    parser.add_argument("--timeout", type=float, default=10.0, help="Client/daemon-start timeout")
    parser.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    parser.add_argument("--rotate-count", type=int, default=DEFAULT_ROTATE_COUNT)
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("daemon-run", help=argparse.SUPPRESS)
    sub.add_parser("daemon-start", help="Start daemon for the root directory")
    stop_daemon = sub.add_parser("daemon-stop", help="Stop daemon")
    stop_daemon.add_argument("--stop-children", action="store_true")
    stop_daemon.add_argument("--grace", type=float, default=5.0)
    sub.add_parser("status", help="Show daemon and process status")

    start = sub.add_parser("start", help="Start a named process")
    start.add_argument("name")
    start.add_argument("--cwd", default=None)
    start.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    start.add_argument("--rotate-count", type=int, default=DEFAULT_ROTATE_COUNT)
    start.add_argument("argv", nargs=argparse.REMAINDER)

    restart = sub.add_parser("restart", help="Stop and start a named process")
    restart.add_argument("name")
    restart.add_argument("--cwd", default=None)
    restart.add_argument("--grace", type=float, default=5.0)
    restart.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    restart.add_argument("--rotate-count", type=int, default=DEFAULT_ROTATE_COUNT)
    restart.add_argument("argv", nargs=argparse.REMAINDER)

    send = sub.add_parser("send", help="Send text to a process stdin")
    send.add_argument("name")
    send.add_argument("text")
    send.add_argument("--no-newline", action="store_true")
    send.add_argument("--wait", type=float, default=1.0)
    send.add_argument("--bytes", type=int, default=65536)

    cmd = sub.add_parser("cmd", help="Alias for send")
    cmd.add_argument("name")
    cmd.add_argument("text")
    cmd.add_argument("--no-newline", action="store_true")
    cmd.add_argument("--wait", type=float, default=1.0)
    cmd.add_argument("--bytes", type=int, default=65536)

    tail = sub.add_parser("tail", help="Print recent process output")
    tail.add_argument("name")
    tail.add_argument("--bytes", type=int, default=65536)

    stop = sub.add_parser("stop", help="Stop a named process")
    stop.add_argument("name")
    stop.add_argument("--grace", type=float, default=5.0)
    return parser


def clean_argv(argv: list[str]) -> list[str]:
    if argv and argv[0] == "--":
        return argv[1:]
    return argv


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root).resolve()
    script = Path(__file__).resolve()

    try:
        if args.cmd == "daemon-run":
            return ProcHostDaemon(root, args.max_log_bytes, args.rotate_count).serve()

        client = ProcHostClient(root, script, args.timeout)
        if args.cmd == "daemon-start":
            print_json(client.start_daemon(args.max_log_bytes, args.rotate_count))
            return 0
        if args.cmd == "status":
            print_json(client.request({"action": "status"}))
            return 0
        if args.cmd == "daemon-stop":
            print_json(
                client.request(
                    {
                        "action": "daemon-stop",
                        "stopChildren": bool(args.stop_children),
                        "grace": float(args.grace),
                    }
                )
            )
            return 0
        if args.cmd in {"start", "restart"}:
            proc_argv = clean_argv(args.argv)
            if not proc_argv:
                raise RpcError("missing process command after --")
            if not client.daemon_running():
                client.start_daemon(args.max_log_bytes, args.rotate_count)
            print_json(
                client.request(
                    {
                        "action": args.cmd,
                        "name": args.name,
                        "cwd": args.cwd,
                        "argv": proc_argv,
                        "grace": getattr(args, "grace", 5.0),
                        "maxLogBytes": args.max_log_bytes,
                        "rotateCount": args.rotate_count,
                    }
                )
            )
            return 0
        if args.cmd in {"send", "cmd"}:
            print_json(
                client.request(
                    {
                        "action": "send",
                        "name": args.name,
                        "text": args.text,
                        "newline": not args.no_newline,
                        "wait": args.wait,
                        "maxBytes": args.bytes,
                    },
                    timeout=args.wait + args.timeout,
                )
            )
            return 0
        if args.cmd == "tail":
            data = client.request({"action": "tail", "name": args.name, "maxBytes": args.bytes})
            sys.stdout.write(str(data.get("output", "")))
            return 0
        if args.cmd == "stop":
            print_json(client.request({"action": "stop", "name": args.name, "grace": args.grace}))
            return 0
        raise RpcError(f"unknown command: {args.cmd}")
    except Exception as exc:
        print(f"pydaemoncontrol: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

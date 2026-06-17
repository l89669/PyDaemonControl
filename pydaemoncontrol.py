#!/usr/bin/env python3
"""PyDaemonControl: a small directory-scoped process controller.

One daemon owns one root directory. Clients are one-shot commands that talk to
the daemon over a local IPC endpoint.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import queue
import signal
import socket
import secrets
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

try:
    import msvcrt
except ImportError:  # pragma: no cover - only available on Windows.
    msvcrt = None


DEFAULT_MAX_LOG_BYTES = 32 * 1024 * 1024
DEFAULT_ROTATE_COUNT = 3
DEFAULT_TAIL_CHARS = 256 * 1024
REQUEST_LIMIT_BYTES = 8 * 1024 * 1024
RESPONSE_LIMIT_BYTES = 8 * 1024 * 1024
MAX_OUTPUT_BYTES = RESPONSE_LIMIT_BYTES // 2
STDIN_QUEUE_LIMIT = 128
MAX_STDIN_LOG_CHARS = 4096
DEFAULT_CLIENT_TIMEOUT = 10.0
DEFAULT_DAEMON_CONN_TIMEOUT = 30.0
DEFAULT_DAEMON_ACCEPT_TIMEOUT = 0.5
DEFAULT_INPUT_WAIT = 0.25
DEFAULT_OUTPUT_WAIT = 1.0
DEFAULT_OUTPUT_QUIET = 0.2
DEFAULT_STOP_GRACE = 5.0
DEFAULT_FORCE_KILL_WAIT = 5.0
DEFAULT_START_POLL_INTERVAL = 0.1
IS_WINDOWS = os.name == "nt"


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


def require_seconds(value: Any, name: str, *, positive: bool) -> float:
    try:
        seconds = float(value)
    except (TypeError, ValueError) as exc:
        raise RpcError(f"{name} must be a number of seconds") from exc
    if not math.isfinite(seconds):
        raise RpcError(f"{name} must be finite")
    if positive and seconds <= 0:
        raise RpcError(f"{name} must be greater than 0")
    if not positive and seconds < 0:
        raise RpcError(f"{name} must be greater than or equal to 0")
    return seconds


def require_int_at_least(value: Any, name: str, minimum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RpcError(f"{name} must be an integer") from exc
    if parsed < minimum:
        raise RpcError(f"{name} must be at least {minimum}")
    return parsed


def require_output_bytes(value: Any) -> int:
    parsed = require_int_at_least(value, "maxBytes", 1)
    if parsed > MAX_OUTPUT_BYTES:
        raise RpcError(f"maxBytes must be at most {MAX_OUTPUT_BYTES}")
    return parsed


def format_stdin_log(text: str) -> str:
    if len(text) <= MAX_STDIN_LOG_CHARS:
        return text
    return (
        text[:MAX_STDIN_LOG_CHARS]
        + f"\n[pydaemoncontrol] stdin log truncated; originalChars={len(text)}"
    )


def state_paths(root: Path) -> dict[str, Path]:
    state = root / ".pydaemoncontrol"
    return {
        "state": state,
        "logs": state / "logs",
        "socket": state / "daemon.sock",
        "endpoint": state / "daemon.endpoint.json",
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


def read_socket_message(sock: socket.socket, limit: int, label: str) -> bytes:
    raw = b""
    try:
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            raw += chunk
            if len(raw) > limit:
                raise RpcError(f"{label} too large")
    except socket.timeout as exc:
        raise RpcError(f"{label} timed out") from exc
    return raw


def write_daemon_log(paths: dict[str, Path], message: str) -> None:
    line = f"[{now_stamp()} daemon] {message}\n"
    with paths["daemon_log"].open("a", encoding="utf-8") as fh:
        fh.write(line)


class DaemonLock:
    def acquire(self) -> None:
        raise NotImplementedError

    def release(self) -> None:
        raise NotImplementedError


class PosixDaemonLock(DaemonLock):
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fh: Any = None

    def acquire(self) -> None:
        if fcntl is None:
            raise RpcError("fcntl is required for POSIX daemon locking")
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.lock_path.open("w")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RpcError(f"daemon lock is already held: {self.lock_path}") from exc

    def release(self) -> None:
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None


class WindowsDaemonLock(DaemonLock):
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fh: Any = None

    def acquire(self) -> None:
        if msvcrt is None:
            raise RpcError("msvcrt is required for Windows daemon locking")
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.lock_path.open("a+b")
        self._fh.seek(0)
        if not self._fh.read(1):
            self._fh.seek(0)
            self._fh.write(b"\0")
            self._fh.flush()
        self._fh.seek(0)
        try:
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise RpcError(f"daemon lock is already held: {self.lock_path}") from exc

    def release(self) -> None:
        if self._fh is not None:
            try:
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                self._fh.close()
                self._fh = None


class IpcEndpoint:
    def bind_server(self) -> socket.socket:
        raise NotImplementedError

    def connect_client(self, timeout: float) -> socket.socket:
        raise NotImplementedError

    def prepare_client_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        return payload

    def validate_server_request(self, payload: dict[str, Any]) -> None:
        return None

    def cleanup(self) -> None:
        return None


class UnixSocketEndpoint(IpcEndpoint):
    def __init__(self, socket_path: Path) -> None:
        self.socket_path = socket_path

    def bind_server(self) -> socket.socket:
        if self.socket_path.exists():
            self.socket_path.unlink()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        return server

    def connect_client(self, timeout: float) -> socket.socket:
        if not self.socket_path.exists():
            raise RpcError(f"daemon socket does not exist: {self.socket_path}")
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(timeout)
        client.connect(str(self.socket_path))
        return client

    def cleanup(self) -> None:
        if self.socket_path.exists():
            self.socket_path.unlink()


class TcpEndpoint(IpcEndpoint):
    def __init__(self, endpoint_path: Path) -> None:
        self.endpoint_path = endpoint_path
        self.host = "127.0.0.1"
        self.port: int | None = None
        self.token: str | None = None

    def bind_server(self) -> socket.socket:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, 0))
        self.port = int(server.getsockname()[1])
        self.token = secrets.token_hex(32)
        payload = {
            "transport": "tcp",
            "host": self.host,
            "port": self.port,
            "token": self.token,
            "pid": os.getpid(),
        }
        tmp_path = self.endpoint_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self.endpoint_path)
        return server

    def connect_client(self, timeout: float) -> socket.socket:
        if not self.endpoint_path.exists():
            raise RpcError(f"daemon endpoint does not exist: {self.endpoint_path}")
        try:
            data = json.loads(self.endpoint_path.read_text(encoding="utf-8"))
            if data.get("transport") != "tcp":
                raise ValueError("unsupported endpoint transport")
            self.host = str(data["host"])
            self.port = int(data["port"])
            self.token = str(data["token"])
        except Exception as exc:
            raise RpcError(f"invalid daemon endpoint file: {exc}") from exc
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(timeout)
        client.connect((self.host, self.port))
        return client

    def prepare_client_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.token:
            raise RpcError("missing daemon endpoint token")
        with_token = dict(payload)
        with_token["_token"] = self.token
        return with_token

    def validate_server_request(self, payload: dict[str, Any]) -> None:
        if payload.pop("_token", None) != self.token:
            raise RpcError("invalid client token")

    def cleanup(self) -> None:
        if self.endpoint_path.exists():
            self.endpoint_path.unlink()


class ProcessSupervisor:
    def popen_kwargs(self, detached: bool = False) -> dict[str, Any]:
        raise NotImplementedError

    def stop_process(self, proc: subprocess.Popen[bytes], write_event: Any, grace: float) -> None:
        raise NotImplementedError


class PosixProcessSupervisor(ProcessSupervisor):
    def popen_kwargs(self, detached: bool = False) -> dict[str, Any]:
        return {"start_new_session": True}

    def stop_process(self, proc: subprocess.Popen[bytes], write_event: Any, grace: float) -> None:
        pid = proc.pid
        write_event("system", f"stopping process group {pid}")
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.time() + max(0.0, grace)
        while time.time() < deadline:
            if proc.poll() is not None:
                return
            time.sleep(0.05)
        if proc.poll() is None:
            write_event("system", f"killing process group {pid}")
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=DEFAULT_FORCE_KILL_WAIT)


class WindowsProcessSupervisor(ProcessSupervisor):
    def popen_kwargs(self, detached: bool = False) -> dict[str, Any]:
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        if detached:
            flags |= getattr(subprocess, "DETACHED_PROCESS", 0)
        return {"creationflags": flags} if flags else {}

    def stop_process(self, proc: subprocess.Popen[bytes], write_event: Any, grace: float) -> None:
        pid = proc.pid
        write_event("system", f"terminating process tree {pid}")
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        deadline = time.time() + max(0.0, grace)
        while time.time() < deadline:
            if proc.poll() is not None:
                return
            time.sleep(0.05)
        if proc.poll() is None:
            write_event("system", f"killing process tree {pid}")
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=DEFAULT_FORCE_KILL_WAIT,
                    check=False,
                )
            except Exception as exc:
                write_event("system", f"taskkill failed: {exc}")
                proc.kill()
            proc.wait(timeout=DEFAULT_FORCE_KILL_WAIT)


class PlatformServices:
    def __init__(self, paths: dict[str, Path]) -> None:
        if IS_WINDOWS:
            self.lock: DaemonLock = WindowsDaemonLock(paths["lock"])
            self.endpoint: IpcEndpoint = TcpEndpoint(paths["endpoint"])
            self.processes: ProcessSupervisor = WindowsProcessSupervisor()
        else:
            self.lock = PosixDaemonLock(paths["lock"])
            self.endpoint = UnixSocketEndpoint(paths["socket"])
            self.processes = PosixProcessSupervisor()


@dataclass
class StdinJob:
    payload: bytes
    display_text: str
    done: threading.Event = field(default_factory=threading.Event)
    written: bool = False
    error: str | None = None


@dataclass
class HostedProcess:
    name: str
    argv: list[str]
    cwd: Path
    log_path: Path
    supervisor: ProcessSupervisor
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
    _stdin_queue: queue.Queue[StdinJob | None] = field(
        default_factory=lambda: queue.Queue(maxsize=STDIN_QUEUE_LIMIT)
    )
    _cond: threading.Condition = field(init=False)
    _tail: deque[str] = field(default_factory=deque)
    _tail_len: int = 0
    _line_open: dict[str, bool] = field(default_factory=dict)

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
            **self.supervisor.popen_kwargs(),
        )
        threading.Thread(target=self._reader, args=("stdout", self.proc.stdout), daemon=True).start()
        threading.Thread(target=self._reader, args=("stderr", self.proc.stderr), daemon=True).start()
        threading.Thread(target=self._stdin_writer, daemon=True).start()
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
        try:
            self._stdin_queue.put_nowait(None)
        except queue.Full:
            pass
        self.write_event("system", f"process exited with code {code}")

    def _stdin_writer(self) -> None:
        while True:
            job = self._stdin_queue.get()
            if job is None:
                return
            try:
                proc = self.proc
                if proc is None or proc.stdin is None or proc.poll() is not None:
                    job.error = f"{self.name} is not running"
                    continue
                proc.stdin.write(job.payload)
                proc.stdin.flush()
                self.write_event("stdin", job.display_text)
                job.written = True
            except Exception as exc:
                job.error = f"stdin write failed: {exc}"
            finally:
                job.done.set()

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
        chunk = chunk.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        with self._cond:
            data = bytearray()
            if not self._line_open.get(stream_name, False):
                data.extend(prefix)
            parts = chunk.split(b"\n")
            for index, part in enumerate(parts):
                if index > 0:
                    data.extend(b"\n")
                    if part:
                        data.extend(prefix)
                data.extend(part)
            self._line_open[stream_name] = not chunk.endswith(b"\n")
            data_bytes = bytes(data)
            self._rotate_if_needed(len(data_bytes))
            if self._fd is None:
                self._fd = os.open(self.log_path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
            os.write(self._fd, data_bytes)
            self.bytes_written += len(data_bytes)
            self._add_tail(data_bytes.decode("utf-8", errors="replace"))
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

    def wait_for_output_after(self, offset: int, timeout: float, quiet: float = DEFAULT_OUTPUT_QUIET) -> None:
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

    def send(
        self,
        text: str,
        append_newline: bool,
        input_wait: float,
        wait: float,
        quiet: float,
        max_bytes: int,
    ) -> dict[str, Any]:
        if not self.running or self.proc is None or self.proc.stdin is None:
            raise RpcError(f"{self.name} is not running")
        payload = text + ("\n" if append_newline else "")
        job = StdinJob(payload=payload.encode("utf-8"), display_text=format_stdin_log(payload.rstrip("\n")))
        with self._lock:
            offset = self.bytes_written
        try:
            self._stdin_queue.put_nowait(job)
        except queue.Full as exc:
            raise RpcError(f"{self.name} stdin queue is full") from exc

        if not job.done.wait(max(0.0, input_wait)):
            return {
                "offset": offset,
                "queued": True,
                "written": False,
                "inputWaitExpired": True,
                "output": self.read_since(offset, max_bytes),
            }
        if job.error is not None:
            raise RpcError(job.error)
        if wait > 0:
            self.wait_for_output_after(offset, wait, quiet)
        return {
            "offset": offset,
            "queued": True,
            "written": job.written,
            "inputWaitExpired": False,
            "output": self.read_since(offset, max_bytes),
        }

    def stop(self, grace: float) -> dict[str, Any]:
        if not self.running or self.proc is None:
            return {"stopped": False, "reason": "not running", "status": self.status()}
        self.supervisor.stop_process(self.proc, self.write_event, grace)
        return {"stopped": True, "status": self.status()}


class ProcHostDaemon:
    def __init__(self, root: Path, max_log_bytes: int, rotate_count: int) -> None:
        self.root = root.resolve()
        self.paths = state_paths(self.root)
        self.platform = PlatformServices(self.paths)
        self.max_log_bytes = max_log_bytes
        self.rotate_count = rotate_count
        self.processes: dict[str, HostedProcess] = {}
        self._shutdown = threading.Event()

    def acquire_lock(self) -> None:
        self.paths["state"].mkdir(parents=True, exist_ok=True)
        self.paths["logs"].mkdir(parents=True, exist_ok=True)
        self.platform.lock.acquire()
        self.paths["pid"].write_text(str(os.getpid()) + "\n", encoding="utf-8")

    def serve(self) -> int:
        self.acquire_lock()
        server: socket.socket | None = None
        try:
            server = self.platform.endpoint.bind_server()
            server.listen(16)
            server.settimeout(DEFAULT_DAEMON_ACCEPT_TIMEOUT)
            write_daemon_log(self.paths, f"daemon started pid={os.getpid()} root={self.root}")
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
            if server is not None:
                server.close()
            self.platform.endpoint.cleanup()
            self.platform.lock.release()
        return 0

    def _handle_conn(self, conn: socket.socket) -> None:
        with conn:
            try:
                conn.settimeout(DEFAULT_DAEMON_CONN_TIMEOUT)
                raw = read_socket_message(conn, REQUEST_LIMIT_BYTES, "request")
                req = decode_request(raw)
                self.platform.endpoint.validate_server_request(req)
                response = {"ok": True, "data": self.handle(req)}
            except Exception as exc:
                response = {"ok": False, "error": str(exc)}
            encoded = encode_response(response)
            if len(encoded) > RESPONSE_LIMIT_BYTES:
                encoded = encode_response({"ok": False, "error": "response too large"})
            conn.sendall(encoded)

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
                self.processes[name].stop(require_seconds(req.get("grace", DEFAULT_STOP_GRACE), "grace", positive=False))
            return self._start(req, replace=True)
        if action == "send":
            entry = self.get_process(str(req.get("name", "")))
            return entry.send(
                str(req.get("text", "")),
                bool(req.get("newline", req.get("appendNewline", True))),
                require_seconds(req.get("inputWait", DEFAULT_INPUT_WAIT), "inputWait", positive=False),
                require_seconds(req.get("wait", DEFAULT_OUTPUT_WAIT), "wait", positive=False),
                require_seconds(req.get("quiet", DEFAULT_OUTPUT_QUIET), "quiet", positive=False),
                require_output_bytes(req.get("maxBytes", 65536)),
            )
        if action == "tail":
            entry = self.get_process(str(req.get("name", "")))
            return {
                "output": entry.read_tail(require_output_bytes(req.get("maxBytes", 65536))),
                "status": entry.status(),
            }
        if action == "stop":
            entry = self.get_process(str(req.get("name", "")))
            return entry.stop(require_seconds(req.get("grace", DEFAULT_STOP_GRACE), "grace", positive=False))
        if action == "daemon-stop":
            if req.get("stopChildren", False):
                grace = require_seconds(req.get("grace", DEFAULT_STOP_GRACE), "grace", positive=False)
                for entry in list(self.processes.values()):
                    entry.stop(grace)
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
            supervisor=self.platform.processes,
            max_log_bytes=require_int_at_least(req.get("maxLogBytes", self.max_log_bytes), "maxLogBytes", 0),
            rotate_count=require_int_at_least(req.get("rotateCount", self.rotate_count), "rotateCount", 0),
        )
        self.processes[name] = entry
        entry.start()
        return {"started": True, "status": entry.status()}


class ProcHostClient:
    def __init__(self, root: Path, script: Path, timeout: float) -> None:
        self.root = root.resolve()
        self.script = script.resolve()
        self.paths = state_paths(self.root)
        self.platform = PlatformServices(self.paths)
        self.timeout = require_seconds(timeout, "timeout", positive=True)

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
            **self.platform.processes.popen_kwargs(detached=True),
        )
        deadline = time.time() + self.timeout
        last_error = "daemon did not start"
        while time.time() < deadline:
            try:
                return self.request({"action": "ping"}, timeout=0.5)
            except Exception as exc:
                last_error = str(exc)
                time.sleep(DEFAULT_START_POLL_INTERVAL)
        raise RpcError(last_error)

    def request(self, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        effective_timeout = self.timeout if timeout is None else require_seconds(timeout, "request timeout", positive=True)
        client = self.platform.endpoint.connect_client(effective_timeout)
        try:
            encoded = encode_response(self.platform.endpoint.prepare_client_request(payload))
            if len(encoded) > REQUEST_LIMIT_BYTES:
                raise RpcError("request too large")
            client.sendall(encoded)
            client.shutdown(socket.SHUT_WR)
            raw = read_socket_message(client, RESPONSE_LIMIT_BYTES, "response")
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
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_CLIENT_TIMEOUT,
        help="Client RPC and daemon-start overhead timeout",
    )
    parser.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    parser.add_argument("--rotate-count", type=int, default=DEFAULT_ROTATE_COUNT)
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("daemon-run", help=argparse.SUPPRESS)
    sub.add_parser("daemon-start", help="Start daemon for the root directory")
    stop_daemon = sub.add_parser("daemon-stop", help="Stop daemon")
    stop_daemon.add_argument("--stop-children", action="store_true")
    stop_daemon.add_argument("--grace", type=float, default=DEFAULT_STOP_GRACE)
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
    restart.add_argument("--grace", type=float, default=DEFAULT_STOP_GRACE)
    restart.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    restart.add_argument("--rotate-count", type=int, default=DEFAULT_ROTATE_COUNT)
    restart.add_argument("argv", nargs=argparse.REMAINDER)

    send = sub.add_parser("send", help="Send text to a process stdin")
    send.add_argument("name")
    send.add_argument("text")
    send.add_argument("--no-newline", action="store_true")
    send.add_argument("--input-wait", type=float, default=DEFAULT_INPUT_WAIT)
    send.add_argument("--wait", type=float, default=DEFAULT_OUTPUT_WAIT)
    send.add_argument("--quiet", type=float, default=DEFAULT_OUTPUT_QUIET)
    send.add_argument("--bytes", type=int, default=65536)

    cmd = sub.add_parser("cmd", help="Alias for send")
    cmd.add_argument("name")
    cmd.add_argument("text")
    cmd.add_argument("--no-newline", action="store_true")
    cmd.add_argument("--input-wait", type=float, default=DEFAULT_INPUT_WAIT)
    cmd.add_argument("--wait", type=float, default=DEFAULT_OUTPUT_WAIT)
    cmd.add_argument("--quiet", type=float, default=DEFAULT_OUTPUT_QUIET)
    cmd.add_argument("--bytes", type=int, default=65536)

    tail = sub.add_parser("tail", help="Print recent process output")
    tail.add_argument("name")
    tail.add_argument("--bytes", type=int, default=65536)

    stop = sub.add_parser("stop", help="Stop a named process")
    stop.add_argument("name")
    stop.add_argument("--grace", type=float, default=DEFAULT_STOP_GRACE)
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
        max_log_bytes = require_int_at_least(args.max_log_bytes, "max-log-bytes", 0)
        rotate_count = require_int_at_least(args.rotate_count, "rotate-count", 0)
        if args.cmd == "daemon-run":
            return ProcHostDaemon(root, max_log_bytes, rotate_count).serve()

        client = ProcHostClient(root, script, args.timeout)
        if args.cmd == "daemon-start":
            print_json(client.start_daemon(max_log_bytes, rotate_count))
            return 0
        if args.cmd == "status":
            print_json(client.request({"action": "status"}))
            return 0
        if args.cmd == "daemon-stop":
            grace = require_seconds(args.grace, "grace", positive=False)
            print_json(
                client.request(
                    {
                        "action": "daemon-stop",
                        "stopChildren": bool(args.stop_children),
                        "grace": grace,
                    },
                    timeout=client.timeout + grace,
                )
            )
            return 0
        if args.cmd in {"start", "restart"}:
            proc_argv = clean_argv(args.argv)
            if not proc_argv:
                raise RpcError("missing process command after --")
            if not client.daemon_running():
                client.start_daemon(max_log_bytes, rotate_count)
            grace = require_seconds(getattr(args, "grace", DEFAULT_STOP_GRACE), "grace", positive=False)
            print_json(
                client.request(
                    {
                        "action": args.cmd,
                        "name": args.name,
                        "cwd": args.cwd,
                        "argv": proc_argv,
                        "grace": grace,
                        "maxLogBytes": max_log_bytes,
                        "rotateCount": rotate_count,
                    },
                    timeout=client.timeout + (grace if args.cmd == "restart" else 0.0),
                )
            )
            return 0
        if args.cmd in {"send", "cmd"}:
            input_wait = require_seconds(args.input_wait, "input-wait", positive=False)
            wait = require_seconds(args.wait, "wait", positive=False)
            quiet = require_seconds(args.quiet, "quiet", positive=False)
            max_bytes = require_output_bytes(args.bytes)
            print_json(
                client.request(
                    {
                        "action": "send",
                        "name": args.name,
                        "text": args.text,
                        "newline": not args.no_newline,
                        "inputWait": input_wait,
                        "wait": wait,
                        "quiet": quiet,
                        "maxBytes": max_bytes,
                    },
                    timeout=client.timeout + input_wait + wait,
                )
            )
            return 0
        if args.cmd == "tail":
            max_bytes = require_output_bytes(args.bytes)
            data = client.request({"action": "tail", "name": args.name, "maxBytes": max_bytes})
            sys.stdout.write(str(data.get("output", "")))
            return 0
        if args.cmd == "stop":
            grace = require_seconds(args.grace, "grace", positive=False)
            print_json(
                client.request(
                    {"action": "stop", "name": args.name, "grace": grace},
                    timeout=client.timeout + grace,
                )
            )
            return 0
        raise RpcError(f"unknown command: {args.cmd}")
    except Exception as exc:
        print(f"pydaemoncontrol: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

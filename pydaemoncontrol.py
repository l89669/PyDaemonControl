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
DEFAULT_ATTACH_HISTORY_BYTES = 20000
DEFAULT_ATTACH_POLL = 0.2
DEFAULT_ATTACH_DRAIN_ON_EOF = 1.0
DEFAULT_RESTART_MODE = "never"
DEFAULT_RESTART_DELAY = 2.0
DEFAULT_RESTART_MAX_ATTEMPTS = 0
DEFAULT_RESTART_WINDOW = 60.0
RESTART_MODES = {"never", "on-failure", "always"}
IS_WINDOWS = os.name == "nt"
LOG_OPEN_FLAGS = os.O_CREAT | os.O_APPEND | os.O_WRONLY | getattr(os, "O_BINARY", 0)
OUTPUT_STREAMS = ("stdout", "stderr", "system", "stdin")
MAX_RECORD_TEXT_BYTES = 4096


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


def require_history_bytes(value: Any) -> int:
    parsed = require_int_at_least(value, "history", 0)
    if parsed > MAX_OUTPUT_BYTES:
        raise RpcError(f"history must be at most {MAX_OUTPUT_BYTES}")
    return parsed


def format_stdin_log(text: str) -> str:
    if len(text) <= MAX_STDIN_LOG_CHARS:
        return text
    return (
        text[:MAX_STDIN_LOG_CHARS]
        + f"\n[pydaemoncontrol] stdin log truncated; originalChars={len(text)}"
    )


@dataclass(frozen=True)
class OutputRecord:
    seq: int
    stream: str
    text: str


def utf8_len(text: str) -> int:
    return len(text.encode("utf-8", errors="replace"))


def split_text_by_utf8_limit(text: str, limit: int = MAX_RECORD_TEXT_BYTES) -> list[str]:
    if not text:
        return []
    parts: list[str] = []
    current: list[str] = []
    current_size = 0
    for ch in text:
        ch_size = utf8_len(ch)
        if current and current_size + ch_size > limit:
            parts.append("".join(current))
            current = []
            current_size = 0
        current.append(ch)
        current_size += ch_size
    if current:
        parts.append("".join(current))
    return parts


def split_output_text(text: str) -> list[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return split_text_by_utf8_limit(normalized)


def record_text_bytes(records: list[OutputRecord]) -> int:
    return sum(utf8_len(record.text) for record in records)


def records_to_wire(records: list[OutputRecord], next_seq: int, *, rotated: bool = False) -> dict[str, Any]:
    payload: dict[str, Any] = {stream: [] for stream in OUTPUT_STREAMS}
    for record in sorted(records, key=lambda item: item.seq):
        if record.stream not in payload:
            payload[record.stream] = []
        payload[record.stream].append([record.seq, record.text])
    payload["nextSeq"] = next_seq
    payload["rotated"] = rotated
    return payload


def iter_wire_records(data: dict[str, Any]) -> list[OutputRecord]:
    records: list[OutputRecord] = []
    for stream in OUTPUT_STREAMS:
        raw_records = data.get(stream, [])
        if not isinstance(raw_records, list):
            continue
        for item in raw_records:
            if not isinstance(item, list) or len(item) != 2:
                continue
            try:
                seq = int(item[0])
            except (TypeError, ValueError):
                continue
            records.append(OutputRecord(seq=seq, stream=stream, text=str(item[1])))
    records.sort(key=lambda item: item.seq)
    return records


def render_records_combined(data: dict[str, Any], *, annotate_internal: bool = False) -> str:
    chunks: list[str] = []
    for record in iter_wire_records(data):
        if annotate_internal and record.stream in {"system", "stdin"}:
            chunks.append(f"[pydaemoncontrol {record.stream}] {record.text}")
        else:
            chunks.append(record.text)
    return "".join(chunks)


def default_restart_policy() -> dict[str, Any]:
    return {
        "mode": DEFAULT_RESTART_MODE,
        "delay": DEFAULT_RESTART_DELAY,
        "maxAttempts": DEFAULT_RESTART_MAX_ATTEMPTS,
        "window": DEFAULT_RESTART_WINDOW,
    }


def normalize_restart_policy(raw: Any | None) -> dict[str, Any]:
    base = default_restart_policy()
    if raw is None:
        return base
    if not isinstance(raw, dict):
        raise RpcError("restart policy must be an object")
    mode = str(raw.get("mode", base["mode"]))
    if mode not in RESTART_MODES:
        raise RpcError(f"restart mode must be one of: {', '.join(sorted(RESTART_MODES))}")
    return {
        "mode": mode,
        "delay": require_seconds(raw.get("delay", base["delay"]), "restart delay", positive=False),
        "maxAttempts": require_int_at_least(raw.get("maxAttempts", base["maxAttempts"]), "restart maxAttempts", 0),
        "window": require_seconds(raw.get("window", base["window"]), "restart window", positive=True),
    }


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
        "profiles": state / "profiles.json",
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


def load_profiles(paths: dict[str, Path]) -> dict[str, Any]:
    profile_path = paths["profiles"]
    if not profile_path.exists():
        return {"version": 1, "profiles": {}}
    try:
        data = json.loads(profile_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RpcError(f"invalid profiles file: {exc}") from exc
    if not isinstance(data, dict):
        raise RpcError("profiles file must contain a json object")
    profiles = data.get("profiles", {})
    if not isinstance(profiles, dict):
        raise RpcError("profiles file field 'profiles' must be an object")
    return {"version": int(data.get("version", 1)), "profiles": profiles}


def save_profiles(paths: dict[str, Path], data: dict[str, Any]) -> None:
    paths["state"].mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "profiles": data.get("profiles", {}),
    }
    tmp_path = paths["profiles"].with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(paths["profiles"])


def normalize_process_spec(
    root: Path,
    name: str,
    cwd_raw: Any,
    argv: Any,
    max_log_bytes: Any,
    rotate_count: Any,
    restart_policy: Any | None,
    shutdown_command: Any | None,
) -> dict[str, Any]:
    name = sanitize_name(name)
    if not isinstance(argv, list) or not argv or not all(isinstance(x, str) for x in argv):
        raise RpcError("argv must be a non-empty list of strings")
    if shutdown_command is not None:
        shutdown_command = str(shutdown_command)
        if not shutdown_command:
            shutdown_command = None
        elif "\n" in shutdown_command or "\r" in shutdown_command:
            raise RpcError("shutdown command must be a single line")
    cwd = Path(cwd_raw).resolve() if cwd_raw else root
    return {
        "name": name,
        "cwd": str(cwd),
        "argv": argv,
        "maxLogBytes": require_int_at_least(max_log_bytes, "maxLogBytes", 0),
        "rotateCount": require_int_at_least(rotate_count, "rotateCount", 0),
        "restart": normalize_restart_policy(restart_policy),
        "shutdownCommand": shutdown_command,
    }


def profile_to_start_request(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "action": "start",
        "name": profile["name"],
        "cwd": profile["cwd"],
        "argv": profile["argv"],
        "maxLogBytes": profile["maxLogBytes"],
        "rotateCount": profile["rotateCount"],
        "restart": profile["restart"],
        "shutdownCommand": profile.get("shutdownCommand"),
    }


def restart_policy_from_args(args: Any) -> dict[str, Any]:
    return normalize_restart_policy(
        {
            "mode": args.restart,
            "delay": args.restart_delay,
            "maxAttempts": args.restart_max_attempts,
            "window": args.restart_window,
        }
    )


def get_profile(paths: dict[str, Path], name: str) -> dict[str, Any]:
    name = sanitize_name(name)
    profiles = load_profiles(paths)["profiles"]
    profile = profiles.get(name)
    if profile is None:
        raise RpcError(f"unknown profile: {name}")
    if not isinstance(profile, dict):
        raise RpcError(f"profile {name} must be an object")
    return profile


def add_process_spec_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("name")
    parser.add_argument("--cwd", default=None)
    parser.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    parser.add_argument("--rotate-count", type=int, default=DEFAULT_ROTATE_COUNT)
    parser.add_argument("--restart", choices=sorted(RESTART_MODES), default=DEFAULT_RESTART_MODE)
    parser.add_argument("--restart-delay", type=float, default=DEFAULT_RESTART_DELAY)
    parser.add_argument("--restart-max-attempts", type=int, default=DEFAULT_RESTART_MAX_ATTEMPTS)
    parser.add_argument("--restart-window", type=float, default=DEFAULT_RESTART_WINDOW)
    parser.add_argument("--shutdown-command", default=None)


def add_outer_process_option_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cwd", dest="proc_cwd")
    parser.add_argument("--max-log-bytes", type=int, dest="proc_max_log_bytes")
    parser.add_argument("--rotate-count", type=int, dest="proc_rotate_count")
    parser.add_argument("--restart", choices=sorted(RESTART_MODES), dest="proc_restart")
    parser.add_argument("--restart-delay", type=float, dest="proc_restart_delay")
    parser.add_argument("--restart-max-attempts", type=int, dest="proc_restart_max_attempts")
    parser.add_argument("--restart-window", type=float, dest="proc_restart_window")
    parser.add_argument("--shutdown-command", dest="proc_shutdown_command")


def apply_outer_process_options(spec_args: Any, outer_args: Any) -> Any:
    mapping = {
        "proc_cwd": "cwd",
        "proc_max_log_bytes": "max_log_bytes",
        "proc_rotate_count": "rotate_count",
        "proc_restart": "restart",
        "proc_restart_delay": "restart_delay",
        "proc_restart_max_attempts": "restart_max_attempts",
        "proc_restart_window": "restart_window",
        "proc_shutdown_command": "shutdown_command",
    }
    for source, target in mapping.items():
        value = getattr(outer_args, source, None)
        if value is not None:
            setattr(spec_args, target, value)
    return spec_args


def parse_process_spec_tokens(tokens: list[str], prog: str) -> tuple[Any, list[str]]:
    if "--" in tokens:
        marker = tokens.index("--")
        spec_tokens = tokens[:marker]
        proc_argv = tokens[marker + 1 :]
    else:
        spec_tokens = tokens
        proc_argv = []
    parser = argparse.ArgumentParser(prog=prog)
    add_process_spec_arguments(parser)
    return parser.parse_args(spec_tokens), proc_argv


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
    log_end_seq: int | None = None


@dataclass
class HostedProcess:
    name: str
    argv: list[str]
    cwd: Path
    log_path: Path
    supervisor: ProcessSupervisor
    restart_policy: dict[str, Any] = field(default_factory=default_restart_policy)
    shutdown_command: str | None = None
    max_log_bytes: int = DEFAULT_MAX_LOG_BYTES
    rotate_count: int = DEFAULT_ROTATE_COUNT
    tail_chars: int = DEFAULT_TAIL_CHARS
    proc: subprocess.Popen[bytes] | None = None
    started_at: float = field(default_factory=time.time)
    exited_at: float | None = None
    exit_code: int | None = None
    suppress_restart_on_next_exit: bool = False
    restart_count: int = 0
    last_restart_at: float | None = None
    restart_pending_until: float | None = None
    next_seq: int = 0
    _fds: dict[str, int] = field(default_factory=dict)
    _stream_bytes: dict[str, int] = field(default_factory=dict)
    _logs_closed: bool = False
    _lock: threading.RLock = field(default_factory=threading.RLock)
    _stdin_queue: queue.Queue[StdinJob | None] = field(
        default_factory=lambda: queue.Queue(maxsize=STDIN_QUEUE_LIMIT)
    )
    _cond: threading.Condition = field(init=False)
    _tail_records: deque[OutputRecord] = field(default_factory=deque)
    _tail_bytes: int = 0
    _restart_attempts: deque[float] = field(default_factory=deque)
    _restart_generation: int = 0

    def __post_init__(self) -> None:
        self._cond = threading.Condition(self._lock)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._stream_bytes = {
            stream: self._stream_log_path(stream).stat().st_size
            if self._stream_log_path(stream).exists()
            else 0
            for stream in OUTPUT_STREAMS
        }

    def close_log(self) -> None:
        with self._lock:
            self._cancel_pending_restart_locked()
            self._logs_closed = True
            for fd in self._fds.values():
                os.close(fd)
            self._fds.clear()

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    @property
    def restart_pending(self) -> bool:
        return self.restart_pending_until is not None

    def _pending_restart_message(self) -> str:
        return f"{self.name} has a restart pending; stop it first or wait until it restarts"

    def _cancel_pending_restart_locked(self) -> bool:
        canceled = self.restart_pending_until is not None
        self._restart_generation += 1
        self.restart_pending_until = None
        self.suppress_restart_on_next_exit = False
        self._cond.notify_all()
        return canceled

    def cancel_pending_restart(self, reason: str) -> bool:
        with self._cond:
            canceled = self._cancel_pending_restart_locked()
            if canceled:
                self._append_records_locked("system", [f"restart canceled: {reason}\n"])
            return canceled

    def status(self) -> dict[str, Any]:
        with self._lock:
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
                "logPaths": {stream: str(self._stream_log_path(stream)) for stream in OUTPUT_STREAMS},
                "nextSeq": self.next_seq,
                "tailStartSeq": self._tail_records[0].seq if self._tail_records else self.next_seq,
                "restartPolicy": self.restart_policy,
                "shutdownCommand": self.shutdown_command,
                "suppressRestartOnNextExit": self.suppress_restart_on_next_exit,
                "restartCount": self.restart_count,
                "lastRestartAt": self.last_restart_at,
                "restartPendingUntil": self.restart_pending_until,
            }

    def start(self, restarted: bool = False, from_pending_restart: bool = False) -> None:
        with self._cond:
            if self._logs_closed:
                raise RpcError(f"{self.name} is closed")
            if self.proc is not None and self.proc.poll() is None:
                raise RpcError(f"{self.name} is already running")
            if self.restart_pending_until is not None and not from_pending_restart:
                raise RpcError(self._pending_restart_message())
            self._restart_generation += 1
            self.started_at = time.time()
            self.exited_at = None
            self.exit_code = None
            self.suppress_restart_on_next_exit = False
            self.restart_pending_until = None
            self._stdin_queue = queue.Queue(maxsize=STDIN_QUEUE_LIMIT)
            if restarted:
                self.last_restart_at = self.started_at
                self.restart_count += 1
            self.write_event("system", ("restarting: " if restarted else "starting: ") + " ".join(self.argv))
            proc = subprocess.Popen(
                self.argv,
                cwd=str(self.cwd),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                **self.supervisor.popen_kwargs(),
            )
            self.proc = proc
            stdin_queue = self._stdin_queue
            threading.Thread(target=self._reader, args=("stdout", proc.stdout), daemon=True).start()
            threading.Thread(target=self._reader, args=("stderr", proc.stderr), daemon=True).start()
            threading.Thread(target=self._stdin_writer, args=(proc, stdin_queue), daemon=True).start()
            threading.Thread(target=self._monitor, args=(proc, stdin_queue), daemon=True).start()

    def _reader(self, stream_name: str, stream: Any) -> None:
        try:
            while True:
                chunk = stream.read(65536)
                if not chunk:
                    break
                self.write_chunk(stream_name, chunk)
        except Exception as exc:
            self.write_event("system", f"{stream_name} reader failed: {exc}")

    def _monitor(self, proc: subprocess.Popen[bytes], stdin_queue: queue.Queue[StdinJob | None]) -> None:
        code = proc.wait()
        if self.proc is not proc:
            self.write_event("system", f"previous process exited with code {code}")
            return
        self.exit_code = code
        self.exited_at = time.time()
        try:
            stdin_queue.put_nowait(None)
        except queue.Full:
            pass
        self.write_event("system", f"process exited with code {code}")
        self._maybe_schedule_restart(code)

    def _restart_allowed(self, code: int) -> bool:
        policy = normalize_restart_policy(self.restart_policy)
        mode = policy["mode"]
        if self.suppress_restart_on_next_exit:
            self.suppress_restart_on_next_exit = False
            self.write_event("system", "restart suppressed for this exit")
            return False
        if mode == "never":
            return False
        if mode == "on-failure" and code == 0:
            return False
        now = time.time()
        window = float(policy["window"])
        while self._restart_attempts and now - self._restart_attempts[0] > window:
            self._restart_attempts.popleft()
        max_attempts = int(policy["maxAttempts"])
        if max_attempts > 0 and len(self._restart_attempts) >= max_attempts:
            self.write_event(
                "system",
                f"restart suppressed: maxAttempts={max_attempts} reached within {window:g}s",
            )
            return False
        self._restart_attempts.append(now)
        return True

    def _maybe_schedule_restart(self, code: int) -> None:
        with self._cond:
            if not self._restart_allowed(code):
                return
            policy = normalize_restart_policy(self.restart_policy)
            delay = float(policy["delay"])
            self._restart_generation += 1
            token = self._restart_generation
            self.restart_pending_until = time.time() + delay
            self._append_records_locked(
                "system",
                [f"restart scheduled in {delay:g}s by policy {policy['mode']}\n"],
            )
        threading.Thread(target=self._restart_after_delay, args=(delay, token), daemon=True).start()

    def _restart_after_delay(self, delay: float, token: int) -> None:
        if delay > 0:
            time.sleep(delay)
        with self._cond:
            if token != self._restart_generation or self.restart_pending_until is None:
                return
            if self._logs_closed:
                self._cancel_pending_restart_locked()
                return
            if self.running:
                self._cancel_pending_restart_locked()
                return
            try:
                self.start(restarted=True, from_pending_restart=True)
            except Exception as exc:
                self._cancel_pending_restart_locked()
                self._append_records_locked("system", [f"restart failed: {exc}\n"])

    def _stdin_writer(self, proc: subprocess.Popen[bytes], stdin_queue: queue.Queue[StdinJob | None]) -> None:
        while True:
            job = stdin_queue.get()
            if job is None:
                return
            try:
                if proc is not self.proc or proc.stdin is None or proc.poll() is not None:
                    job.error = f"{self.name} is not running"
                    continue
                proc.stdin.write(job.payload)
                proc.stdin.flush()
                job.log_end_seq = self.write_event("stdin", job.display_text)
                job.written = True
            except Exception as exc:
                job.error = f"stdin write failed: {exc}"
            finally:
                job.done.set()

    def _stream_log_path(self, stream_name: str) -> Path:
        return self.log_path.with_name(f"{self.log_path.stem}.{stream_name}{self.log_path.suffix}")

    def _rotate_if_needed(self, stream_name: str, incoming_len: int) -> None:
        if self.max_log_bytes <= 0:
            return
        current_size = self._stream_bytes.get(stream_name, 0)
        if current_size + incoming_len <= self.max_log_bytes:
            return
        fd = self._fds.pop(stream_name, None)
        if fd is not None:
            os.close(fd)
        path = self._stream_log_path(stream_name)
        if self.rotate_count <= 0:
            if path.exists():
                path.unlink()
            self._stream_bytes[stream_name] = 0
            return
        for index in range(self.rotate_count, 0, -1):
            src = path.with_name(path.name + ("" if index == 1 else f".{index - 1}"))
            dst = path.with_name(path.name + f".{index}")
            if index == 1:
                src = path
            if src.exists():
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
        self._stream_bytes[stream_name] = 0

    def _add_tail_record(self, record: OutputRecord) -> None:
        size = utf8_len(record.text)
        self._tail_records.append(record)
        self._tail_bytes += size
        while self._tail_bytes > self.tail_chars and len(self._tail_records) > 1:
            removed = self._tail_records.popleft()
            self._tail_bytes -= utf8_len(removed.text)

    def _record_log_bytes(self, record: OutputRecord) -> bytes:
        return (
            json.dumps([record.seq, record.text], ensure_ascii=False, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")

    def _write_record_to_log(self, record: OutputRecord) -> None:
        if self._logs_closed:
            return
        encoded = self._record_log_bytes(record)
        self._rotate_if_needed(record.stream, len(encoded))
        fd = self._fds.get(record.stream)
        if fd is None:
            fd = os.open(self._stream_log_path(record.stream), LOG_OPEN_FLAGS, 0o644)
            self._fds[record.stream] = fd
        os.write(fd, encoded)
        self._stream_bytes[record.stream] = self._stream_bytes.get(record.stream, 0) + len(encoded)

    def _append_records_locked(self, stream_name: str, texts: list[str]) -> int:
        for text in texts:
            if not text:
                continue
            record = OutputRecord(seq=self.next_seq, stream=stream_name, text=text)
            self.next_seq += 1
            self._write_record_to_log(record)
            self._add_tail_record(record)
        self._cond.notify_all()
        return self.next_seq

    def write_event(self, stream_name: str, text: str) -> int:
        return self.write_records(stream_name, [text.rstrip("\n") + "\n"])

    def write_chunk(self, stream_name: str, chunk: bytes) -> int:
        text = chunk.decode("utf-8", errors="replace")
        return self.write_records(stream_name, split_output_text(text))

    def write_records(self, stream_name: str, texts: list[str]) -> int:
        if stream_name not in OUTPUT_STREAMS:
            raise RpcError(f"unknown output stream: {stream_name}")
        with self._cond:
            return self._append_records_locked(stream_name, texts)

    def _limit_records_from_end(self, records: list[OutputRecord], max_bytes: int) -> list[OutputRecord]:
        selected: list[OutputRecord] = []
        total = 0
        for record in reversed(records):
            size = utf8_len(record.text)
            if selected and total + size > max_bytes:
                break
            if not selected and size > max_bytes:
                text_bytes = record.text.encode("utf-8", errors="replace")[-max_bytes:]
                selected.append(
                    OutputRecord(
                        seq=record.seq,
                        stream=record.stream,
                        text=text_bytes.decode("utf-8", errors="replace"),
                    )
                )
                break
            selected.append(record)
            total += size
        selected.reverse()
        return selected

    def _limit_records_from_start(self, records: list[OutputRecord], max_bytes: int) -> tuple[list[OutputRecord], bool]:
        selected: list[OutputRecord] = []
        total = 0
        truncated = False
        for record in records:
            size = utf8_len(record.text)
            if selected and total + size > max_bytes:
                truncated = True
                break
            if not selected and size > max_bytes:
                text_bytes = record.text.encode("utf-8", errors="replace")[:max_bytes]
                selected.append(
                    OutputRecord(
                        seq=record.seq,
                        stream=record.stream,
                        text=text_bytes.decode("utf-8", errors="replace"),
                    )
                )
                truncated = True
                break
            selected.append(record)
            total += size
        return selected, truncated

    def read_tail(self, max_bytes: int) -> dict[str, Any]:
        max_bytes = max(1, max_bytes)
        with self._lock:
            records = self._limit_records_from_end(list(self._tail_records), max_bytes)
            next_seq = self.next_seq
        return records_to_wire(records, next_seq, rotated=False)

    def read_from(self, since_seq: int, max_bytes: int) -> dict[str, Any]:
        max_bytes = max(1, max_bytes)
        since_seq = max(0, since_seq)
        with self._lock:
            # Reads come from the in-memory record tail. Log files are write-through
            # history, not the live read source, so rotation cannot swap the file under
            # a reader and make seq-based reads return bytes from the wrong file.
            tail_records = list(self._tail_records)
            next_seq = self.next_seq
            tail_start_seq = tail_records[0].seq if tail_records else next_seq
            rotated = since_seq < tail_start_seq
            records = [record for record in tail_records if record.seq >= max(since_seq, tail_start_seq)]
            records, truncated = self._limit_records_from_start(records, max_bytes)
            if records:
                response_next_seq = records[-1].seq + 1 if truncated else next_seq
            else:
                response_next_seq = next_seq
            status = self.status()
        payload = records_to_wire(records, response_next_seq, rotated=rotated)
        payload["truncated"] = truncated
        payload["status"] = status
        return payload

    def wait_for_output_after(self, seq: int, timeout: float, quiet: float = DEFAULT_OUTPUT_QUIET) -> None:
        end = time.time() + max(0.0, timeout)
        seen = False
        last_seq = seq
        with self._cond:
            while time.time() < end:
                remaining = end - time.time()
                if remaining <= 0:
                    break
                self._cond.wait(min(quiet, remaining))
                if self.next_seq > last_seq:
                    seen = True
                    last_seq = self.next_seq
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
        suppress_restart: bool = False,
    ) -> dict[str, Any]:
        if not self.running or self.proc is None or self.proc.stdin is None:
            if self.restart_pending:
                raise RpcError(f"{self.name} is not running; restart pending")
            raise RpcError(f"{self.name} is not running")
        payload = text + ("\n" if append_newline else "")
        job = StdinJob(payload=payload.encode("utf-8"), display_text=format_stdin_log(payload.rstrip("\n")))
        with self._lock:
            since_seq = self.next_seq
        try:
            self._stdin_queue.put_nowait(job)
        except queue.Full as exc:
            raise RpcError(f"{self.name} stdin queue is full") from exc
        if suppress_restart:
            self.suppress_restart_on_next_exit = True
            self.write_event("system", "next process exit will suppress restart")

        if not job.done.wait(max(0.0, input_wait)):
            payload_data = self.read_from(since_seq, max_bytes)
            payload_data.update(
                {
                    "queued": True,
                    "written": False,
                    "inputWaitExpired": True,
                    "suppressRestartRequested": suppress_restart,
                    "suppressRestartOnNextExit": self.suppress_restart_on_next_exit,
                }
            )
            return payload_data
        if job.error is not None:
            raise RpcError(job.error)
        if wait > 0:
            self.wait_for_output_after(job.log_end_seq if job.log_end_seq is not None else since_seq, wait, quiet)
        payload_data = self.read_from(since_seq, max_bytes)
        payload_data.update(
            {
                "queued": True,
                "written": job.written,
                "inputWaitExpired": False,
                "suppressRestartRequested": suppress_restart,
                "suppressRestartOnNextExit": self.suppress_restart_on_next_exit,
            }
        )
        return payload_data

    def _wait_for_process_exit(self, proc: subprocess.Popen[bytes], timeout: float) -> bool:
        deadline = time.time() + max(0.0, timeout)
        while time.time() < deadline:
            if proc.poll() is not None:
                return True
            time.sleep(0.05)
        return proc.poll() is not None

    def _write_shutdown_command(self) -> bool:
        if not self.shutdown_command:
            return False
        if not self.running or self.proc is None or self.proc.stdin is None:
            self.write_event("system", "shutdown command skipped: process is not running")
            return False
        payload = self.shutdown_command + "\n"
        job = StdinJob(
            payload=payload.encode("utf-8"),
            display_text=format_stdin_log(payload.rstrip("\n")),
        )
        try:
            self._stdin_queue.put_nowait(job)
        except queue.Full:
            self.write_event("system", "shutdown command skipped: stdin queue is full")
            return False
        if not job.done.wait(DEFAULT_INPUT_WAIT):
            self.write_event("system", f"shutdown command write not confirmed within {DEFAULT_INPUT_WAIT:g}s")
            return False
        if job.error is not None:
            self.write_event("system", f"shutdown command failed: {job.error}")
            return False
        if not job.written:
            self.write_event("system", "shutdown command was not written")
            return False
        self.write_event("system", "shutdown command written")
        return True

    def _stop_running_process(self, grace: float) -> None:
        proc = self.proc
        if proc is None:
            return
        if self.shutdown_command:
            if self._write_shutdown_command():
                if self._wait_for_process_exit(proc, grace):
                    return
                self.write_event("system", f"shutdown command grace expired after {grace:g}s")
                self.supervisor.stop_process(proc, self.write_event, 0.0)
                return
            self.write_event("system", "falling back to process signal stop")
        self.supervisor.stop_process(proc, self.write_event, grace)

    def stop(self, grace: float, suppress_restart: bool = False) -> dict[str, Any]:
        if not self.running or self.proc is None:
            canceled = self.cancel_pending_restart("stop requested")
            reason = "restart canceled" if canceled else "not running"
            return {"stopped": False, "reason": reason, "status": self.status()}
        if suppress_restart:
            self.suppress_restart_on_next_exit = True
            self.write_event("system", "next process exit will suppress restart")
        self._stop_running_process(grace)
        return {"stopped": True, "status": self.status()}

    def restart(self, grace: float) -> dict[str, Any]:
        if self.restart_pending:
            raise RpcError(self._pending_restart_message())
        if self.running and self.proc is not None:
            self.suppress_restart_on_next_exit = True
            self._stop_running_process(grace)
        self.start(restarted=True)
        return {"restarted": True, "status": self.status()}


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

    def _entry_matches_spec(self, entry: HostedProcess, spec: dict[str, Any]) -> bool:
        return (
            entry.argv == list(spec["argv"])
            and entry.cwd.resolve() == Path(spec["cwd"]).resolve()
            and entry.max_log_bytes == int(spec["maxLogBytes"])
            and entry.rotate_count == int(spec["rotateCount"])
            and normalize_restart_policy(entry.restart_policy) == normalize_restart_policy(spec["restart"])
            and entry.shutdown_command == spec.get("shutdownCommand")
        )

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
            return self._start(req)
        if action == "restart":
            name = sanitize_name(str(req.get("name", "")))
            entry = self.get_process(name)
            return entry.restart(require_seconds(req.get("grace", DEFAULT_STOP_GRACE), "grace", positive=False))
        if action == "forget":
            return self._forget(req)
        if action == "send":
            entry = self.get_process(str(req.get("name", "")))
            return entry.send(
                str(req.get("text", "")),
                bool(req.get("newline", req.get("appendNewline", True))),
                require_seconds(req.get("inputWait", DEFAULT_INPUT_WAIT), "inputWait", positive=False),
                require_seconds(req.get("wait", DEFAULT_OUTPUT_WAIT), "wait", positive=False),
                require_seconds(req.get("quiet", DEFAULT_OUTPUT_QUIET), "quiet", positive=False),
                require_output_bytes(req.get("maxBytes", 65536)),
                bool(req.get("suppressRestart", False)),
            )
        if action == "tail":
            entry = self.get_process(str(req.get("name", "")))
            return entry.read_tail(require_output_bytes(req.get("maxBytes", 65536)))
        if action == "read":
            entry = self.get_process(str(req.get("name", "")))
            return entry.read_from(
                require_int_at_least(req.get("sinceSeq", 0), "sinceSeq", 0),
                require_output_bytes(req.get("maxBytes", 65536)),
            )
        if action == "stop":
            entry = self.get_process(str(req.get("name", "")))
            return entry.stop(
                require_seconds(req.get("grace", DEFAULT_STOP_GRACE), "grace", positive=False),
                bool(req.get("suppressRestart", False)),
            )
        if action == "daemon-stop":
            if req.get("stopChildren", False):
                grace = require_seconds(req.get("grace", DEFAULT_STOP_GRACE), "grace", positive=False)
                for entry in list(self.processes.values()):
                    entry.cancel_pending_restart("daemon stopping")
                    entry.stop(grace, suppress_restart=True)
            self._shutdown.set()
            return {"stopping": True, "daemonPid": os.getpid()}
        raise RpcError(f"unknown action: {action}")

    def _start(self, req: dict[str, Any]) -> dict[str, Any]:
        name = sanitize_name(str(req.get("name", "")))
        old_entry = self.processes.get(name)
        has_spec = "argv" in req
        if old_entry is not None:
            if old_entry.running:
                raise RpcError(f"{name} is already running")
            if old_entry.restart_pending:
                raise RpcError(old_entry._pending_restart_message())
            if has_spec:
                spec = normalize_process_spec(
                    self.root,
                    name,
                    req.get("cwd"),
                    req.get("argv"),
                    req.get("maxLogBytes", self.max_log_bytes),
                    req.get("rotateCount", self.rotate_count),
                    req.get("restart"),
                    req.get("shutdownCommand"),
                )
                if not self._entry_matches_spec(old_entry, spec):
                    raise RpcError(f"{name} already exists with a different process spec; forget it first")
            old_entry.start()
            return {"started": True, "reused": True, "status": old_entry.status()}

        if not has_spec:
            raise RpcError(f"unknown process: {name}; provide argv or create a profile")
        spec = normalize_process_spec(
            self.root,
            name,
            req.get("cwd"),
            req.get("argv"),
            req.get("maxLogBytes", self.max_log_bytes),
            req.get("rotateCount", self.rotate_count),
            req.get("restart"),
            req.get("shutdownCommand"),
        )
        name = spec["name"]
        log_path = self.paths["logs"] / f"{name}.log"
        entry = HostedProcess(
            name=name,
            argv=list(spec["argv"]),
            cwd=Path(spec["cwd"]),
            log_path=log_path,
            supervisor=self.platform.processes,
            restart_policy=dict(spec["restart"]),
            shutdown_command=spec["shutdownCommand"],
            max_log_bytes=int(spec["maxLogBytes"]),
            rotate_count=int(spec["rotateCount"]),
        )
        try:
            entry.start()
        except Exception:
            entry.close_log()
            raise
        self.processes[name] = entry
        return {"started": True, "status": entry.status()}

    def _forget(self, req: dict[str, Any]) -> dict[str, Any]:
        name = sanitize_name(str(req.get("name", "")))
        entry = self.get_process(name)
        if entry.running:
            raise RpcError(f"{name} is running; stop it before forgetting it")
        if entry.restart_pending:
            raise RpcError(f"{name} has a restart pending; stop it before forgetting it")
        status = entry.status()
        entry.close_log()
        del self.processes[name]
        return {"forgotten": True, "name": name, "status": status}


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
    print(json.dumps(data, ensure_ascii=True, indent=2))


def write_stdout_text(text: str) -> None:
    try:
        sys.stdout.write(text)
        sys.stdout.flush()
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "utf-8"
        if hasattr(sys.stdout, "buffer"):
            sys.stdout.buffer.write(text.encode(encoding, errors="replace"))
            sys.stdout.buffer.flush()
        else:
            sys.stdout.write(text.encode(encoding, errors="replace").decode(encoding, errors="replace"))
            sys.stdout.flush()


def run_attach(
    client: ProcHostClient,
    name: str,
    history: int,
    poll: float,
    read_bytes: int,
    input_wait: float,
    drain_on_eof: float,
) -> int:
    status = client.request({"action": "status"})
    processes = status.get("processes", {})
    if not isinstance(processes, dict) or name not in processes:
        raise RpcError(f"unknown process: {name}")
    proc_status = processes[name]
    if not isinstance(proc_status, dict):
        raise RpcError(f"invalid process status for {name}")

    next_seq = int(proc_status.get("nextSeq", 0))
    tail_start_seq = int(proc_status.get("tailStartSeq", next_seq))
    since_seq = next_seq
    if history > 0:
        since_seq = tail_start_seq
        initial = client.request({"action": "read", "name": name, "sinceSeq": since_seq, "maxBytes": history})
        write_stdout_text(render_records_combined(initial, annotate_internal=True))
        since_seq = int(initial.get("nextSeq", since_seq))

    input_queue: queue.Queue[str | None] = queue.Queue()

    def read_input() -> None:
        try:
            for line in sys.stdin:
                input_queue.put(line.rstrip("\r\n"))
        finally:
            input_queue.put(None)

    threading.Thread(target=read_input, daemon=True).start()
    eof_deadline: float | None = None

    while True:
        while True:
            try:
                line = input_queue.get_nowait()
            except queue.Empty:
                break
            if line is None:
                if eof_deadline is None:
                    eof_deadline = time.time() + drain_on_eof
                continue
            try:
                response = client.request(
                    {
                        "action": "send",
                        "name": name,
                        "text": line,
                        "newline": True,
                        "inputWait": input_wait,
                        "wait": 0.0,
                        "quiet": 0.0,
                        "maxBytes": 1,
                    },
                    timeout=client.timeout + input_wait,
                )
            except Exception as exc:
                print(f"[pydaemoncontrol] input rejected: {exc}", file=sys.stderr, flush=True)
                continue
            if not response.get("written", False):
                print(
                    f"[pydaemoncontrol] input queued; write not confirmed within {input_wait:.3f}s",
                    file=sys.stderr,
                    flush=True,
                )

        data = client.request({"action": "read", "name": name, "sinceSeq": since_seq, "maxBytes": read_bytes})
        output = render_records_combined(data, annotate_internal=True)
        if output:
            write_stdout_text(output)
        since_seq = int(data.get("nextSeq", since_seq))
        proc_status = data.get("status", {})
        running = bool(proc_status.get("running", False)) if isinstance(proc_status, dict) else False
        restart_pending = (
            proc_status.get("restartPendingUntil") is not None if isinstance(proc_status, dict) else False
        )

        if eof_deadline is not None:
            if time.time() >= eof_deadline:
                return 0
        elif not running and not restart_pending:
            return 0

        time.sleep(poll)


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

    start = sub.add_parser("start", help="Start a named process or saved profile")
    add_outer_process_option_arguments(start)
    start.add_argument("tokens", nargs=argparse.REMAINDER)

    restart = sub.add_parser("restart", help="Restart an existing daemon-managed process")
    restart.add_argument("name")
    restart.add_argument("--grace", type=float, default=DEFAULT_STOP_GRACE)

    forget = sub.add_parser("forget", help="Forget a stopped process entry")
    forget.add_argument("name")

    profile = sub.add_parser("profile", help="Manage saved process profiles")
    profile_sub = profile.add_subparsers(dest="profile_cmd", required=True)
    profile_sub.add_parser("list", help="List saved profiles")
    profile_show = profile_sub.add_parser("show", help="Show one saved profile")
    profile_show.add_argument("name")
    profile_remove = profile_sub.add_parser("remove", help="Remove one saved profile")
    profile_remove.add_argument("name")
    profile_set = profile_sub.add_parser("set", help="Create or update a saved profile")
    add_outer_process_option_arguments(profile_set)
    profile_set.add_argument("tokens", nargs=argparse.REMAINDER)

    send = sub.add_parser("send", help="Send text to a process stdin")
    send.add_argument("name")
    send.add_argument("text")
    send.add_argument("--no-newline", action="store_true")
    send.add_argument("--input-wait", type=float, default=DEFAULT_INPUT_WAIT)
    send.add_argument("--wait", type=float, default=DEFAULT_OUTPUT_WAIT)
    send.add_argument("--quiet", type=float, default=DEFAULT_OUTPUT_QUIET)
    send.add_argument("--bytes", type=int, default=65536)
    send.add_argument("--suppress-restart", action="store_true")

    cmd = sub.add_parser("cmd", help="Alias for send")
    cmd.add_argument("name")
    cmd.add_argument("text")
    cmd.add_argument("--no-newline", action="store_true")
    cmd.add_argument("--input-wait", type=float, default=DEFAULT_INPUT_WAIT)
    cmd.add_argument("--wait", type=float, default=DEFAULT_OUTPUT_WAIT)
    cmd.add_argument("--quiet", type=float, default=DEFAULT_OUTPUT_QUIET)
    cmd.add_argument("--bytes", type=int, default=65536)
    cmd.add_argument("--suppress-restart", action="store_true")

    tail = sub.add_parser("tail", help="Print recent process output")
    tail.add_argument("name")
    tail.add_argument("--bytes", type=int, default=65536)

    attach = sub.add_parser("attach", help="Attach a polling console to a named process")
    attach.add_argument("name")
    attach.add_argument("--history", type=int, default=DEFAULT_ATTACH_HISTORY_BYTES)
    attach.add_argument("--poll", type=float, default=DEFAULT_ATTACH_POLL)
    attach.add_argument("--bytes", type=int, default=65536)
    attach.add_argument("--input-wait", type=float, default=DEFAULT_INPUT_WAIT)
    attach.add_argument("--drain-on-eof", type=float, default=DEFAULT_ATTACH_DRAIN_ON_EOF)

    stop = sub.add_parser("stop", help="Stop a named process")
    stop.add_argument("name")
    stop.add_argument("--grace", type=float, default=DEFAULT_STOP_GRACE)
    stop.add_argument("--suppress-restart", action="store_true")
    return parser


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
        if args.cmd == "profile":
            profiles_doc = load_profiles(client.paths)
            profiles = profiles_doc["profiles"]
            if args.profile_cmd == "list":
                print_json({"profiles": profiles})
                return 0
            if args.profile_cmd == "show":
                print_json(get_profile(client.paths, args.name))
                return 0
            if args.profile_cmd == "remove":
                name = sanitize_name(args.name)
                if name not in profiles:
                    raise RpcError(f"unknown profile: {name}")
                del profiles[name]
                save_profiles(client.paths, profiles_doc)
                print_json({"removed": True, "name": name})
                return 0
            if args.profile_cmd == "set":
                spec_args, proc_argv = parse_process_spec_tokens(args.tokens, "pydaemoncontrol profile set")
                spec_args = apply_outer_process_options(spec_args, args)
                profile = normalize_process_spec(
                    root,
                    spec_args.name,
                    spec_args.cwd,
                    proc_argv,
                    spec_args.max_log_bytes,
                    spec_args.rotate_count,
                    restart_policy_from_args(spec_args),
                    spec_args.shutdown_command,
                )
                profiles[profile["name"]] = profile
                save_profiles(client.paths, profiles_doc)
                print_json({"saved": True, "profile": profile})
                return 0
            raise RpcError(f"unknown profile command: {args.profile_cmd}")
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
        if args.cmd == "start":
            spec_args, proc_argv = parse_process_spec_tokens(
                args.tokens,
                "pydaemoncontrol start",
            )
            spec_args = apply_outer_process_options(spec_args, args)
            if proc_argv:
                start_request = profile_to_start_request(
                    normalize_process_spec(
                        root,
                        spec_args.name,
                        spec_args.cwd,
                        proc_argv,
                        spec_args.max_log_bytes,
                        spec_args.rotate_count,
                        restart_policy_from_args(spec_args),
                        spec_args.shutdown_command,
                    )
                )
            else:
                try:
                    start_request = profile_to_start_request(get_profile(client.paths, spec_args.name))
                except RpcError as exc:
                    if not str(exc).startswith("unknown profile:"):
                        raise
                    if not client.daemon_running():
                        raise
                    start_request = {
                        "action": "start",
                        "name": sanitize_name(spec_args.name),
                    }
            if not client.daemon_running():
                client.start_daemon(max_log_bytes, rotate_count)
            print_json(
                client.request(
                    start_request,
                    timeout=client.timeout,
                )
            )
            return 0
        if args.cmd == "forget":
            print_json(
                client.request(
                    {
                        "action": "forget",
                        "name": args.name,
                    },
                    timeout=client.timeout,
                )
            )
            return 0
        if args.cmd == "restart":
            grace = require_seconds(args.grace, "grace", positive=False)
            print_json(
                client.request(
                    {
                        "action": "restart",
                        "name": args.name,
                        "grace": grace,
                    },
                    timeout=client.timeout + grace,
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
                        "suppressRestart": bool(args.suppress_restart),
                    },
                    timeout=client.timeout + input_wait + wait,
                )
            )
            return 0
        if args.cmd == "tail":
            max_bytes = require_output_bytes(args.bytes)
            data = client.request({"action": "tail", "name": args.name, "maxBytes": max_bytes})
            print_json(data)
            return 0
        if args.cmd == "attach":
            history = require_history_bytes(args.history)
            poll = require_seconds(args.poll, "poll", positive=True)
            read_bytes = require_output_bytes(args.bytes)
            input_wait = require_seconds(args.input_wait, "input-wait", positive=False)
            drain_on_eof = require_seconds(args.drain_on_eof, "drain-on-eof", positive=False)
            return run_attach(client, args.name, history, poll, read_bytes, input_wait, drain_on_eof)
        if args.cmd == "stop":
            grace = require_seconds(args.grace, "grace", positive=False)
            print_json(
                client.request(
                    {
                        "action": "stop",
                        "name": args.name,
                        "grace": grace,
                        "suppressRestart": bool(args.suppress_restart),
                    },
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

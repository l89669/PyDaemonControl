import concurrent.futures
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "pydaemoncontrol.py"
sys.path.insert(0, str(REPO_ROOT))
import pydaemoncontrol as pdc  # noqa: E402


class PyDaemonControlTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="pydaemoncontrol-test-")
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.run_ctl("daemon-stop", "--stop-children", "--grace", "1", check=False, timeout=5)
        time.sleep(0.7)
        self.tempdir.cleanup()

    def run_ctl(self, *args: str, check: bool = True, timeout: float = 15) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(self.root), *args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        if check and proc.returncode != 0:
            self.fail(f"command failed: {args}\nstdout={proc.stdout}\nstderr={proc.stderr}")
        return proc

    def run_json(self, *args: str, timeout: float = 15) -> dict:
        proc = self.run_ctl(*args, timeout=timeout)
        return json.loads(proc.stdout)

    def combined(self, response: dict) -> str:
        return pdc.render_records_combined(response)

    def stream_text(self, response: dict, stream: str) -> str:
        return "".join(str(item[1]) for item in response.get(stream, []))

    def start_echo_process(self) -> None:
        code = (
            "import sys\n"
            "print('READY', flush=True)\n"
            "for line in sys.stdin:\n"
            "    line = line.rstrip('\\n')\n"
            "    if line == 'exit':\n"
            "        print('BYE', flush=True)\n"
            "        break\n"
            "    print('ECHO:' + line, flush=True)\n"
        )
        self.run_ctl("start", "echo", "--", sys.executable, "-u", "-c", code)

    def wait_process_status(self, name: str, predicate: Any, timeout: float = 5.0) -> dict:
        client = pdc.ProcHostClient(self.root, SCRIPT, timeout=2)
        deadline = time.time() + timeout
        status: dict = {}
        while time.time() < deadline:
            status = client.request({"action": "status"})
            proc_status = status["processes"].get(name, {})
            if predicate(proc_status):
                return proc_status
            time.sleep(0.05)
        self.fail(f"timed out waiting for {name}: {status}")

    def start_flapping_process(self, name: str, delay: str = "0.5") -> None:
        code = (
            "import pathlib, sys\n"
            f"path = pathlib.Path('{name}-attempts.txt')\n"
            "count = int(path.read_text()) if path.exists() else 0\n"
            "path.write_text(str(count + 1))\n"
            "print('ATTEMPT:%d' % (count + 1), flush=True)\n"
            "if count == 0:\n"
            "    sys.exit(7)\n"
            "print('RESTARTED', flush=True)\n"
            "for line in sys.stdin:\n"
            "    print('AFTER_RESTART:' + line.strip(), flush=True)\n"
        )
        self.run_ctl(
            "start",
            name,
            "--restart",
            "on-failure",
            "--restart-delay",
            delay,
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )

    def graceful_shutdown_code(self, marker_name: str, *, keep_running_after_command: bool = False) -> str:
        if keep_running_after_command:
            return (
                "import pathlib, sys, time\n"
                "print('READY', flush=True)\n"
                "for line in sys.stdin:\n"
                "    if line.strip() == 'quit':\n"
                f"        pathlib.Path('{marker_name}').write_text('ignored')\n"
                "        print('IGNORED_SHUTDOWN', flush=True)\n"
                "        time.sleep(30)\n"
            )
        return (
            "import pathlib, sys\n"
            "print('READY', flush=True)\n"
            "for line in sys.stdin:\n"
            "    if line.strip() == 'quit':\n"
            f"        pathlib.Path('{marker_name}').write_text('graceful')\n"
            "        print('GRACEFUL_SHUTDOWN', flush=True)\n"
            "        sys.exit(0)\n"
        )

    def run_with_stdio_encoding(
        self,
        encoding: str,
        *args: str,
        input_text: str | None = None,
        timeout: float = 15,
    ) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = encoding
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(self.root), *args],
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=env,
        )

    def test_single_daemon_per_root(self) -> None:
        first = self.run_json("daemon-start")
        second = self.run_json("daemon-start")
        self.assertEqual(first["pid"], second["pid"])

    def test_forget_closes_stopped_log_handles(self) -> None:
        daemon = pdc.ProcHostDaemon(self.root, max_log_bytes=1024 * 1024, rotate_count=1)
        entry = pdc.HostedProcess(
            name="svc",
            argv=[sys.executable, "-c", "pass"],
            cwd=self.root,
            log_path=daemon.paths["logs"] / "svc.log",
            supervisor=daemon.platform.processes,
        )
        entry.write_event("system", "old entry")
        self.assertTrue(entry._fds)
        daemon.processes["svc"] = entry

        response = daemon.handle({"action": "forget", "name": "svc"})

        self.assertTrue(response["forgotten"])
        self.assertNotIn("svc", daemon.processes)
        self.assertTrue(entry._logs_closed)
        self.assertFalse(entry._fds)

    def test_failed_start_closes_new_log_handles(self) -> None:
        daemon = pdc.ProcHostDaemon(self.root, max_log_bytes=1024 * 1024, rotate_count=1)
        created: list[pdc.HostedProcess] = []
        original_start = pdc.HostedProcess.start

        def fake_start(entry: pdc.HostedProcess, restarted: bool = False) -> None:
            created.append(entry)
            entry.write_event("system", "about to fail")
            raise RuntimeError("boom")

        try:
            pdc.HostedProcess.start = fake_start
            with self.assertRaises(RuntimeError):
                daemon._start(
                    {
                        "name": "svc",
                        "cwd": str(self.root),
                        "argv": [sys.executable, "-c", "pass"],
                        "maxLogBytes": 1024 * 1024,
                        "rotateCount": 1,
                    }
                )
        finally:
            pdc.HostedProcess.start = original_start

        self.assertNotIn("svc", daemon.processes)
        self.assertEqual(1, len(created))
        self.assertTrue(created[0]._logs_closed)
        self.assertFalse(created[0]._fds)

    def test_profile_set_show_start_and_remove(self) -> None:
        code = (
            "import sys\n"
            "print('PROFILE_READY', flush=True)\n"
            "for line in sys.stdin:\n"
            "    print('PROFILE:' + line.strip(), flush=True)\n"
        )
        saved = self.run_json(
            "profile",
            "set",
            "profiled",
            "--restart",
            "never",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )
        self.assertTrue(saved["saved"])
        self.assertEqual("profiled", saved["profile"]["name"])

        shown = self.run_json("profile", "show", "profiled")
        self.assertEqual(shown["argv"], [sys.executable, "-u", "-c", code])

        self.run_json("start", "profiled")
        response = self.run_json("cmd", "profiled", "ok", "--wait", "1", "--bytes", "4096")
        self.assertIn("PROFILE:ok", self.stream_text(response, "stdout"))

        removed = self.run_json("profile", "remove", "profiled")
        self.assertTrue(removed["removed"])

    def test_profile_shutdown_command_stops_process_gracefully(self) -> None:
        code = self.graceful_shutdown_code("profile-graceful.txt")
        saved = self.run_json(
            "profile",
            "set",
            "profilegrace",
            "--shutdown-command",
            "quit",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )
        self.assertEqual("quit", saved["profile"]["shutdownCommand"])
        self.run_json("start", "profilegrace")

        stopped = self.run_json("stop", "profilegrace", "--grace", "2", "--suppress-restart", timeout=6)

        self.assertTrue(stopped["stopped"])
        self.assertEqual("graceful", (self.root / "profile-graceful.txt").read_text())
        proc_status = self.run_json("status")["processes"]["profilegrace"]
        self.assertFalse(proc_status["running"])
        self.assertEqual("quit", proc_status["shutdownCommand"])
        tail = self.run_json("tail", "profilegrace", "--bytes", "4096")
        self.assertIn("quit\n", self.stream_text(tail, "stdin"))
        self.assertIn("shutdown command written", self.stream_text(tail, "system"))

    def test_one_shot_shutdown_command_is_part_of_fixed_spec(self) -> None:
        code = self.graceful_shutdown_code("oneshot-graceful.txt")
        self.run_json(
            "start",
            "oneshotgrace",
            "--shutdown-command",
            "quit",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )
        self.run_json("stop", "oneshotgrace", "--grace", "2", "--suppress-restart", timeout=6)

        proc = self.run_ctl(
            "start",
            "oneshotgrace",
            "--shutdown-command",
            "other",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
            check=False,
            timeout=5,
        )
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("different process spec", proc.stderr)

    def test_one_shot_start_reuses_stopped_entry_and_rejects_replacement(self) -> None:
        code = (
            "import pathlib\n"
            "path = pathlib.Path('one-shot-count.txt')\n"
            "count = int(path.read_text()) if path.exists() else 0\n"
            "path.write_text(str(count + 1))\n"
            "print('ONE_SHOT:%d' % (count + 1), flush=True)\n"
        )
        self.run_json("start", "oneshot", "--", sys.executable, "-u", "-c", code)
        self.wait_process_status("oneshot", lambda item: item.get("running") is False)

        reused = self.run_json("start", "oneshot")
        self.assertTrue(reused["reused"])
        self.wait_process_status("oneshot", lambda item: item.get("running") is False)
        self.assertEqual("2", (self.root / "one-shot-count.txt").read_text())

        proc = self.run_ctl(
            "start",
            "oneshot",
            "--",
            sys.executable,
            "-c",
            "import pathlib; pathlib.Path('replacement-ran.txt').write_text('bad')",
            check=False,
            timeout=5,
        )
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("different process spec", proc.stderr)
        self.assertFalse((self.root / "replacement-ran.txt").exists())

    def test_profile_change_requires_forget_before_new_spec_is_used(self) -> None:
        old_code = "import time; print('OLD_PROFILE', flush=True); time.sleep(30)"
        new_code = "import time; print('NEW_PROFILE', flush=True); time.sleep(30)"
        self.run_json("profile", "set", "profiled", "--", sys.executable, "-u", "-c", old_code)
        self.run_json("start", "profiled")
        self.run_json("stop", "profiled", "--grace", "0.2", "--suppress-restart", timeout=5)
        self.wait_process_status("profiled", lambda item: item.get("running") is False)

        self.run_json("profile", "set", "profiled", "--", sys.executable, "-u", "-c", new_code)
        proc = self.run_ctl("start", "profiled", check=False, timeout=5)
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("different process spec", proc.stderr)

        forgotten = self.run_json("forget", "profiled")
        self.assertTrue(forgotten["forgotten"])
        self.run_json("start", "profiled")
        deadline = time.time() + 5
        tail: dict = {}
        while time.time() < deadline:
            tail = self.run_json("tail", "profiled", "--bytes", "4096")
            if "NEW_PROFILE" in self.stream_text(tail, "stdout"):
                break
            time.sleep(0.05)
        else:
            self.fail(f"NEW_PROFILE did not appear in tail: {tail}")
        self.assertIn("NEW_PROFILE", self.stream_text(tail, "stdout"))
        self.assertNotIn("OLD_PROFILE", self.stream_text(tail, "stdout"))

    def test_restart_rejects_new_argv_and_keeps_process_running(self) -> None:
        self.start_echo_process()
        proc = self.run_ctl(
            "restart",
            "echo",
            "--",
            sys.executable,
            "-c",
            "raise SystemExit(9)",
            check=False,
            timeout=5,
        )
        self.assertNotEqual(0, proc.returncode)
        self.assertIn("unrecognized arguments", proc.stderr)

        response = self.run_json("cmd", "echo", "still-here", "--wait", "1", "--bytes", "4096")
        self.assertIn("ECHO:still-here", self.stream_text(response, "stdout"))

    def test_restart_rpc_ignores_spec_fields(self) -> None:
        daemon = pdc.ProcHostDaemon(self.root, max_log_bytes=1024 * 1024, rotate_count=1)
        original_argv = [sys.executable, "-c", "print('original')"]
        entry = pdc.HostedProcess(
            name="svc",
            argv=original_argv,
            cwd=self.root,
            log_path=daemon.paths["logs"] / "svc.log",
            supervisor=daemon.platform.processes,
        )
        daemon.processes["svc"] = entry
        started_argvs: list[list[str]] = []
        original_start = pdc.HostedProcess.start

        def fake_start(process: pdc.HostedProcess, restarted: bool = False) -> None:
            started_argvs.append(list(process.argv))
            process.write_event("system", "fake restart")

        try:
            pdc.HostedProcess.start = fake_start
            response = daemon.handle(
                {
                    "action": "restart",
                    "name": "svc",
                    "argv": ["definitely-not-the-command"],
                    "cwd": str(self.root / "wrong"),
                    "restart": {"mode": "always"},
                    "maxLogBytes": 1,
                    "rotateCount": 0,
                    "grace": 0,
                }
            )
        finally:
            pdc.HostedProcess.start = original_start
            entry.close_log()

        self.assertTrue(response["restarted"])
        self.assertEqual([original_argv], started_argvs)
        self.assertEqual(original_argv, response["status"]["argv"])

    def test_restart_uses_existing_process_spec(self) -> None:
        code = (
            "import pathlib, sys\n"
            "path = pathlib.Path('restart-count.txt')\n"
            "count = int(path.read_text()) if path.exists() else 0\n"
            "count += 1\n"
            "path.write_text(str(count))\n"
            "print('START:%d' % count, flush=True)\n"
            "for line in sys.stdin:\n"
            "    print('ECHO%d:%s' % (count, line.strip()), flush=True)\n"
        )
        self.run_ctl("start", "reuser", "--", sys.executable, "-u", "-c", code)
        first = self.run_json("cmd", "reuser", "one", "--wait", "1", "--bytes", "4096")
        self.assertIn("ECHO1:one", self.stream_text(first, "stdout"))

        restarted = self.run_json("restart", "reuser", "--grace", "0.2", timeout=5)
        self.assertTrue(restarted["restarted"])
        second = self.run_json("cmd", "reuser", "two", "--wait", "1", "--bytes", "4096")
        self.assertIn("ECHO2:two", self.stream_text(second, "stdout"))
        self.assertEqual("2", (self.root / "restart-count.txt").read_text())

    def test_restart_uses_shutdown_command_before_starting_again(self) -> None:
        code = (
            "import pathlib, sys\n"
            "starts = pathlib.Path('restart-grace-starts.txt')\n"
            "count = int(starts.read_text()) if starts.exists() else 0\n"
            "starts.write_text(str(count + 1))\n"
            "print('START:%d' % (count + 1), flush=True)\n"
            "for line in sys.stdin:\n"
            "    if line.strip() == 'quit':\n"
            "        pathlib.Path('restart-graceful.txt').write_text('graceful')\n"
            "        print('GRACEFUL_RESTART', flush=True)\n"
            "        sys.exit(0)\n"
        )
        self.run_json(
            "start",
            "restartgrace",
            "--shutdown-command",
            "quit",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )

        restarted = self.run_json("restart", "restartgrace", "--grace", "2", timeout=6)

        self.assertTrue(restarted["restarted"])
        self.assertEqual("graceful", (self.root / "restart-graceful.txt").read_text())
        self.wait_process_status(
            "restartgrace",
            lambda item: item.get("running") is True and item.get("restartCount") == 1,
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            if (self.root / "restart-grace-starts.txt").read_text() == "2":
                break
            time.sleep(0.05)
        else:
            self.fail("restartgrace did not start a second time")
        self.assertEqual("2", (self.root / "restart-grace-starts.txt").read_text())

    def test_on_failure_restart_policy_restarts_process(self) -> None:
        code = (
            "import pathlib, sys\n"
            "path = pathlib.Path('attempt.txt')\n"
            "count = int(path.read_text()) if path.exists() else 0\n"
            "path.write_text(str(count + 1))\n"
            "print('ATTEMPT:%d' % (count + 1), flush=True)\n"
            "if count == 0:\n"
            "    sys.exit(7)\n"
            "for line in sys.stdin:\n"
            "    print('AFTER_RESTART:' + line.strip(), flush=True)\n"
        )
        self.run_ctl(
            "start",
            "flaky",
            "--restart",
            "on-failure",
            "--restart-delay",
            "0.1",
            "--restart-max-attempts",
            "3",
            "--restart-window",
            "5",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )

        client = pdc.ProcHostClient(self.root, SCRIPT, timeout=2)
        deadline = time.time() + 5
        status = {}
        while time.time() < deadline:
            status = client.request({"action": "status"})
            proc_status = status["processes"]["flaky"]
            if proc_status["running"] and proc_status["restartCount"] >= 1:
                break
            time.sleep(0.1)
        else:
            self.fail(f"process did not restart: {status}")

        response = self.run_json("cmd", "flaky", "ok", "--wait", "1", "--bytes", "4096")
        self.assertIn("AFTER_RESTART:ok", self.stream_text(response, "stdout"))
        self.assertEqual("2", (self.root / "attempt.txt").read_text())

    def test_stop_cancels_pending_restart(self) -> None:
        self.start_flapping_process("flap", delay="1.0")
        self.wait_process_status("flap", lambda item: item.get("restartPendingUntil") is not None)

        stopped = self.run_json("stop", "flap", "--grace", "0.1", timeout=5)
        self.assertFalse(stopped["stopped"])
        self.assertEqual("restart canceled", stopped["reason"])
        time.sleep(1.3)

        proc_status = self.run_json("status")["processes"]["flap"]
        self.assertFalse(proc_status["running"])
        self.assertIsNone(proc_status["restartPendingUntil"])
        self.assertEqual(0, proc_status["restartCount"])
        self.assertEqual("1", (self.root / "flap-attempts.txt").read_text())

    def test_pending_restart_rejects_send_start_and_restart(self) -> None:
        self.start_flapping_process("pending", delay="1.0")
        self.wait_process_status("pending", lambda item: item.get("restartPendingUntil") is not None)

        send = self.run_ctl("cmd", "pending", "should-not-queue", check=False, timeout=5)
        self.assertNotEqual(0, send.returncode)
        self.assertIn("restart pending", send.stderr)

        start = self.run_ctl(
            "start",
            "pending",
            "--",
            sys.executable,
            "-c",
            "print('replacement')",
            check=False,
            timeout=5,
        )
        self.assertNotEqual(0, start.returncode)
        self.assertIn("restart pending", start.stderr)

        restart = self.run_ctl("restart", "pending", "--grace", "0.1", check=False, timeout=5)
        self.assertNotEqual(0, restart.returncode)
        self.assertIn("restart pending", restart.stderr)

        self.wait_process_status(
            "pending",
            lambda item: item.get("running") is True and item.get("restartCount") == 1,
            timeout=5,
        )
        response = self.run_json("cmd", "pending", "after", "--wait", "1", "--bytes", "4096")
        stdout = self.stream_text(response, "stdout")
        self.assertIn("AFTER_RESTART:after", stdout)
        self.assertNotIn("should-not-queue", stdout)

    def test_daemon_stop_cancels_pending_restart(self) -> None:
        self.start_flapping_process("daemonpending", delay="1.0")
        self.wait_process_status("daemonpending", lambda item: item.get("restartPendingUntil") is not None)

        stopped = self.run_json("daemon-stop", "--stop-children", "--grace", "0.1", timeout=5)
        self.assertTrue(stopped["stopping"])
        time.sleep(1.3)

        self.assertEqual("1", (self.root / "daemonpending-attempts.txt").read_text())

    def test_daemon_stop_uses_shutdown_command_for_running_children(self) -> None:
        code = self.graceful_shutdown_code("daemon-graceful.txt")
        self.run_json(
            "start",
            "daemongrace",
            "--shutdown-command",
            "quit",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )

        stopped = self.run_json("daemon-stop", "--stop-children", "--grace", "2", timeout=6)
        time.sleep(0.5)

        self.assertTrue(stopped["stopping"])
        self.assertEqual("graceful", (self.root / "daemon-graceful.txt").read_text())

    def test_shutdown_command_timeout_falls_back_to_force_stop(self) -> None:
        code = self.graceful_shutdown_code("ignored-shutdown.txt", keep_running_after_command=True)
        self.run_json(
            "start",
            "ignoregrace",
            "--shutdown-command",
            "quit",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )

        stopped = self.run_json("stop", "ignoregrace", "--grace", "0.2", "--suppress-restart", timeout=6)

        self.assertTrue(stopped["stopped"])
        self.assertEqual("ignored", (self.root / "ignored-shutdown.txt").read_text())
        proc_status = self.run_json("status")["processes"]["ignoregrace"]
        self.assertFalse(proc_status["running"])
        tail = self.run_json("tail", "ignoregrace", "--bytes", "4096")
        self.assertIn("shutdown command grace expired", self.stream_text(tail, "system"))

    def test_stop_suppress_restart_blocks_always_restart_once(self) -> None:
        code = "import time; print('SLEEPING', flush=True); time.sleep(30)"
        self.run_ctl(
            "start",
            "sleeper",
            "--restart",
            "always",
            "--restart-delay",
            "0.1",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )
        self.run_json("stop", "sleeper", "--grace", "0.2", "--suppress-restart", timeout=5)
        time.sleep(0.6)
        status = self.run_json("status")
        proc_status = status["processes"]["sleeper"]
        self.assertFalse(proc_status["running"])
        self.assertEqual(0, proc_status["restartCount"])
        self.assertFalse(proc_status["suppressRestartOnNextExit"])

    def test_stop_without_suppress_restart_follows_always_policy(self) -> None:
        code = "import time; print('SLEEPING', flush=True); time.sleep(30)"
        self.run_ctl(
            "start",
            "sleeper",
            "--restart",
            "always",
            "--restart-delay",
            "0.1",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )
        self.run_json("stop", "sleeper", "--grace", "0.2", timeout=5)
        deadline = time.time() + 5
        status = {}
        while time.time() < deadline:
            status = self.run_json("status")
            proc_status = status["processes"]["sleeper"]
            if proc_status["running"] and proc_status["restartCount"] >= 1:
                break
            time.sleep(0.1)
        else:
            self.fail(f"process did not restart after unsuppressed stop: {status}")

    def test_cmd_suppress_restart_blocks_next_exit_once(self) -> None:
        code = (
            "import sys\n"
            "print('READY', flush=True)\n"
            "for line in sys.stdin:\n"
            "    if line.strip() == 'exit':\n"
            "        print('EXITING', flush=True)\n"
            "        sys.exit(3)\n"
            "    print('ECHO:' + line.strip(), flush=True)\n"
        )
        self.run_ctl(
            "start",
            "commanded",
            "--restart",
            "always",
            "--restart-delay",
            "0.1",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )
        response = self.run_json(
            "cmd",
            "commanded",
            "exit",
            "--suppress-restart",
            "--wait",
            "1",
            "--bytes",
            "4096",
        )
        self.assertTrue(response["suppressRestartRequested"])
        self.assertIn("EXITING", self.stream_text(response, "stdout"))
        time.sleep(0.6)
        status = self.run_json("status")
        proc_status = status["processes"]["commanded"]
        self.assertFalse(proc_status["running"])
        self.assertEqual(0, proc_status["restartCount"])
        self.assertFalse(proc_status["suppressRestartOnNextExit"])

    def test_cmd_tail_and_clean_exit(self) -> None:
        self.start_echo_process()
        response = self.run_json("cmd", "echo", "hello", "--wait", "1", "--bytes", "4096")
        self.assertIn("ECHO:hello", self.stream_text(response, "stdout"))

        tail = self.run_json("tail", "echo", "--bytes", "4096")
        self.assertIn("READY", self.stream_text(tail, "stdout"))
        self.assertIn("ECHO:hello", self.stream_text(tail, "stdout"))

        response = self.run_json("cmd", "echo", "exit", "--wait", "1", "--bytes", "4096")
        self.assertIn("BYE", self.stream_text(response, "stdout"))

    def test_json_output_is_ascii_safe_for_restricted_stdout_encoding(self) -> None:
        text = "UNICODE: 中文 🚀"
        code = f"import sys; sys.stdout.buffer.write(({text!r} + '\\n').encode('utf-8')); sys.stdout.flush()"
        self.run_json("start", "unicodejson", "--", sys.executable, "-u", "-c", code)
        self.wait_process_status("unicodejson", lambda item: item.get("running") is False)

        deadline = time.time() + 5
        proc: subprocess.CompletedProcess[str] | None = None
        payload: dict = {}
        while time.time() < deadline:
            proc = self.run_with_stdio_encoding(
                "cp1252",
                "tail",
                "unicodejson",
                "--bytes",
                "4096",
                timeout=5,
            )
            self.assertEqual(0, proc.returncode, proc.stderr)
            payload = json.loads(proc.stdout)
            if text in self.stream_text(payload, "stdout"):
                break
            time.sleep(0.05)
        else:
            self.fail(f"unicode output did not appear in tail: {payload}")

        self.assertIsNotNone(proc)
        self.assertNotIn(text, proc.stdout)
        self.assertIn(text, self.stream_text(payload, "stdout"))

    def test_stdout_stderr_are_split_with_global_seq(self) -> None:
        code = (
            "import sys, time\n"
            "print('OUT1', flush=True)\n"
            "time.sleep(0.1)\n"
            "print('ERR1', file=sys.stderr, flush=True)\n"
            "time.sleep(0.1)\n"
            "print('OUT2', flush=True)\n"
            "time.sleep(1)\n"
        )
        self.run_ctl("start", "mixed", "--", sys.executable, "-u", "-c", code)
        time.sleep(0.5)

        tail = self.run_json("tail", "mixed", "--bytes", "4096")
        stdout = self.stream_text(tail, "stdout")
        stderr = self.stream_text(tail, "stderr")
        self.assertIn("OUT1", stdout)
        self.assertIn("OUT2", stdout)
        self.assertIn("ERR1", stderr)
        self.assertNotIn("ERR1", stdout)
        self.assertNotIn("OUT1", stderr)

        records = pdc.iter_wire_records(tail)
        seqs = [record.seq for record in records]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(seqs), len(set(seqs)))
        combined = pdc.render_records_combined(tail)
        self.assertLess(combined.index("OUT1"), combined.index("ERR1"))
        self.assertLess(combined.index("ERR1"), combined.index("OUT2"))

    def test_read_from_ignores_replaced_log_file_after_rotation(self) -> None:
        entry = pdc.HostedProcess(
            name="rot",
            argv=[sys.executable, "-c", "pass"],
            cwd=self.root,
            log_path=self.root / ".pydaemoncontrol" / "logs" / "rot.log",
            supervisor=pdc.PlatformServices(pdc.state_paths(self.root)).processes,
            max_log_bytes=80,
            rotate_count=1,
        )
        try:
            entry.write_records("stdout", ["A" * 120])
            since_seq = entry.next_seq
            entry.write_records("stdout", ["EXPECTED\n"])
            entry._stream_log_path("stdout").write_text(
                json.dumps([9999, "WRONG\n"]) + "\n",
                encoding="utf-8",
            )

            response = entry.read_from(since_seq, 4096)
        finally:
            entry.close_log()

        self.assertIn("EXPECTED", self.stream_text(response, "stdout"))
        self.assertNotIn("WRONG", self.stream_text(response, "stdout"))

    def test_read_action_advances_seq_without_repeating(self) -> None:
        self.start_echo_process()
        client = pdc.ProcHostClient(self.root, SCRIPT, timeout=2)
        status = client.request({"action": "status"})
        since_seq = status["processes"]["echo"]["nextSeq"]

        self.run_json("cmd", "echo", "first", "--wait", "1", "--bytes", "4096")
        first = client.request({"action": "read", "name": "echo", "sinceSeq": since_seq, "maxBytes": 4096})
        self.assertIn("ECHO:first", self.stream_text(first, "stdout"))
        self.assertGreater(first["nextSeq"], since_seq)

        empty = client.request({"action": "read", "name": "echo", "sinceSeq": first["nextSeq"], "maxBytes": 4096})
        self.assertEqual("", self.combined(empty))
        self.assertEqual(first["nextSeq"], empty["nextSeq"])

        self.run_json("cmd", "echo", "second", "--wait", "1", "--bytes", "4096")
        second = client.request({"action": "read", "name": "echo", "sinceSeq": empty["nextSeq"], "maxBytes": 4096})
        self.assertNotIn("ECHO:first", self.stream_text(second, "stdout"))
        self.assertIn("ECHO:second", self.stream_text(second, "stdout"))

    def test_attach_accepts_piped_input_and_detaches_on_eof(self) -> None:
        self.start_echo_process()
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--root",
                str(self.root),
                "attach",
                "echo",
                "--history",
                "0",
                "--poll",
                "0.05",
                "--drain-on-eof",
                "1",
            ],
            input="hello\nexit\n",
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
        if proc.returncode != 0:
            self.fail(f"attach failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        self.assertIn("ECHO:hello", proc.stdout)
        self.assertIn("BYE", proc.stdout)

    def test_attach_does_not_crash_on_restricted_stdout_encoding(self) -> None:
        text = "UNICODE: 中文 🚀"
        code = (
            f"import sys, time; sys.stdout.buffer.write(({text!r} + '\\n').encode('utf-8')); "
            "sys.stdout.flush(); time.sleep(1)"
        )
        self.run_json("start", "unicodeattach", "--", sys.executable, "-u", "-c", code)

        proc = self.run_with_stdio_encoding(
            "cp1252",
            "attach",
            "unicodeattach",
            "--history",
            "4096",
            "--poll",
            "0.05",
            "--drain-on-eof",
            "0.2",
            input_text="",
            timeout=10,
        )

        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn("UNICODE:", proc.stdout)

    def test_attach_rejects_input_while_restart_is_pending(self) -> None:
        self.start_flapping_process("attachpending", delay="0.5")
        self.wait_process_status("attachpending", lambda item: item.get("restartPendingUntil") is not None)

        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--root",
                str(self.root),
                "attach",
                "attachpending",
                "--history",
                "0",
                "--poll",
                "0.05",
                "--drain-on-eof",
                "1",
            ],
            input="during-pending\n",
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
        if proc.returncode != 0:
            self.fail(f"attach failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        self.assertIn("input rejected", proc.stderr)
        self.assertNotIn("AFTER_RESTART:during-pending", proc.stdout)

    def test_concurrent_cmd_writes_do_not_interleave_stdin(self) -> None:
        self.start_echo_process()

        def send(index: int) -> str:
            token = f"msg-{index:02d}"
            return self.stream_text(self.run_json("cmd", "echo", token, "--wait", "1", "--bytes", "4096"), "stdout")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outputs = list(pool.map(send, range(16)))

        combined = "\n".join(outputs)
        for index in range(16):
            self.assertIn(f"ECHO:msg-{index:02d}", combined)

    def test_large_output_is_bounded_and_rotated(self) -> None:
        code = "import sys, time; sys.stdout.write('X' * 2500000); sys.stdout.flush(); time.sleep(0.2)"
        self.run_ctl(
            "start",
            "--max-log-bytes",
            "1048576",
            "spam",
            "--",
            sys.executable,
            "-u",
            "-c",
            code,
        )
        time.sleep(1.0)

        tail = self.run_json("tail", "spam", "--bytes", "100")
        self.assertLessEqual(pdc.record_text_bytes(pdc.iter_wire_records(tail)), 100)
        log_dir = self.root / ".pydaemoncontrol" / "logs"
        self.assertTrue((log_dir / "spam.stdout.log.1").exists())

    def test_cmd_after_process_exit_fails_without_hanging(self) -> None:
        self.run_ctl("start", "quick", "--", sys.executable, "-c", "print('done')")
        time.sleep(0.5)
        proc = self.run_ctl("cmd", "quick", "hello", "--wait", "0.2", check=False, timeout=5)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("not running", proc.stderr)

    def test_large_stdin_to_non_reader_does_not_hang_daemon(self) -> None:
        code = "import time; print('READY', flush=True); time.sleep(30)"
        self.run_ctl("start", "blocked", "--", sys.executable, "-u", "-c", code)
        client = pdc.ProcHostClient(self.root, SCRIPT, timeout=2)

        start = time.monotonic()
        response = client.request(
            {
                "action": "send",
                "name": "blocked",
                "text": "X" * (2 * 1024 * 1024),
                "newline": False,
                "inputWait": 0.1,
                "wait": 0,
                "maxBytes": 1024,
            },
            timeout=2,
        )
        elapsed = time.monotonic() - start

        self.assertLess(elapsed, 1.5)
        self.assertFalse(response["written"])
        self.assertTrue(response["inputWaitExpired"])
        status = client.request({"action": "status"}, timeout=2)
        self.assertIn("blocked", status["processes"])

    def test_input_wait_allows_slow_large_stdin_write_to_be_captured(self) -> None:
        size = 2 * 1024 * 1024
        code = (
            "import sys, time\n"
            "print('READY', flush=True)\n"
            "time.sleep(0.5)\n"
            f"data = sys.stdin.buffer.read({size})\n"
            "print('READ:%d' % len(data), flush=True)\n"
        )
        self.run_ctl("start", "slowread", "--", sys.executable, "-u", "-c", code)
        client = pdc.ProcHostClient(self.root, SCRIPT, timeout=2)

        response = client.request(
            {
                "action": "send",
                "name": "slowread",
                "text": "X" * size,
                "newline": False,
                "inputWait": 3,
                "wait": 2,
                "quiet": 0.1,
                "maxBytes": 8192,
            },
            timeout=7,
        )

        self.assertTrue(response["written"])
        self.assertFalse(response["inputWaitExpired"])
        self.assertIn(f"READ:{size}", self.stream_text(response, "stdout"))

    def test_output_byte_limit_is_rejected_before_daemon_response_grows(self) -> None:
        self.start_echo_process()
        proc = self.run_ctl(
            "tail",
            "echo",
            "--bytes",
            str(pdc.MAX_OUTPUT_BYTES + 1),
            check=False,
            timeout=5,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("maxBytes must be at most", proc.stderr)


if __name__ == "__main__":
    unittest.main()

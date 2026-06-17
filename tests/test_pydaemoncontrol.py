import concurrent.futures
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


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

    def test_single_daemon_per_root(self) -> None:
        first = self.run_json("daemon-start")
        second = self.run_json("daemon-start")
        self.assertEqual(first["pid"], second["pid"])

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
        self.assertIn("PROFILE:ok", response["output"])

        removed = self.run_json("profile", "remove", "profiled")
        self.assertTrue(removed["removed"])

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
        self.assertIn("AFTER_RESTART:ok", response["output"])
        self.assertEqual("2", (self.root / "attempt.txt").read_text())

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
        self.assertIn("EXITING", response["output"])
        time.sleep(0.6)
        status = self.run_json("status")
        proc_status = status["processes"]["commanded"]
        self.assertFalse(proc_status["running"])
        self.assertEqual(0, proc_status["restartCount"])
        self.assertFalse(proc_status["suppressRestartOnNextExit"])

    def test_cmd_tail_and_clean_exit(self) -> None:
        self.start_echo_process()
        response = self.run_json("cmd", "echo", "hello", "--wait", "1", "--bytes", "4096")
        self.assertIn("ECHO:hello", response["output"])

        tail = self.run_ctl("tail", "echo", "--bytes", "4096").stdout
        self.assertIn("READY", tail)
        self.assertIn("ECHO:hello", tail)

        response = self.run_json("cmd", "echo", "exit", "--wait", "1", "--bytes", "4096")
        self.assertIn("BYE", response["output"])

    def test_read_action_advances_offset_without_repeating(self) -> None:
        self.start_echo_process()
        client = pdc.ProcHostClient(self.root, SCRIPT, timeout=2)
        status = client.request({"action": "status"})
        offset = status["processes"]["echo"]["bytesWritten"]

        self.run_json("cmd", "echo", "first", "--wait", "1", "--bytes", "4096")
        first = client.request({"action": "read", "name": "echo", "offset": offset, "maxBytes": 4096})
        self.assertIn("ECHO:first", first["output"])
        self.assertGreater(first["nextOffset"], offset)

        empty = client.request({"action": "read", "name": "echo", "offset": first["nextOffset"], "maxBytes": 4096})
        self.assertEqual("", empty["output"])
        self.assertEqual(first["nextOffset"], empty["nextOffset"])

        self.run_json("cmd", "echo", "second", "--wait", "1", "--bytes", "4096")
        second = client.request({"action": "read", "name": "echo", "offset": empty["nextOffset"], "maxBytes": 4096})
        self.assertNotIn("ECHO:first", second["output"])
        self.assertIn("ECHO:second", second["output"])

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

    def test_concurrent_cmd_writes_do_not_interleave_stdin(self) -> None:
        self.start_echo_process()

        def send(index: int) -> str:
            token = f"msg-{index:02d}"
            return self.run_json("cmd", "echo", token, "--wait", "1", "--bytes", "4096")["output"]

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

        tail = self.run_ctl("tail", "spam", "--bytes", "100").stdout
        self.assertLessEqual(len(tail.encode("utf-8")), 512)
        log_dir = self.root / ".pydaemoncontrol" / "logs"
        self.assertTrue((log_dir / "spam.log.1").exists())

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
        self.assertIn(f"READ:{size}", response["output"])

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

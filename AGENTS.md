# AGENTS.md

## Project Shape

This project is a small Python daemon/client process controller. Keep it focused:

- one daemon per root directory
- short-lived clients that send one action to the daemon
- named child processes owned by the daemon
- stdin forwarding, bounded output capture, and tail/status/stop controls

Do not turn this into a general replacement for `systemd`, `tmux`, containers, or
a distributed process manager.

## Implementation Constraints

- Runtime dependencies should stay at zero unless there is a strong reason.
- Support Linux/POSIX and Windows. Platform differences must stay behind the
  lock, IPC endpoint, and process supervisor abstractions.
- Keep startup and client commands fast. A client should connect, issue one
  request, print a bounded response, and exit.
- Never keep unbounded process output in memory.
- Large output must go to rolling log files. Client responses must be explicitly
  size-limited.
- Preserve the model where `cmd` returns output generated after its stdin write,
  while allowing unrelated interleaved output.
- Never let a child process that stops reading stdin pin a daemon request thread
  indefinitely. Keep stdin writes behind bounded queues or bounded waits.
- Keep operation timeouts phase-specific: RPC/startup overhead, stdin write
  confirmation, post-input output capture, output quiet window, and stop grace
  are separate knobs with separate tests.

## Editing Guidance

- Prefer simple protocol changes over abstractions. The daemon and client live in
  one file intentionally.
- Do not scatter platform checks through command handling. Add or adjust the
  platform adapter layer instead.
- If you add a command, document it in `README.md`.
- If you change state layout under `.pydaemoncontrol/`, include migration or
  compatibility notes.
## Verification

Before considering a change done, run at least:

```bash
python3 -m py_compile pydaemoncontrol.py
python3 -m unittest discover -s tests -v
```

For behavior changes, smoke test:

Linux/POSIX:

```bash
python3 pydaemoncontrol.py --root /tmp/pdc-test start shell -- /bin/sh
python3 pydaemoncontrol.py --root /tmp/pdc-test cmd shell 'echo ok' --wait 1 --bytes 4096
python3 pydaemoncontrol.py --root /tmp/pdc-test tail shell --bytes 4096
python3 pydaemoncontrol.py --root /tmp/pdc-test stop shell --grace 1
python3 pydaemoncontrol.py --root /tmp/pdc-test daemon-stop --stop-children --grace 1
```

Windows:

```powershell
$root = Join-Path $env:TEMP pdc-test
python pydaemoncontrol.py --root $root start shell -- powershell -NoProfile -NoLogo -Command "while (`$line = [Console]::In.ReadLine()) { if (`$line -eq 'exit') { break }; Invoke-Expression `$line }"
python pydaemoncontrol.py --root $root cmd shell "Write-Output ok" --wait 1 --bytes 4096
python pydaemoncontrol.py --root $root tail shell --bytes 4096
python pydaemoncontrol.py --root $root cmd shell "exit" --wait 1 --bytes 4096
python pydaemoncontrol.py --root $root daemon-stop --stop-children --grace 1
```

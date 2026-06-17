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
- Support Linux/POSIX first. Unix domain sockets and `fcntl` are expected.
- Keep startup and client commands fast. A client should connect, issue one
  request, print a bounded response, and exit.
- Never keep unbounded process output in memory.
- Large output must go to rolling log files. Client responses must be explicitly
  size-limited.
- Preserve the model where `cmd` returns output generated after its stdin write,
  while allowing unrelated interleaved output.

## Editing Guidance

- Prefer simple protocol changes over abstractions. The daemon and client live in
  one file intentionally.
- If you add a command, document it in `README.md`.
- If you change state layout under `.pydaemoncontrol/`, include migration or
  compatibility notes.
## Verification

Before considering a change done, run at least:

```bash
python3 -m py_compile pydaemoncontrol.py
```

For behavior changes, smoke test:

```bash
python3 pydaemoncontrol.py --root /tmp/pdc-test start shell -- /bin/sh
python3 pydaemoncontrol.py --root /tmp/pdc-test cmd shell 'echo ok' --wait 1 --bytes 4096
python3 pydaemoncontrol.py --root /tmp/pdc-test tail shell --bytes 4096
python3 pydaemoncontrol.py --root /tmp/pdc-test stop shell --grace 1
python3 pydaemoncontrol.py --root /tmp/pdc-test daemon-stop --stop-children --grace 1
```

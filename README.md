# pydaemoncontrol

`pydaemoncontrol` is a lightweight directory-scoped daemon/client for controlling
long-running processes from short-lived commands.

It is useful when a remote execution channel is good at running one command at a
time, but bad at keeping an interactive process open. A client command starts a
single daemon for a directory, the daemon starts and owns child processes, and
later clients talk to that daemon over a local IPC endpoint.

## What It Does

- Starts one long-lived daemon per root directory.
- Starts named child processes under that daemon.
- Sends text to a child process stdin and returns the new output produced after
  that input.
- Keeps stdout and stderr in bounded rolling log files.
- Returns recent output with `tail --bytes N` without streaming large logs back
  through the caller.
- Stops a child process tree with graceful termination first, then force kill
  after a grace period.

The tool is intentionally small. It does not try to replace `systemd`, `tmux`,
or a production process supervisor. It is meant for controlled per-directory
workflows where a short-lived client needs to manage one or more long-running
backend processes.

## Install

From a checkout:

```bash
python3 -m pip install .
```

Or run the script directly:

```bash
python3 pydaemoncontrol.py --help
```

## Basic Usage

Start a process. If the directory daemon is not already running, this command
starts it first.

```bash
pydaemoncontrol --root /srv/example start app -- ./run-server.sh
```

Save a reusable process profile and start it later:

```bash
pydaemoncontrol --root /srv/example profile set app --restart on-failure --restart-delay 3 -- ./run-server.sh
pydaemoncontrol --root /srv/example start app
```

Send one line to the process stdin and return output produced after the input:

```bash
pydaemoncontrol --root /srv/example cmd app 'status' --wait 1 --bytes 12000
```

If stdin writing itself may be slow, give that phase its own budget:

```bash
pydaemoncontrol --root /srv/example cmd app 'status' --input-wait 2 --wait 1 --bytes 12000
```

If a line command is intended to make the process exit, suppress restart for
that one exit:

```bash
pydaemoncontrol --root /srv/example cmd app 'stop' --suppress-restart --wait 5 --bytes 20000
```

Read recent output without sending input:

```bash
pydaemoncontrol --root /srv/example tail app --bytes 20000
```

Attach a polling console for human use:

```bash
pydaemoncontrol --root /srv/example attach app --history 20000 --poll 0.2
```

Show daemon and process status:

```bash
pydaemoncontrol --root /srv/example status
```

Stop a child process:

```bash
pydaemoncontrol --root /srv/example stop app --grace 5 --suppress-restart
```

Stop the daemon:

```bash
pydaemoncontrol --root /srv/example daemon-stop --stop-children --grace 5
```

## Minecraft Server Example

```bash
pydaemoncontrol \
  --root /home/minecraft/server \
  profile set \
  mc \
  --restart on-failure \
  --restart-delay 10 \
  --restart-max-attempts 5 \
  --restart-window 300 \
  --cwd /home/minecraft/server \
  -- java -Xms512M -Xmx2G -jar spigot.jar nogui
```

Start the saved profile:

```bash
pydaemoncontrol --root /home/minecraft/server start mc
```

Run server console commands through the daemon:

```bash
pydaemoncontrol --root /home/minecraft/server cmd mc 'list' --wait 1 --bytes 12000
pydaemoncontrol --root /home/minecraft/server cmd mc 'say hello from pydaemoncontrol' --wait 1 --bytes 12000
pydaemoncontrol --root /home/minecraft/server cmd mc 'stop' --wait 5 --bytes 20000
```

Or attach a human-facing console:

```bash
pydaemoncontrol --root /home/minecraft/server attach mc --history 40000 --poll 0.2
```

## State And Logs

For each root directory, `pydaemoncontrol` creates:

```text
.pydaemoncontrol/
  daemon.lock
  daemon.pid
  daemon.sock
  daemon.endpoint.json
  daemon.log
  profiles.json
  logs/
    <process>.log
    <process>.log.1
    <process>.log.2
```

`daemon.sock` is used on POSIX systems. `daemon.endpoint.json` is used on
Windows and contains a loopback TCP endpoint plus a random per-daemon token.

Only one daemon can hold the lock for a root directory. Process output is written
to rolling log files. Defaults:

- max log file size: `32 MiB`
- rotated files kept: `3`
- in-memory tail cache: `256 KiB`

The daemon does not keep full process output in memory.

## Profiles

Profiles are saved per root directory in `.pydaemoncontrol/profiles.json`. A
profile records:

- process name
- working directory
- argv
- log rotation settings
- restart policy

Manage profiles with:

```bash
pydaemoncontrol --root /srv/example profile set app --restart on-failure -- ./run-server.sh
pydaemoncontrol --root /srv/example profile list
pydaemoncontrol --root /srv/example profile show app
pydaemoncontrol --root /srv/example profile remove app
```

`start app` and `restart app` use the saved profile when no argv is provided. If
argv is provided, the command remains an immediate one-shot start spec:

```bash
pydaemoncontrol --root /srv/example start app -- ./run-once.sh
```

Process spec options can be placed before or after the name:

```bash
pydaemoncontrol --root /srv/example profile set --restart on-failure app -- ./run-server.sh
pydaemoncontrol --root /srv/example profile set app --restart on-failure -- ./run-server.sh
```

## Restart Policies

Restart policy modes:

- `never`: do not auto-restart.
- `on-failure`: restart only when the process exits with a non-zero code.
- `always`: restart after any exit unless the process was stopped by
  `pydaemoncontrol stop`, `restart`, or `daemon-stop`.

Policy options:

- `--restart-delay`: seconds to wait before restarting.
- `--restart-max-attempts`: maximum restarts inside the restart window; `0`
  means unlimited.
- `--restart-window`: rolling seconds used for `--restart-max-attempts`.

For line-oriented server consoles that exit with code `0` after a normal
shutdown command, `on-failure` is usually the safer default: crashes restart,
while controlled exits stay stopped. Use `always` only when you explicitly want
the process to come back after any unsuppressed exit.

`cmd/send --suppress-restart` and `stop --suppress-restart` set a one-shot
suppression flag for the process's next exit. They do not wait for the process
to die, and they do not create a time window. `cmd/send --wait` still only
controls how much output is collected after the input write. If shutdown takes
longer, use `tail`, `read`, `status`, or `attach` to observe progress.

Without `--suppress-restart`, exits still follow the configured restart policy.
For example, a process with `--restart always` will come back after an
unsuppressed `stop`.

## Command Semantics

`cmd` records the current log offset, writes the requested text to stdin, waits
for output until a short quiet window or timeout, then returns bytes from that
offset. If other process output is emitted at the same time, it can be included
in the response. That is expected behavior.

Commands sent to the same child process are queued through a per-process stdin
writer. This prevents concurrent clients from interleaving bytes in the child
process stdin. It also keeps daemon request threads from blocking forever if a
child process stops reading stdin. In that case the response returns
`written: false`, and later `status`, `tail`, or `stop` requests can still be
served.

Large stdin values are truncated in the log, so a command response is not filled
with the input text before the child process output can be captured.

`attach` is a polling console built on the same RPC protocol. It reads output by
offset and sends each entered line through the same stdin queue as `cmd`, so it
does not block other one-shot clients. Output from other clients remains visible
in attach, and attach input remains visible to `tail` and later `cmd` responses.

This is not a true PTY or ConPTY. It is intended for line-oriented consoles such
as Minecraft server stdin, REPLs, and long-running service consoles. Full-screen
interactive programs such as shells with readline, editors, or `top` need a real
terminal backend and are outside this mode.

## Timeout Model

Timeouts are split by operation phase:

- `--timeout`: client RPC overhead and daemon startup budget.
- `--input-wait`: maximum time `send`/`cmd` waits for stdin write confirmation.
- `--wait`: maximum time after confirmed input to collect new output.
- `--quiet`: early-return quiet window after output has started.
- `--grace`: graceful stop budget before force kill.
- `attach --poll`: interval between output reads.
- `attach --drain-on-eof`: final output drain window after stdin EOF.

For `send`/`cmd`, the client request timeout is `--timeout + --input-wait +
--wait`. For `stop`, `restart`, and `daemon-stop`, it is `--timeout + --grace`.
The daemon also caps request and response bodies at `8 MiB`, and `--bytes` is
limited to `4 MiB` per response.

## Requirements

- Linux/POSIX or Windows
- Python 3.10+
- No third-party runtime dependencies

## Platform Notes

The core daemon/client flow is shared across platforms. Platform-specific code is
kept behind three small adapters:

- daemon lock: `fcntl` on POSIX, `msvcrt` file locking on Windows
- IPC endpoint: Unix domain socket on POSIX, `127.0.0.1` TCP plus token on Windows
- process control: process groups on POSIX, Windows process groups and `taskkill`
  fallback on Windows

## Tests

Run the standard-library test suite:

```bash
python3 -m unittest discover -s tests -v
```

The tests cover daemon singleton behavior, command/tail flow, concurrent client
commands, attach with piped input, offset-based reads, bounded large-output
handling with log rotation, command failure after child process exit, a
non-reading child process that would otherwise block stdin writes, slow large
stdin writes with explicit `--input-wait`, response byte limit rejection,
profiles, and on-failure restarts.

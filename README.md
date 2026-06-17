# pydaemoncontrol

`pydaemoncontrol` is a lightweight directory-scoped daemon/client for controlling
long-running processes from short-lived commands.

It is useful when a remote execution channel is good at running one command at a
time, but bad at keeping an interactive process open. A client command starts a
single daemon for a directory, the daemon starts and owns child processes, and
later clients talk to that daemon over a Unix socket.

## What It Does

- Starts one long-lived daemon per root directory.
- Starts named child processes under that daemon.
- Sends text to a child process stdin and returns the new output produced after
  that input.
- Keeps stdout and stderr in bounded rolling log files.
- Returns recent output with `tail --bytes N` without streaming large logs back
  through the caller.
- Stops a process group with `SIGTERM`, then `SIGKILL` after a grace period.

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

Send one line to the process stdin and return output produced after the input:

```bash
pydaemoncontrol --root /srv/example cmd app 'status' --wait 1 --bytes 12000
```

Read recent output without sending input:

```bash
pydaemoncontrol --root /srv/example tail app --bytes 20000
```

Show daemon and process status:

```bash
pydaemoncontrol --root /srv/example status
```

Stop a child process:

```bash
pydaemoncontrol --root /srv/example stop app --grace 5
```

Stop the daemon:

```bash
pydaemoncontrol --root /srv/example daemon-stop --stop-children --grace 5
```

## Minecraft Server Example

```bash
pydaemoncontrol \
  --root /home/minecraft/server \
  start \
  --cwd /home/minecraft/server \
  mc \
  -- java -Xms512M -Xmx2G -jar spigot.jar nogui
```

Run server console commands through the daemon:

```bash
pydaemoncontrol --root /home/minecraft/server cmd mc 'list' --wait 1 --bytes 12000
pydaemoncontrol --root /home/minecraft/server cmd mc 'say hello from pydaemoncontrol' --wait 1 --bytes 12000
pydaemoncontrol --root /home/minecraft/server cmd mc 'stop' --wait 5 --bytes 20000
```

## State And Logs

For each root directory, `pydaemoncontrol` creates:

```text
.pydaemoncontrol/
  daemon.lock
  daemon.pid
  daemon.sock
  daemon.log
  logs/
    <process>.log
    <process>.log.1
    <process>.log.2
```

Only one daemon can hold the lock for a root directory. Process output is written
to rolling log files. Defaults:

- max log file size: `32 MiB`
- rotated files kept: `3`
- in-memory tail cache: `256 KiB`

The daemon does not keep full process output in memory.

## Command Semantics

`cmd` records the current log offset, writes the requested text to stdin, waits
for output until a short quiet window or timeout, then returns bytes from that
offset. If other process output is emitted at the same time, it can be included
in the response. That is expected behavior.

## Requirements

- Linux or another POSIX system with Unix domain sockets and `fcntl`
- Python 3.10+
- No third-party runtime dependencies

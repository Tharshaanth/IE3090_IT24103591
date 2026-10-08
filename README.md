# RemoteOps – IE3090 Network Programming Assignment

**Registration number:** IT24103591

## Personalised values (all derived from IT24103591)

| Item | Calculation | Value |
|---|---|---|
| Agent TCP port | 7000 + first four digits of 24103591 (2410) | **9410** |
| Source files | last three digits = 591 | `agent_591.c`, `controller_591.c`, `Makefile_591` |
| SID tag | last four digits 3591, reversed | **SID:1953** |
| Auth token | `OPS-` + 3591 | **OPS-3591** |
| Log file | `remoteops_<regno>.log` | `remoteops_IT24103591.log` |
| Storage path | `./agentfiles/<regno>/<filename>` | `./agentfiles/IT24103591/` |
| Submission ZIP | `IE3090_<regno>.zip` | `IE3090_IT24103591.zip` |

## Build and run

```
make -f Makefile_591          # builds agent_591 and controller_591 (gcc, -Wall -Wextra)
./agent_591                   # terminal 1: Agent listens on TCP 9410
./controller_591 127.0.0.1    # terminal 2+: Controller (host, optional port)
```

Example session:
```
AUTH OPS-3591
SYSINFO
LISTPROC
EXEC DATE
PUT notes.txt            (Controller sends: PUT notes.txt <size> + raw bytes)
GET notes.txt copy.txt
MONITOR START            (Controller opens a UDP socket and tells the Agent its port)
MONITOR STOP
QUIT
```

Automated checks: `bash run_tests_591.sh` (builds, starts the Agent, runs 23 checks, stops it).

## Design summary
- **Concurrency:** one detached pthread per Controller connection (max 64). Each session owns its own state (auth flag, receive buffer, monitor thread), so no shared mutable state except the log (mutex-protected).
- **Framing:** each session has a 16 KB receive buffer; `read_line()` consumes byte-by-byte from it, so partial lines and several lines per `recv()` work. PUT consumes exactly `<filesize>` bytes (buffered bytes first); GET sends exactly `<filesize>` bytes.
- **Auth:** everything except `AUTH` is rejected (`ERR 006 NOT_AUTHENTICATED`) until the token matches; 3 failed attempts close the connection.
- **EXEC:** user text is only *compared* against a fixed table (DATE, UPTIME, DISKFREE, HOSTNAME, WHOAMI); the shell string always comes from the table.
- **Files:** filenames limited to `[A-Za-z0-9._-]`, no leading dot (blocks `../`); uploads go to `name.part` then `rename()` so partial uploads are never visible and are deleted if the client drops.
- **Monitoring:** one extra thread per session sends `SYSINFO <cpu> <mem_mb> <uptime> SID:1953` over UDP every **2 seconds** to the Controller's IP and the port given in `MONITOR START`. Stopped on `MONITOR STOP`, `QUIT` or disconnect.
- **Limits/assumptions:** IPv4 only; max upload 10 MB; idle timeout 300 s; CPU load = 1-minute load average from `/proc/loadavg`; memory used = MemTotal − MemAvailable.

## Error codes
| Code | Reason | Meaning |
|---|---|---|
| 001 | AUTH_FAILED | wrong token |
| 002 | COMMAND_NOT_ALLOWED | EXEC name not whitelisted |
| 003 | UNKNOWN_COMMAND | unrecognised command |
| 004 | FILE_TOO_LARGE | PUT over 10 MB |
| 005 | FILE_NOT_FOUND | GET of missing/invalid name |
| 006 | NOT_AUTHENTICATED | command before AUTH |
| 007 | BAD_ARGUMENTS | malformed arguments |
| 008 | BAD_FILENAME | PUT with disallowed filename |
| 009 | INTERNAL_ERROR | server-side failure |
| 010 / 011 | MONITOR_ALREADY_RUNNING / MONITOR_NOT_RUNNING | monitoring state errors |
| 012 | SERVER_BUSY | 64 clients already connected |
| 013 | LINE_TOO_LONG | command line over 1023 bytes |

## Optional extension implemented
Transfer throughput (bytes/second) for PUT and GET, shown by the Controller and recorded in the Agent log.

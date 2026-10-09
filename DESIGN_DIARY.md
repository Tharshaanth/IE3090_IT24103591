# Design Diary - IE3090 RemoteOps (IT24103591)

## 8 Oct 2026
- Read the brief and worked out my personalised values: port 9410 (7000 + 2410), SID:1953 (3591 reversed), token OPS-3591, files agent_591.c / controller_591.c / Makefile_591, log remoteops_IT24103591.log, storage ./agentfiles/IT24103591/.
- Used an AI assistant (Claude) to produce a first full version of the Agent, Controller, Makefile and a test script. I recorded this in my prompt log.
- Design decision: one detached thread per connection. Each session has its own state (auth flag, buffer, monitor thread), which is simple to reason about and easy to extend to 5+ clients.
- Design decision: every session has a receive buffer and read_line() consumes from it, so partial lines and several lines in one recv() both work. PUT reads exactly <filesize> bytes.
- Set up Kali Linux in VirtualBox. apt update failed with a 403 from the Kali mirror, but gcc, make, git and ps were already installed, so I didn't need it.
- Built with make -f Makefile_591 with no warnings. The test script passed 23/23 checks.
- Obstacle: a git config command failed because I typed user.name"..." without a space. Fixed it and checked with git config --global --list.
- Made my first commits (Makefile and gitignore, Agent, Controller, test script, README).

## 9 Oct 2026
- Ran the Agent and Controller by hand: AUTH, SYSINFO, LISTPROC, EXEC, PUT/GET (cmp showed the files identical), UDP monitoring, and error cases (bad token, EXEC rm, missing file).
- Obstacle: the UDP stream kept printing in the Controller. I learned SLEEP only pauses the Controller; the Agent keeps sending until MONITOR STOP or QUIT.
- Obstacle: my Controller session was dropped by the Agent after sitting idle. The Agent has a 300 s idle timeout on the TCP connection, and the UDP stream doesn't count as activity. This is intended behaviour.
- Obstacle: the Kali VM froze once. My work survived because it was committed in git; I then pushed to GitHub as a backup.
- Design decisions: 10 MB upload limit with ERR 004; uploads go to name.part and are renamed on success so partial files never appear; filenames limited to letters, digits, dot, underscore and hyphen to block ../ traversal; EXEC compares user text to a fixed table and never passes it to the shell; 3 failed AUTH attempts close the connection; UDP interval is 2 seconds.
- Captured annotated code screenshots and prepared the Implementation Report.

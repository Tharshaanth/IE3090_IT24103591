# Prompt Log - IE3090 RemoteOps (IT24103591)

I used Claude (made by Anthropic) in a chat window on 8 and 9 October 2026.

| # | Date | What I asked | What I did with the answer |
|---|------|--------------|----------------------------|
| 1 | 8 Oct | I uploaded the assignment brief and asked Claude to help me build it using my registration number IT24103591. | Claude worked out my personal values: port 9410, SID:1953, token OPS-3591, the *_591 file names, the log name and the storage folder. I checked them against the formulas in section 2.4. |
| 2 | 8 Oct | I asked Claude to write the code. | Claude wrote agent_591.c, controller_591.c, Makefile_591, a test script and a README. I compiled it, ran the tests (23 passed) and tried every command by hand. |
| 3 | 8 Oct | I asked for step-by-step help setting up Kali, copying files and setting up git. | I followed the steps. The apt update failed, but gcc, make and git were already installed. I fixed a typo in my git config. |
| 4 | 9 Oct | I asked why the UDP stream kept running and why my Controller got disconnected. | I learned that SLEEP only pauses the Controller, and that the Agent drops idle connections after 300 seconds. |
| 5 | 9 Oct | I asked which parts of the code to screenshot for the report. | I used the line numbers it gave me and took my own screenshots. |
| 6 | 9 Oct | I asked for help making a GitHub account, a token and pushing my code. | I followed the steps and fixed my email address (I had typed mygmail.com by mistake) before pushing. |

## What I changed
I did not change how the code works. I compiled it, ran the 23 tests, and tested each command myself. The mistakes I fixed were in my own setup (a git config typo and a wrong email address).

## What I learned
TCP is just a stream of bytes, so the Agent has to read until a newline for commands and count exactly the file size for PUT and GET. Each client gets its own thread, and the UDP monitoring runs in a separate thread that stops on MONITOR STOP or when the client disconnects.

#!/bin/bash
# run_tests_591.sh - automated checks for the mandatory features (IT24103591)
# Usage: ./run_tests_591.sh      (builds, starts the Agent, runs tests, stops it)
cd "$(dirname "$0")" || exit 1
PORT=9410; SID="SID:1953"; TOKEN="OPS-3591"
PASS=0; FAIL=0
check() { # name, condition-result
    if [ "$2" = "0" ]; then echo "PASS  $1"; PASS=$((PASS+1)); else echo "FAIL  $1"; FAIL=$((FAIL+1)); fi
}
make -f Makefile_591 >/dev/null || { echo "build failed"; exit 1; }
./agent_591 > agent_stdout.txt 2>&1 &
AGENT=$!
for i in $(seq 1 20); do (echo > /dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break; sleep 0.2; done

head -c 1048576 /dev/urandom > test_upload.bin
OUT=$(printf 'AUTH %s\nSYSINFO\nLISTPROC\nEXEC DATE\nEXEC HOSTNAME\nEXEC rm\nEXEC DATE;ls\nPUT test_upload.bin\nGET test_upload.bin test_download.bin\nGET nofile.txt\nBOGUS\nMONITOR START\nSLEEP 5\nMONITOR STOP\nQUIT\n' "$TOKEN" | ./controller_591 127.0.0.1 $PORT)
echo "$OUT" > session_output.txt

echo "$OUT" | grep -q "OK AUTHENTICATED $SID";            check "AUTH success + SID tag" $?
echo "$OUT" | grep -qE "OK SYSINFO [0-9.]+ [0-9]+ [0-9]+ $SID"; check "SYSINFO format" $?
echo "$OUT" | grep -qE "OK PROCS .*[0-9]+:.* $SID";       check "LISTPROC" $?
echo "$OUT" | grep -q "OK EXEC_RESULT .* $SID";           check "EXEC whitelisted" $?
[ "$(echo "$OUT" | grep -c "ERR 002 COMMAND_NOT_ALLOWED $SID")" = "2" ]; check "EXEC rejects non-whitelist (2 cases)" $?
echo "$OUT" | grep -q "OK FILE_RECEIVED test_upload.bin $SID"; check "PUT" $?
echo "$OUT" | grep -q "OK FILE_SEND test_upload.bin 1048576 $SID"; check "GET header" $?
cmp -s test_upload.bin test_download.bin;               check "GET byte-for-byte identical" $?
[ -f agentfiles/IT24103591/test_upload.bin ];            check "stored in personalised path" $?
echo "$OUT" | grep -q "ERR 005 FILE_NOT_FOUND $SID";      check "GET missing file" $?
echo "$OUT" | grep -q "ERR 003 UNKNOWN_COMMAND $SID";     check "unknown command" $?
echo "$OUT" | grep -q "OK MONITOR_STARTED $SID";          check "MONITOR START" $?
[ "$(echo "$OUT" | grep -cE '^\[UDP\] SYSINFO .* SID:1953')" -ge 2 ]; check "UDP datagrams received (>=2) with SID" $?
echo "$OUT" | grep -q "OK MONITOR_STOPPED $SID";          check "MONITOR STOP" $?
echo "$OUT" | grep -q "OK BYE $SID";                      check "QUIT" $?

# auth gating + bad token
R=$(printf 'SYSINFO\nAUTH wrong\nQUIT\n' | ./controller_591 127.0.0.1 $PORT)
echo "$R" | grep -q "ERR 006 NOT_AUTHENTICATED $SID";     check "commands rejected before AUTH" $?
echo "$R" | grep -q "ERR 001 AUTH_FAILED $SID";           check "bad token rejected" $?

# framing: partial line, then two lines in one segment (raw TCP via bash)
exec 3<>/dev/tcp/127.0.0.1/$PORT
printf 'AUT' >&3; sleep 0.3; printf 'H %s\nSYSINFO\nEXEC DATE\n' "$TOKEN" >&3
sleep 0.5; timeout 1 cat <&3 > framing.txt; exec 3>&-
[ "$(grep -c "$SID" framing.txt)" = "3" ]; check "framing: partial line + multiple lines per recv" $?

# file too large (11 MB) is refused and connection stays usable
head -c 11534336 /dev/zero > big.bin
R=$(printf 'AUTH %s\nPUT big.bin\nSYSINFO\nQUIT\n' "$TOKEN" | ./controller_591 127.0.0.1 $PORT)
echo "$R" | grep -q "ERR 004 FILE_TOO_LARGE $SID";        check "PUT too large refused" $?
echo "$R" | grep -q "OK SYSINFO .* $SID";                 check "session still usable after refusal" $?
rm -f big.bin

# path traversal attempt
R=$(printf 'AUTH %s\nGET ../agent_591.c\nQUIT\n' "$TOKEN" | ./controller_591 127.0.0.1 $PORT)
echo "$R" | grep -q "ERR 005 FILE_NOT_FOUND $SID";        check "path traversal blocked" $?

# ungraceful disconnect during UDP monitoring: agent must survive
( exec 4<>/dev/tcp/127.0.0.1/$PORT; printf 'AUTH %s\nMONITOR START 40001\n' "$TOKEN" >&4; sleep 0.5; exec 4>&- )
sleep 1
kill -0 $AGENT 2>/dev/null;                              check "agent survives abrupt disconnect" $?

# 6 simultaneous clients
for i in 1 2 3 4 5 6; do
  ( printf 'AUTH %s\nSLEEP 1\nSYSINFO\nQUIT\n' "$TOKEN" | ./controller_591 127.0.0.1 $PORT > par_$i.txt ) &
done
wait $(jobs -p | grep -v "^$AGENT$") 2>/dev/null
[ "$(cat par_*.txt | grep -c 'OK SYSINFO')" = "6" ];     check "6 simultaneous clients served" $?
rm -f par_*.txt framing.txt

kill $AGENT 2>/dev/null; wait $AGENT 2>/dev/null
echo; echo "Result: $PASS passed, $FAIL failed"
echo "Log file: remoteops_IT24103591.log"

/*
 * agent_591.c - RemoteOps Agent (server)            IE3090 Assignment
 * Registration number : IT24103591
 * Port                : 7000 + 2410 = 9410
 * SID tag             : last four digits 3591 reversed -> SID:1953
 * Auth token          : OPS-3591
 * Log file            : remoteops_IT24103591.log
 * Storage path        : ./agentfiles/IT24103591/
 *
 * Concurrency model   : one detached POSIX thread per Controller connection.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* ---- personalised values (derived from IT24103591) ---- */
#define REG_NO        "IT24103591"
#define AGENT_PORT    9410
#define SID_TAG       "SID:1953"
#define AUTH_TOKEN    "OPS-3591"
#define LOG_FILE      "remoteops_IT24103591.log"
#define STORAGE_ROOT  "./agentfiles"
#define STORAGE_DIR   "./agentfiles/IT24103591"

/* ---- tunables ---- */
#define MAX_FILE_SIZE        (10ULL * 1024 * 1024)  /* PUT limit: 10 MB   */
#define MAX_LINE             1024                   /* longest command    */
#define BUF_SIZE             16384                  /* per-session recv buf */
#define MAX_CLIENTS          64
#define MONITOR_INTERVAL_SEC 2                      /* UDP stream period  */
#define IDLE_TIMEOUT_SEC     300
#define MAX_AUTH_FAILS       3
#define PROC_LIST_CAP        32768

static volatile sig_atomic_t g_running = 1;
static atomic_int g_active = 0;
static time_t g_start_time;
static FILE *g_log;
static pthread_mutex_t g_log_mu = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int fd;
    struct sockaddr_in addr;     /* Controller's TCP address (UDP goes here) */
    char peer[64];               /* "ip:port" for logging                    */
    char buf[BUF_SIZE];          /* receive buffer (framing)                 */
    size_t buf_pos, buf_len;
    int authed, auth_fails;
    int monitoring, udp_port;
    atomic_int mon_stop;
    pthread_t mon_thread;
} session_t;

/* ------------------------------------------------------------------ */
/* Logging: timestamped, mutex-protected, to file and stdout           */
/* ------------------------------------------------------------------ */
static void log_event(const char *peer, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void log_event(const char *peer, const char *fmt, ...)
{
    char msg[512], ts[32];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    for (char *c = msg; *c; c++)               /* keep log lines clean */
        if (!isprint((unsigned char)*c)) *c = '?';
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tm);
    pthread_mutex_lock(&g_log_mu);
    fprintf(g_log, "[%s] [%s] %s\n", ts, peer, msg);
    fflush(g_log);
    printf("[%s] [%s] %s\n", ts, peer, msg);
    fflush(stdout);
    pthread_mutex_unlock(&g_log_mu);
}

/* ------------------------------------------------------------------ */
/* Sending helpers. Every reply gets " SID:1953\n" appended.            */
/* ------------------------------------------------------------------ */
static int send_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int send_reply(session_t *s, const char *body)
{
    size_t n = strlen(body);
    char *out = malloc(n + 16);
    if (!out) return -1;
    memcpy(out, body, n);
    int m = snprintf(out + n, 16, " %s\n", SID_TAG);
    int rc = send_all(s->fd, out, n + (size_t)m);
    free(out);
    return rc;
}

static int replyf(session_t *s, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
static int replyf(session_t *s, const char *fmt, ...)
{
    char body[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    return send_reply(s, body);
}

/* ------------------------------------------------------------------ */
/* Framing: buffered reader. Handles partial lines, several lines in   */
/* one recv(), and raw file bytes that follow a PUT line.              */
/* ------------------------------------------------------------------ */
static ssize_t fill_buf(session_t *s)
{
    ssize_t n;
    do {
        n = recv(s->fd, s->buf, sizeof s->buf, 0);
    } while (n < 0 && errno == EINTR);
    if (n > 0) { s->buf_pos = 0; s->buf_len = (size_t)n; }
    return n;                       /* 0 = closed, <0 = error/timeout */
}

/* 1 = got a line, 0 = connection closed/error, -1 = line too long */
static int read_line(session_t *s, char *out, size_t max)
{
    size_t i = 0;
    for (;;) {
        if (s->buf_pos >= s->buf_len && fill_buf(s) <= 0) return 0;
        char c = s->buf[s->buf_pos++];
        if (c == '\n') {
            out[i] = '\0';
            if (i > 0 && out[i - 1] == '\r') out[--i] = '\0';
            return 1;
        }
        if (i + 1 < max) out[i++] = c; else return -1;
    }
}

/* Consume exactly n bytes (buffered bytes first, then recv()).
 * Writes them to fp, or discards them if fp == NULL.
 * Returns 0 on success, -1 if the connection died. */
static int recv_bytes(session_t *s, FILE *fp, unsigned long long n, int *io_err)
{
    while (n > 0) {
        if (s->buf_pos >= s->buf_len && fill_buf(s) <= 0) return -1;
        size_t avail = s->buf_len - s->buf_pos;
        size_t take = avail < n ? avail : (size_t)n;
        if (fp && !*io_err && fwrite(s->buf + s->buf_pos, 1, take, fp) != take)
            *io_err = 1;
        s->buf_pos += take;
        n -= take;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* SYSINFO: read from /proc, fall back to simulated values              */
/* ------------------------------------------------------------------ */
static long read_mem_used_mb(void)
{
    FILE *f = fopen("/proc/meminfo", "r");
    if (!f) return -1;
    char line[128];
    long total = -1, avail = -1;
    while (fgets(line, sizeof line, f)) {
        if (sscanf(line, "MemTotal: %ld kB", &total) == 1) continue;
        if (sscanf(line, "MemAvailable: %ld kB", &avail) == 1) continue;
    }
    fclose(f);
    return (total < 0 || avail < 0) ? -1 : (total - avail) / 1024;
}

static void get_sysinfo(double *cpu, long *mem_mb, long *uptime)
{
    FILE *f;
    double up;
    *cpu = -1.0;
    *uptime = -1;
    if ((f = fopen("/proc/loadavg", "r"))) {        /* 1-minute load average */
        if (fscanf(f, "%lf", cpu) != 1) *cpu = -1.0;
        fclose(f);
    }
    *mem_mb = read_mem_used_mb();
    if ((f = fopen("/proc/uptime", "r"))) {
        if (fscanf(f, "%lf", &up) == 1) *uptime = (long)up;
        fclose(f);
    }
    time_t now = time(NULL);                         /* plausible simulation */
    if (*cpu < 0) *cpu = (now % 100) / 100.0;
    if (*mem_mb < 0) *mem_mb = 1000 + now % 500;
    if (*uptime < 0) *uptime = (long)(now - g_start_time);
}

/* ------------------------------------------------------------------ */
/* UDP monitoring thread (one per monitoring session)                  */
/* ------------------------------------------------------------------ */
static void *monitor_thread(void *arg)
{
    session_t *s = arg;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return NULL;
    struct sockaddr_in dst = s->addr;                /* Controller's IP...   */
    dst.sin_port = htons((uint16_t)s->udp_port);     /* ...on its UDP port   */
    while (!atomic_load(&s->mon_stop)) {
        double cpu; long mem, up;
        char msg[128];
        get_sysinfo(&cpu, &mem, &up);
        int n = snprintf(msg, sizeof msg, "SYSINFO %.2f %ld %ld %s",
                         cpu, mem, up, SID_TAG);
        sendto(fd, msg, (size_t)n, 0, (struct sockaddr *)&dst, sizeof dst);
        for (int i = 0; i < MONITOR_INTERVAL_SEC * 10 && !atomic_load(&s->mon_stop); i++)
            usleep(100000);                          /* wake often to stop quickly */
    }
    close(fd);
    return NULL;
}

static void stop_monitor(session_t *s)
{
    if (!s->monitoring) return;
    atomic_store(&s->mon_stop, 1);
    pthread_join(s->mon_thread, NULL);
    s->monitoring = 0;
}

/* ------------------------------------------------------------------ */
/* Command handlers. Return 0 = keep session, -1 = close session.       */
/* ------------------------------------------------------------------ */
static int handle_listproc(session_t *s)
{
    FILE *p = popen("ps -eo pid=,comm=", "r");
    char *list = p ? malloc(PROC_LIST_CAP) : NULL;
    if (!list) { if (p) pclose(p); return replyf(s, "ERR 009 INTERNAL_ERROR"); }
    size_t len = 0;
    char line[256];
    while (fgets(line, sizeof line, p)) {
        int pid; char name[128], item[200];
        if (sscanf(line, " %d %127[^\n]", &pid, name) != 2) continue;
        for (char *c = name; *c; c++)
            if (*c == ',' || !isprint((unsigned char)*c) || *c == ' ') *c = '_';
        int n = snprintf(item, sizeof item, "%s%d:%s", len ? "," : "", pid, name);
        if (len + (size_t)n + 1 > PROC_LIST_CAP) break;   /* truncate */
        memcpy(list + len, item, (size_t)n + 1);
        len += (size_t)n;
    }
    pclose(p);
    if (len == 0) { free(list); return replyf(s, "ERR 009 INTERNAL_ERROR"); }
    char *body = malloc(len + 16);
    if (!body) { free(list); return -1; }
    snprintf(body, len + 16, "OK PROCS %s", list);
    int rc = send_reply(s, body);
    free(body); free(list);
    return rc;
}

/* The ONLY commands that can ever run. The user's text is compared with
 * the names; the shell command string always comes from this table. */
static const struct { const char *name, *cmd; } WHITELIST[] = {
    {"DATE", "date"}, {"UPTIME", "uptime"}, {"DISKFREE", "df -h /"},
    {"HOSTNAME", "hostname"}, {"WHOAMI", "whoami"},
};

static int handle_exec(session_t *s, const char *arg)
{
    const char *cmd = NULL;
    if (*arg == '\0') return replyf(s, "ERR 007 BAD_ARGUMENTS");
    for (size_t i = 0; i < sizeof WHITELIST / sizeof WHITELIST[0]; i++)
        if (strcmp(arg, WHITELIST[i].name) == 0) cmd = WHITELIST[i].cmd;
    if (!cmd) return replyf(s, "ERR 002 COMMAND_NOT_ALLOWED");
    FILE *p = popen(cmd, "r");
    if (!p) return replyf(s, "ERR 009 INTERNAL_ERROR");
    char out[1024];
    size_t len = fread(out, 1, sizeof out - 1, p);
    pclose(p);
    while (len > 0 && isspace((unsigned char)out[len - 1])) len--;
    out[len] = '\0';
    for (size_t i = 0; i < len; i++)                 /* keep reply on ONE line */
        if (out[i] == '\n') out[i] = '|'; else if (out[i] == '\r') out[i] = ' ';
    if (len == 0) strcpy(out, "(no output)");
    char body[1100];
    snprintf(body, sizeof body, "OK EXEC_RESULT %s", out);
    return send_reply(s, body);
}

static int valid_filename(const char *n)
{
    size_t len = strlen(n);
    if (len == 0 || len > 100 || n[0] == '.') return 0;   /* no "..", no hidden */
    if (len > 5 && strcmp(n + len - 5, ".part") == 0) return 0; /* temp suffix */
    for (; *n; n++)
        if (!(isalnum((unsigned char)*n) || *n == '.' || *n == '_' || *n == '-'))
            return 0;                                      /* no '/', no spaces */
    return 1;
}

static int handle_put(session_t *s, char *args)
{
    char *save, *name = strtok_r(args, " ", &save);
    char *sz = strtok_r(NULL, " ", &save), *extra = strtok_r(NULL, " ", &save);
    if (!name || !sz || extra || !isdigit((unsigned char)sz[0])) {
        replyf(s, "ERR 007 BAD_ARGUMENTS");
        return -1;               /* size unknown: can't resync, so close */
    }
    char *end;
    errno = 0;
    unsigned long long size = strtoull(sz, &end, 10);
    if (*end || errno) { replyf(s, "ERR 007 BAD_ARGUMENTS"); return -1; }

    int io_err = 0;
    if (size > MAX_FILE_SIZE) {                       /* drain, then refuse */
        if (recv_bytes(s, NULL, size, &io_err) < 0) return -1;
        log_event(s->peer, "PUT %s refused: %llu bytes exceeds limit", name, size);
        return replyf(s, "ERR 004 FILE_TOO_LARGE");
    }
    if (!valid_filename(name)) {
        if (recv_bytes(s, NULL, size, &io_err) < 0) return -1;
        return replyf(s, "ERR 008 BAD_FILENAME");
    }
    char path[256], tmp[264];
    snprintf(path, sizeof path, "%s/%s", STORAGE_DIR, name);
    snprintf(tmp, sizeof tmp, "%s.part", path);
    FILE *fp = fopen(tmp, "wb");
    if (!fp) {
        if (recv_bytes(s, NULL, size, &io_err) < 0) return -1;
        return replyf(s, "ERR 009 INTERNAL_ERROR");
    }
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    if (recv_bytes(s, fp, size, &io_err) < 0) {       /* client vanished mid-upload */
        fclose(fp); remove(tmp);
        log_event(s->peer, "PUT %s aborted: connection lost, partial file removed", name);
        return -1;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (fclose(fp) != 0 || io_err || rename(tmp, path) != 0) {
        remove(tmp);
        return replyf(s, "ERR 009 INTERNAL_ERROR");
    }
    double secs = (double)(t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    log_event(s->peer, "PUT %s stored: %llu bytes (%.0f B/s)", name, size,
              secs > 0 ? (double)size / secs : (double)size);
    return replyf(s, "OK FILE_RECEIVED %s", name);
}

static int handle_get(session_t *s, char *args)
{
    char *save, *name = strtok_r(args, " ", &save), *extra = strtok_r(NULL, " ", &save);
    if (!name || extra) return replyf(s, "ERR 007 BAD_ARGUMENTS");
    if (!valid_filename(name)) return replyf(s, "ERR 005 FILE_NOT_FOUND");
    char path[256];
    snprintf(path, sizeof path, "%s/%s", STORAGE_DIR, name);
    FILE *fp = fopen(path, "rb");
    struct stat st;
    if (!fp || fstat(fileno(fp), &st) != 0 || !S_ISREG(st.st_mode)) {
        if (fp) fclose(fp);
        return replyf(s, "ERR 005 FILE_NOT_FOUND");
    }
    unsigned long long size = (unsigned long long)st.st_size, left = size;
    if (replyf(s, "OK FILE_SEND %s %llu", name, size) < 0) { fclose(fp); return -1; }
    char chunk[BUF_SIZE];
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    while (left > 0) {                                /* exactly <size> raw bytes */
        size_t want = left < sizeof chunk ? (size_t)left : sizeof chunk;
        size_t got = fread(chunk, 1, want, fp);
        if (got == 0 || send_all(s->fd, chunk, got) < 0) {
            fclose(fp);
            log_event(s->peer, "GET %s aborted", name);
            return -1;
        }
        left -= got;
    }
    fclose(fp);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double secs = (double)(t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    log_event(s->peer, "GET %s sent: %llu bytes (%.0f B/s)", name, size,
              secs > 0 ? (double)size / secs : (double)size);
    return 0;
}

static int handle_monitor(session_t *s, char *args)
{
    char *save, *sub = strtok_r(args, " ", &save), *port = strtok_r(NULL, " ", &save);
    if (sub && strcmp(sub, "START") == 0) {
        long p = port ? strtol(port, NULL, 10) : 0;
        if (!port || p < 1 || p > 65535) return replyf(s, "ERR 007 BAD_ARGUMENTS");
        if (s->monitoring) return replyf(s, "ERR 010 MONITOR_ALREADY_RUNNING");
        s->udp_port = (int)p;
        atomic_store(&s->mon_stop, 0);
        if (pthread_create(&s->mon_thread, NULL, monitor_thread, s) != 0)
            return replyf(s, "ERR 009 INTERNAL_ERROR");
        s->monitoring = 1;
        log_event(s->peer, "MONITOR started: UDP datagrams every %ds to port %d",
                  MONITOR_INTERVAL_SEC, s->udp_port);
        return replyf(s, "OK MONITOR_STARTED");
    }
    if (sub && strcmp(sub, "STOP") == 0) {
        if (!s->monitoring) return replyf(s, "ERR 011 MONITOR_NOT_RUNNING");
        stop_monitor(s);
        log_event(s->peer, "MONITOR stopped");
        return replyf(s, "OK MONITOR_STOPPED");
    }
    return replyf(s, "ERR 007 BAD_ARGUMENTS");
}

static int dispatch(session_t *s, char *line)
{
    char *rest = strchr(line, ' ');
    if (rest) { *rest++ = '\0'; while (*rest == ' ') rest++; }
    else rest = line + strlen(line);
    const char *cmd = line;

    if (strcmp(cmd, "AUTH") == 0) {                   /* token never logged */
        if (strcmp(rest, AUTH_TOKEN) == 0) {
            s->authed = 1;
            log_event(s->peer, "AUTH success");
            return replyf(s, "OK AUTHENTICATED");
        }
        s->auth_fails++;
        log_event(s->peer, "AUTH failed (attempt %d/%d)", s->auth_fails, MAX_AUTH_FAILS);
        int rc = replyf(s, "ERR 001 AUTH_FAILED");
        return (s->auth_fails >= MAX_AUTH_FAILS) ? -1 : rc;
    }
    if (!s->authed) {                                 /* gate everything else */
        log_event(s->peer, "REJECTED (not authenticated): %s", cmd);
        replyf(s, "ERR 006 NOT_AUTHENTICATED");
        return strcmp(cmd, "PUT") == 0 ? -1 : 0;     /* PUT bytes would corrupt framing */
    }
    log_event(s->peer, "CMD %s%s%s", cmd, *rest ? " " : "", rest);

    if (strcmp(cmd, "SYSINFO") == 0) {
        double cpu; long mem, up;
        get_sysinfo(&cpu, &mem, &up);
        return replyf(s, "OK SYSINFO %.2f %ld %ld", cpu, mem, up);
    }
    if (strcmp(cmd, "LISTPROC") == 0) return handle_listproc(s);
    if (strcmp(cmd, "EXEC") == 0)     return handle_exec(s, rest);
    if (strcmp(cmd, "PUT") == 0)      return handle_put(s, rest);
    if (strcmp(cmd, "GET") == 0)      return handle_get(s, rest);
    if (strcmp(cmd, "MONITOR") == 0)  return handle_monitor(s, rest);
    if (strcmp(cmd, "QUIT") == 0) {
        stop_monitor(s);
        replyf(s, "OK BYE");
        return -1;
    }
    return replyf(s, "ERR 003 UNKNOWN_COMMAND");
}

/* ------------------------------------------------------------------ */
/* One thread per Controller connection                                 */
/* ------------------------------------------------------------------ */
static void *session_thread(void *arg)
{
    session_t *s = arg;
    char line[MAX_LINE];
    log_event(s->peer, "CONNECTED");
    for (;;) {
        int r = read_line(s, line, sizeof line);
        if (r == 0) { log_event(s->peer, "DISCONNECTED (closed, reset or timed out)"); break; }
        if (r < 0)  { replyf(s, "ERR 013 LINE_TOO_LONG"); log_event(s->peer, "closed: line too long"); break; }
        if (line[0] == '\0') continue;
        if (dispatch(s, line) < 0) break;
    }
    stop_monitor(s);                                  /* never leak the UDP thread */
    close(s->fd);
    log_event(s->peer, "SESSION ENDED");
    free(s);
    atomic_fetch_sub(&g_active, 1);
    return NULL;
}

static void on_signal(int sig) { (void)sig; g_running = 0; }

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa = { .sa_handler = on_signal };  /* no SA_RESTART: accept() wakes */
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    g_start_time = time(NULL);
    g_log = fopen(LOG_FILE, "a");
    if (!g_log) { perror("open log"); return 1; }
    mkdir(STORAGE_ROOT, 0755);
    if (mkdir(STORAGE_DIR, 0755) != 0 && errno != EEXIST) { perror("mkdir storage"); return 1; }

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }
    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    struct sockaddr_in me = { .sin_family = AF_INET, .sin_port = htons(AGENT_PORT),
                              .sin_addr.s_addr = htonl(INADDR_ANY) };
    if (bind(lfd, (struct sockaddr *)&me, sizeof me) < 0) { perror("bind"); return 1; }
    if (listen(lfd, 16) < 0) { perror("listen"); return 1; }
    log_event("agent", "RemoteOps Agent (%s) listening on TCP port %d, storage %s",
              REG_NO, AGENT_PORT, STORAGE_DIR);

    while (g_running) {
        struct sockaddr_in ca;
        socklen_t cl = sizeof ca;
        int cfd = accept(lfd, (struct sockaddr *)&ca, &cl);
        if (cfd < 0) {
            if (errno != EINTR) { perror("accept"); usleep(100000); }
            continue;
        }
        if (atomic_load(&g_active) >= MAX_CLIENTS) {
            send_all(cfd, "ERR 012 SERVER_BUSY " SID_TAG "\n", strlen("ERR 012 SERVER_BUSY " SID_TAG "\n"));
            close(cfd);
            continue;
        }
        session_t *s = calloc(1, sizeof *s);
        if (!s) { close(cfd); continue; }
        s->fd = cfd;
        s->addr = ca;
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &ca.sin_addr, ip, sizeof ip);
        snprintf(s->peer, sizeof s->peer, "%s:%d", ip, ntohs(ca.sin_port));
        struct timeval rt = { IDLE_TIMEOUT_SEC, 0 }, st = { 30, 0 };
        setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &rt, sizeof rt);
        setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &st, sizeof st);
        setsockopt(cfd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof yes);

        atomic_fetch_add(&g_active, 1);
        pthread_attr_t attr;
        pthread_t tid;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&tid, &attr, session_thread, s) != 0) {
            atomic_fetch_sub(&g_active, 1);
            close(cfd);
            free(s);
        }
        pthread_attr_destroy(&attr);
    }
    log_event("agent", "shutting down");
    close(lfd);
    fclose(g_log);
    return 0;
}

/*
 * controller_591.c - RemoteOps Controller (client)      IE3090 Assignment
 * Registration number : IT24103591   (Agent port 9410, SID:1953)
 *
 * Usage: ./controller_591 [agent_host] [agent_port]     (defaults 127.0.0.1 9410)
 *
 * Type protocol commands directly (AUTH OPS-3591, SYSINFO, LISTPROC,
 * EXEC DATE, QUIT ...). Convenience commands handled locally:
 *   PUT <localfile>              -> PUT <basename> <size> + raw bytes
 *   GET <name> [localfile]       -> saves to localfile (default downloaded_<name>)
 *   MONITOR START [udp_port]     -> opens a local UDP socket, prints datagrams
 *   MONITOR STOP
 *   SLEEP <seconds>              -> local pause (useful with piped scripts)
 *   HELP
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 9410
#define RBUF_SIZE    16384
#define RESP_MAX     65536

static int g_fd = -1;
static char g_rbuf[RBUF_SIZE];
static size_t g_rpos, g_rlen;

static int g_udp = -1;                 /* local UDP socket while monitoring */
static pthread_t g_udp_thread;
static atomic_int g_udp_stop;

/* ---------- socket helpers (same framing approach as the Agent) ---------- */
static int send_all(const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(g_fd, p, len, MSG_NOSIGNAL);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        p += n; len -= (size_t)n;
    }
    return 0;
}

static int send_line(const char *line)
{
    char *tmp = malloc(strlen(line) + 2);
    if (!tmp) return -1;
    sprintf(tmp, "%s\n", line);
    int rc = send_all(tmp, strlen(tmp));
    free(tmp);
    return rc;
}

static ssize_t fill(void)
{
    ssize_t n;
    do { n = recv(g_fd, g_rbuf, sizeof g_rbuf, 0); } while (n < 0 && errno == EINTR);
    if (n > 0) { g_rpos = 0; g_rlen = (size_t)n; }
    return n;
}

/* 1 = line read, 0 = closed/error, -1 = too long */
static int read_line(char *out, size_t max)
{
    size_t i = 0;
    for (;;) {
        if (g_rpos >= g_rlen && fill() <= 0) return 0;
        char c = g_rbuf[g_rpos++];
        if (c == '\n') {
            out[i] = '\0';
            if (i > 0 && out[i - 1] == '\r') out[--i] = '\0';
            return 1;
        }
        if (i + 1 < max) out[i++] = c; else return -1;
    }
}

/* read exactly n raw bytes into fp (or discard if fp == NULL) */
static int recv_bytes(FILE *fp, unsigned long long n)
{
    while (n > 0) {
        if (g_rpos >= g_rlen && fill() <= 0) return -1;
        size_t avail = g_rlen - g_rpos;
        size_t take = avail < n ? avail : (size_t)n;
        if (fp) fwrite(g_rbuf + g_rpos, 1, take, fp);
        g_rpos += take; n -= take;
    }
    return 0;
}

/* read one response line and print it; copy it to resp. 0 = connection lost */
static int get_response(char *resp, size_t max)
{
    int r = read_line(resp, max);
    if (r <= 0) { printf("Connection closed by Agent.\n"); return 0; }
    printf("%s\n", resp);
    return 1;
}

/* ---------- UDP monitoring receiver ---------- */
static void *udp_thread(void *arg)
{
    (void)arg;
    char buf[512];
    while (!atomic_load(&g_udp_stop)) {
        ssize_t n = recvfrom(g_udp, buf, sizeof buf - 1, 0, NULL, NULL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;
            break;
        }
        buf[n] = '\0';
        printf("[UDP] %s\n", buf);
        fflush(stdout);
    }
    return NULL;
}

static void udp_close(void)
{
    if (g_udp < 0) return;
    atomic_store(&g_udp_stop, 1);
    pthread_join(g_udp_thread, NULL);
    close(g_udp);
    g_udp = -1;
}

static int udp_open(int want_port)
{
    g_udp = socket(AF_INET, SOCK_DGRAM, 0);
    if (g_udp < 0) { perror("udp socket"); return -1; }
    struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = htons((uint16_t)want_port),
                             .sin_addr.s_addr = htonl(INADDR_ANY) };
    if (bind(g_udp, (struct sockaddr *)&a, sizeof a) < 0) {
        perror("udp bind"); close(g_udp); g_udp = -1; return -1;
    }
    socklen_t l = sizeof a;
    getsockname(g_udp, (struct sockaddr *)&a, &l);
    struct timeval tv = { 0, 500000 };               /* so the thread can notice stop */
    setsockopt(g_udp, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    atomic_store(&g_udp_stop, 0);
    pthread_create(&g_udp_thread, NULL, udp_thread, NULL);
    return ntohs(a.sin_port);
}

/* ---------- local commands ---------- */
static int do_put(const char *local, char *resp)
{
    FILE *fp = fopen(local, "rb");
    struct stat st;
    if (!fp || fstat(fileno(fp), &st) != 0 || !S_ISREG(st.st_mode)) {
        printf("Cannot read local file '%s'\n", local);
        if (fp) fclose(fp);
        return 1;
    }
    const char *slash = strrchr(local, '/');
    const char *name = slash ? slash + 1 : local;
    if (strchr(name, ' ')) { printf("Filename must not contain spaces.\n"); fclose(fp); return 1; }
    unsigned long long size = (unsigned long long)st.st_size, left = size;
    char hdr[300];
    snprintf(hdr, sizeof hdr, "PUT %s %llu", name, size);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    if (send_line(hdr) < 0) { fclose(fp); return 0; }
    char chunk[8192];
    while (left > 0) {
        size_t want = left < sizeof chunk ? (size_t)left : sizeof chunk;
        size_t got = fread(chunk, 1, want, fp);
        if (got == 0 || send_all(chunk, got) < 0) { fclose(fp); printf("Upload failed.\n"); return 0; }
        left -= got;
    }
    fclose(fp);
    int ok = get_response(resp, RESP_MAX);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double secs = (double)(t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    if (ok) printf("[sent %llu bytes, %.0f B/s]\n", size, secs > 0 ? (double)size / secs : (double)size);
    return ok;
}

static int do_get(const char *name, const char *local, char *resp)
{
    char hdr[300];
    snprintf(hdr, sizeof hdr, "GET %s", name);
    if (send_line(hdr) < 0) return 0;
    if (!get_response(resp, RESP_MAX)) return 0;
    char fname[256];
    unsigned long long size;
    if (strncmp(resp, "OK FILE_SEND ", 13) != 0 ||
        sscanf(resp, "OK FILE_SEND %255s %llu", fname, &size) != 2)
        return 1;                                    /* an ERR line: nothing follows */
    char defname[300];
    if (!local) { snprintf(defname, sizeof defname, "downloaded_%s", name); local = defname; }
    FILE *fp = fopen(local, "wb");
    if (!fp) printf("Cannot write '%s' (discarding data)\n", local);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int rc = recv_bytes(fp, size);                   /* exactly <size> bytes */
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (fp) fclose(fp);
    if (rc < 0) { printf("Connection lost during download.\n"); return 0; }
    double secs = (double)(t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    if (fp) printf("[saved %llu bytes to %s, %.0f B/s]\n", size, local,
                   secs > 0 ? (double)size / secs : (double)size);
    return 1;
}

static int do_monitor(char *args, char *resp)
{
    char *save, *sub = strtok_r(args, " ", &save), *port = strtok_r(NULL, " ", &save);
    if (sub && strcasecmp(sub, "START") == 0) {
        if (g_udp >= 0) { printf("Already monitoring locally. Use MONITOR STOP first.\n"); return 1; }
        int p = udp_open(port ? atoi(port) : 0);
        if (p < 0) return 1;
        char cmd[64];
        snprintf(cmd, sizeof cmd, "MONITOR START %d", p);
        if (send_line(cmd) < 0) return 0;
        int ok = get_response(resp, RESP_MAX);
        if (ok && strncmp(resp, "OK", 2) != 0) udp_close();   /* Agent refused */
        return ok;
    }
    if (sub && strcasecmp(sub, "STOP") == 0) {
        if (send_line("MONITOR STOP") < 0) return 0;
        int ok = get_response(resp, RESP_MAX);
        udp_close();
        return ok;
    }
    printf("Usage: MONITOR START [udp_port] | MONITOR STOP\n");
    return 1;
}

static void help(void)
{
    printf("Commands: AUTH <token> | SYSINFO | LISTPROC | EXEC <name> | PUT <localfile> |\n"
           "          GET <name> [localfile] | MONITOR START [port] | MONITOR STOP | QUIT |\n"
           "          SLEEP <sec> | HELP   (anything else is sent to the Agent as typed)\n");
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    const char *host = argc > 1 ? argv[1] : DEFAULT_HOST;
    char port[16];
    snprintf(port, sizeof port, "%d", argc > 2 ? atoi(argv[2]) : DEFAULT_PORT);

    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM }, *res;
    int gai = getaddrinfo(host, port, &hints, &res);
    if (gai != 0) { fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(gai)); return 1; }
    g_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (g_fd < 0 || connect(g_fd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("connect");
        return 1;
    }
    freeaddrinfo(res);
    printf("Connected to %s:%s\n", host, port);
    int tty = isatty(STDIN_FILENO);
    if (tty) help();

    char input[1024];
    char *resp = malloc(RESP_MAX);
    if (!resp) return 1;
    for (;;) {
        if (tty) { printf("remoteops> "); fflush(stdout); }
        if (!fgets(input, sizeof input, stdin)) break;
        input[strcspn(input, "\r\n")] = '\0';
        if (input[0] == '\0') continue;
        if (!tty) { printf("> %s\n", input); fflush(stdout); }

        char copy[1024];
        strcpy(copy, input);
        char *save, *cmd = strtok_r(copy, " ", &save);
        char *rest = save ? save : (char *)"";
        while (*rest == ' ') rest++;
        int alive = 1;

        if (strcasecmp(cmd, "HELP") == 0) help();
        else if (strcasecmp(cmd, "SLEEP") == 0) sleep((unsigned)atoi(rest));
        else if (strcasecmp(cmd, "PUT") == 0) alive = do_put(rest, resp);
        else if (strcasecmp(cmd, "GET") == 0) {
            char *s2, *n = strtok_r(rest, " ", &s2), *l = strtok_r(NULL, " ", &s2);
            if (!n) printf("Usage: GET <name> [localfile]\n");
            else alive = do_get(n, l, resp);
        }
        else if (strcasecmp(cmd, "MONITOR") == 0) alive = do_monitor(rest, resp);
        else {                                       /* plain protocol line */
            if (send_line(input) < 0) { printf("Send failed.\n"); break; }
            alive = get_response(resp, RESP_MAX);
            if (alive && strcasecmp(cmd, "QUIT") == 0) break;
        }
        fflush(stdout);
        if (!alive) break;
    }
    udp_close();
    close(g_fd);
    free(resp);
    return 0;
}

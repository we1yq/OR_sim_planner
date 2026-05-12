#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
    char workload[64];
    char instance[64];
    char target[256];
    int accepting;
    int inflight;
    int queued;
    pthread_mutex_t lock;
} state_t;

static state_t g = {
    .workload = "resnet50",
    .instance = "instance",
    .target = "",
    .accepting = 1,
    .inflight = 0,
    .queued = 0,
    .lock = PTHREAD_MUTEX_INITIALIZER,
};

static const char* arg_value(int argc, char** argv, const char* key, const char* fallback) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (strcmp(argv[i], key) == 0) return argv[i + 1];
    }
    return fallback;
}

static int arg_int(int argc, char** argv, const char* key, int fallback) {
    return atoi(arg_value(argc, argv, key, ""));
}

static void url_decode(char* s) {
    char* o = s;
    for (char* p = s; *p; ++p) {
        if (*p == '%' && p[1] && p[2]) {
            char hex[3] = {p[1], p[2], 0};
            *o++ = (char)strtol(hex, NULL, 16);
            p += 2;
        } else if (*p == '+') {
            *o++ = ' ';
        } else {
            *o++ = *p;
        }
    }
    *o = 0;
}

static void query_value(const char* path, const char* key, char* out, size_t out_len) {
    out[0] = 0;
    const char* q = strchr(path, '?');
    if (!q) return;
    q++;
    size_t key_len = strlen(key);
    while (*q) {
        if (strncmp(q, key, key_len) == 0 && q[key_len] == '=') {
            q += key_len + 1;
            size_t i = 0;
            while (*q && *q != '&' && i + 1 < out_len) out[i++] = *q++;
            out[i] = 0;
            url_decode(out);
            return;
        }
        q = strchr(q, '&');
        if (!q) return;
        q++;
    }
}

static void send_response(int fd, int code, const char* body) {
    const char* text = code == 200 ? "OK" : (code == 503 ? "Service Unavailable" : "Bad Request");
    dprintf(fd,
            "HTTP/1.1 %d %s\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: %zu\r\n"
            "Connection: close\r\n\r\n%s",
            code, text, strlen(body), body);
}

static int connect_host(const char* host, int port) {
    struct addrinfo hints = {0}, *res = NULL;
    char port_s[16];
    snprintf(port_s, sizeof(port_s), "%d", port);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port_s, &hints, &res) != 0) return -1;
    int fd = -1;
    for (struct addrinfo* it = res; it; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

static int parse_http_url(const char* url, char* host, size_t host_len, int* port) {
    const char* p = strstr(url, "http://");
    p = p ? p + 7 : url;
    const char* slash = strchr(p, '/');
    size_t len = slash ? (size_t)(slash - p) : strlen(p);
    char hp[256];
    if (len >= sizeof(hp)) return -1;
    memcpy(hp, p, len);
    hp[len] = 0;
    char* colon = strrchr(hp, ':');
    if (colon) {
        *colon = 0;
        *port = atoi(colon + 1);
    } else {
        *port = 80;
    }
    snprintf(host, host_len, "%s", hp);
    return 0;
}

static void http_get(const char* base, const char* path, char* out, size_t out_len) {
    char host[256];
    int port = 80;
    out[0] = 0;
    if (parse_http_url(base, host, sizeof(host), &port) != 0) return;
    int fd = connect_host(host, port);
    if (fd < 0) return;
    dprintf(fd, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", path, host);
    ssize_t n;
    size_t used = 0;
    while ((n = read(fd, out + used, out_len - used - 1)) > 0) {
        used += (size_t)n;
        if (used + 1 >= out_len) break;
    }
    out[used] = 0;
    close(fd);
    char* body = strstr(out, "\r\n\r\n");
    if (body) memmove(out, body + 4, strlen(body + 4) + 1);
}

static void handle_workload(int fd, const char* path) {
    if (strncmp(path, "/healthz", 8) == 0) {
        send_response(fd, 200, "{\"ok\":true}");
        return;
    }
    if (strncmp(path, "/drain", 6) == 0) {
        pthread_mutex_lock(&g.lock);
        g.accepting = 0;
        pthread_mutex_unlock(&g.lock);
        char body[256];
        snprintf(body, sizeof(body), "{\"ok\":true,\"instance\":\"%s\",\"accepting\":false}", g.instance);
        send_response(fd, 200, body);
        return;
    }
    if (strncmp(path, "/metrics", 8) == 0) {
        pthread_mutex_lock(&g.lock);
        int inflight = g.inflight, queued = g.queued, accepting = g.accepting;
        pthread_mutex_unlock(&g.lock);
        char body[256];
        snprintf(body, sizeof(body), "{\"ok\":true,\"instance\":\"%s\",\"inflight\":%d,\"queued\":%d,\"accepting\":%s}",
                 g.instance, inflight, queued, accepting ? "true" : "false");
        send_response(fd, 200, body);
        return;
    }
    if (strncmp(path, "/work", 5) == 0) {
        pthread_mutex_lock(&g.lock);
        if (!g.accepting) {
            pthread_mutex_unlock(&g.lock);
            send_response(fd, 503, "{\"ok\":false,\"error\":\"draining\"}");
            return;
        }
        g.inflight++;
        pthread_mutex_unlock(&g.lock);
        char ms_s[32];
        query_value(path, "ms", ms_s, sizeof(ms_s));
        int ms = ms_s[0] ? atoi(ms_s) : 100;
        if (ms < 0) ms = 0;
        usleep((useconds_t)ms * 1000);
        pthread_mutex_lock(&g.lock);
        g.inflight--;
        pthread_mutex_unlock(&g.lock);
        char body[256];
        snprintf(body, sizeof(body), "{\"ok\":true,\"workload\":\"%s\",\"instance\":\"%s\",\"ms\":%d}",
                 g.workload, g.instance, ms);
        send_response(fd, 200, body);
        return;
    }
    send_response(fd, 400, "{\"ok\":false,\"error\":\"unknown_path\"}");
}

static void handle_router(int fd, const char* path) {
    if (strncmp(path, "/healthz", 8) == 0) {
        send_response(fd, 200, "{\"ok\":true}");
        return;
    }
    if (strncmp(path, "/reroute", 8) == 0) {
        char target[256];
        query_value(path, "target", target, sizeof(target));
        pthread_mutex_lock(&g.lock);
        snprintf(g.target, sizeof(g.target), "%s", target);
        pthread_mutex_unlock(&g.lock);
        char body[384];
        snprintf(body, sizeof(body), "{\"ok\":true,\"workload\":\"%s\",\"target\":\"%s\"}", g.workload, target);
        send_response(fd, 200, body);
        return;
    }
    if (strncmp(path, "/target", 7) == 0) {
        pthread_mutex_lock(&g.lock);
        char target[256];
        snprintf(target, sizeof(target), "%s", g.target);
        pthread_mutex_unlock(&g.lock);
        char body[384];
        snprintf(body, sizeof(body), "{\"ok\":true,\"workload\":\"%s\",\"target\":\"%s\"}", g.workload, target);
        send_response(fd, 200, body);
        return;
    }
    if (strncmp(path, "/route", 6) == 0) {
        pthread_mutex_lock(&g.lock);
        char target[256];
        snprintf(target, sizeof(target), "%s", g.target);
        pthread_mutex_unlock(&g.lock);
        if (!target[0]) {
            send_response(fd, 503, "{\"ok\":false,\"error\":\"no_target\"}");
            return;
        }
        char ms[32];
        query_value(path, "ms", ms, sizeof(ms));
        char work_path[128];
        snprintf(work_path, sizeof(work_path), "/work?ms=%s", ms[0] ? ms : "100");
        char upstream[4096];
        http_get(target, work_path, upstream, sizeof(upstream));
        char body[4608];
        snprintf(body, sizeof(body), "{\"ok\":true,\"workload\":\"%s\",\"target\":\"%s\",\"upstream\":%s}",
                 g.workload, target, upstream[0] ? upstream : "{}");
        send_response(fd, 200, body);
        return;
    }
    send_response(fd, 400, "{\"ok\":false,\"error\":\"unknown_path\"}");
}

typedef struct {
    int fd;
    int router;
} client_arg_t;

static void* client_thread(void* raw) {
    client_arg_t* arg = (client_arg_t*)raw;
    char buf[4096] = {0};
    ssize_t n = read(arg->fd, buf, sizeof(buf) - 1);
    if (n > 0) {
        char method[16], path[2048];
        sscanf(buf, "%15s %2047s", method, path);
        if (arg->router) handle_router(arg->fd, path);
        else handle_workload(arg->fd, path);
    }
    close(arg->fd);
    free(arg);
    return NULL;
}

int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);
    const char* mode = arg_value(argc, argv, "--mode", "workload");
    int is_router = strcmp(mode, "router") == 0;
    snprintf(g.workload, sizeof(g.workload), "%s", arg_value(argc, argv, "--workload", "resnet50"));
    snprintf(g.instance, sizeof(g.instance), "%s", arg_value(argc, argv, "--instance", is_router ? "router" : "instance"));
    snprintf(g.target, sizeof(g.target), "%s", arg_value(argc, argv, "--target", ""));
    int port = arg_int(argc, argv, "--port", 8080);

    int server = socket(AF_INET, SOCK_STREAM, 0);
    int yes = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(server, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        perror("bind");
        return 2;
    }
    if (listen(server, 128) != 0) {
        perror("listen");
        return 2;
    }
    printf("or-sim simple %s listening port=%d workload=%s instance=%s target=%s\n",
           is_router ? "router" : "workload", port, g.workload, g.instance, g.target);
    fflush(stdout);

    while (1) {
        int fd = accept(server, NULL, NULL);
        if (fd < 0) continue;
        client_arg_t* arg = calloc(1, sizeof(*arg));
        arg->fd = fd;
        arg->router = is_router;
        pthread_t tid;
        pthread_create(&tid, NULL, client_thread, arg);
        pthread_detach(tid);
    }
}

/*
 * server.c — single-process TCP chat server (CSC209 Sockets Project).
 *
 * Architecture
 * ------------
 * One process multiplexes every client with select(). Client sockets are
 * non-blocking. Each client has an input buffer (so a line split across
 * several read() calls is reassembled) and an output buffer (so a short
 * write() does not block the process, and a slow client does not stop the
 * others). When an output buffer fills up, that client is disconnected.
 *
 * The listening socket, the client table, the select loop, and the read
 * loop are marked with "REPORT:" comments so the write-up can cite a file
 * and a line without searching.
 *
 * Protocol (newline-terminated UTF-8 text, one message per line)
 * --------------------------------------------------------------
 * Client -> server
 *   NICK <name>     register, or change nickname
 *   MSG <text>      chat message (spaces allowed in <text>)
 *   WHO             ask who is in the room
 *   QUIT            leave
 *   <text>          shorthand for MSG <text>, only after NICK succeeds
 *
 * Server -> client
 *   OK <text>       command succeeded, or the welcome line
 *   ERR <text>      command rejected, or the server is full
 *   JOIN <nick>     <nick> entered the room (also sent to a newcomer for
 *                   everyone already present)
 *   LEAVE <nick>    <nick> left, or changed away from this nickname
 *   MSG <nick> <text>
 *   WHO <nick> <nick> ...
 *
 * A nickname is 1..NICK_MAX characters, starts with a letter, and then
 * contains only letters, digits, '_' or '-'. Commands are the whole first
 * word, case-sensitive. "WHO" is a command; to chat that word, send
 * "MSG WHO".
 *
 */

#define _POSIX_C_SOURCE 200809L

#include "common.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

/* One connected client. fd == -1 means the slot is free. */
/*
* This is just a record for one person connected to the chat server. 
* For every client, the server keeps a little bundle of info
*/
typedef struct {
    int fd;                     /* connected socket, or -1 if slot free */
    char nick[NICK_MAX + 1];    /* nickname plus the terminating NUL */
    int registered;             /* 1 after a nickname has been accepted */
    int closing;                /* 1 after QUIT: flush output, then drop */
    int dead;                   /* 1: close this slot at the next sweep */
    char inbuf[IN_SIZE];        /* unread bytes, waiting for a newline */
    size_t inlen;               /* how many of those bytes are valid */
    char outbuf[OUT_SIZE];      /* bytes queued because write() was short */
    size_t outlen;              /* how many of those bytes are still unsent */
} Client;

/* REPORT: client table. Fixed array, no malloc in the connection path. */
static Client clients[MAX_CLIENTS];

/* Mark every slot free. fd == -1 means empty. */
static void init_clients(void) {
    int i;
    for (i = 0; i < MAX_CLIENTS; i++) {
        clients[i].fd = -1;
    }
}

/* Make fd non-blocking so a slow peer cannot stall select(). */
static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }
    return 0;
}

/* Close the socket if it is open, wipe the slot, mark it free. */
static void clear_client(Client *c) {
    if (c->fd >= 0) {
        close(c->fd);
    }
    memset(c, 0, sizeof(*c));
    c->fd = -1;     /* memset leaves fd at 0, which is not "free" */
}

/* Append bytes to the output buffer. Does not write to the socket.
 * Returns 0, or -1 if the buffer cannot hold the bytes (slow client). */
static int queue_bytes(Client *c, const char *data, size_t len) {
    if (len > OUT_SIZE || c->outlen > OUT_SIZE - len) {
        return -1;
    }
    memcpy(c->outbuf + c->outlen, data, len);
    c->outlen += len;
    return 0;
}

/*
 * REPORT: partial write handling.
 * Push outbuf to the socket. A short write leaves the rest queued.
 * EAGAIN: kernel buffer full, caller waits for select(). Other errors
 * or a 0-length write: peer is gone, return -1 so the caller drops them.
 * Returns 0 if the client is still usable.
 */
static int flush_out(Client *c) {
    while (c->outlen > 0) {
        ssize_t n = write(c->fd, c->outbuf, c->outlen);
        if (n < 0) {
            if (errno == EINTR) {
                continue;            /* signal, try again */
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;            /* would block; wait for select */
            }
            return -1;               /* EPIPE, ECONNRESET, ... */
        }
        if (n == 0) {
            /* A zero-length write is not a clean short write; treat it as failure. */
            errno = EPIPE;           /* not a clean short write */
            return -1;
        }
        memmove(c->outbuf, c->outbuf + n, c->outlen - (size_t)n);
        c->outlen -= (size_t)n;
    }
    return 0;
}

/* Queue a C string and try to flush it now.
 * Marks the client dead if the output buffer is full or the write fails. */
static int send_now(Client *c, const char *s) {
    if (c->fd < 0 || c->dead) {
        return -1;
    }
    if (queue_bytes(c, s, strlen(s)) < 0) {
        fprintf(stderr, "[server] output full, dropping fd=%d\n", c->fd);
        c->dead = 1;                /* slow client: do not block the others */
        return -1;
    }
    if (flush_out(c) < 0) {
        fprintf(stderr, "[server] write failed, dropping fd=%d: %s\n",
                c->fd, strerror(errno));
        c->dead = 1;                /* EPIPE / ECONNRESET: peer is gone */
        return -1;
    }
    return 0;                       /* queued; leftover bytes wait for select */
}

/* Send msg to every live, registered client except skip (may be NULL).
 * A client whose outbuf is full, or whose write fails, is marked dead
 * so a slow peer cannot stall the room. */
static void broadcast(Client *skip, const char *msg, int registered_only) {
    size_t len = strlen(msg);
    int i;
    for (i = 0; i < MAX_CLIENTS; i++) {
        Client *c = &clients[i];
        if (c->fd < 0 || c->dead || c == skip) {
            continue;                /* empty, dying, or the sender */
        }
        if (registered_only && !c->registered) {
            continue;                /* not in the room yet */
        }
        if (queue_bytes(c, msg, len) < 0) {
            fprintf(stderr, "[server] output full, dropping fd=%d\n", c->fd);
            c->dead = 1;            /* too slow: drop, keep going */
            continue;
        }
        if (flush_out(c) < 0) {
            fprintf(stderr, "[server] write failed, dropping fd=%d: %s\n",
                    c->fd, strerror(errno));
            c->dead = 1;
        }
    }
}

/* 1 if s is a legal nickname: 1..NICK_MAX chars, starts with a letter,
 * then only letters, digits, '_' or '-'. Otherwise 0. */
static int valid_nick(const char *s) {
    size_t n;
    size_t i;
    unsigned char ch;

    if (s == NULL) {
        return 0;
    }
    n = strlen(s);
    if (n < 1 || n > NICK_MAX) {
        return 0;
    }
    ch = (unsigned char)s[0];
    if (!isalpha(ch)) {                /* must start with a letter */
        return 0;
    }
    for (i = 1; i < n; i++) {
        ch = (unsigned char)s[i];        /* unsigned: isalnum is undefined on < 0 */
        if (!isalnum(ch) && ch != '_' && ch != '-') {
            return 0;
        }
    }
    return 1;
}

/* 1 if some other live, registered client already has this nickname.
 * self is ignored, so a client may keep their own name. */
static int nick_in_use(const char *nick, const Client *self) {
    int i;
    for (i = 0; i < MAX_CLIENTS; i++) {
        Client *o = &clients[i];
        if (o->fd < 0 || o->dead || !o->registered || o == self) {
            continue;                /* empty, dying, not joined, or us */
        }
        if (strcmp(o->nick, nick) == 0) {
            return 1;
        }
    }
    return 0;
}

/* Register a nickname, or change it.
 * First join: send the newcomer the current roster, then announce JOIN.
 * Rename: announce LEAVE for the old name, then JOIN for the new one. */
static void handle_nick(Client *c, const char *nick) {
    int first;
    char old[NICK_MAX + 1];
    char line[NICK_MAX + 32];
    int i;

    if (!valid_nick(nick)) {
        send_now(c, "ERR invalid nickname\n");
        return;
    }
    if (c->registered && strcmp(c->nick, nick) == 0) {
        send_now(c, "OK nick unchanged\n");
        return;
    }
    if (nick_in_use(nick, c)) {
        send_now(c, "ERR nickname taken\n");
        return;
    }

    first = !c->registered;
    if (!first) {
        memcpy(old, c->nick, sizeof(old));
    }

    strncpy(c->nick, nick, NICK_MAX);
    c->nick[NICK_MAX] = '\0';
    c->registered = 1;

    snprintf(line, sizeof(line), "OK nick set to %s\n", c->nick);
    if (send_now(c, line) < 0) {
        return;              /* marked dead; stop */
    }

    /* A newcomer learns who is already in the room. Each JOIN is the same
     * message everyone else received when that person arrived. */
    if (first) {
        for (i = 0; i < MAX_CLIENTS; i++) {
            Client *o = &clients[i];
            if (o->fd < 0 || o->dead || o == c || !o->registered) {
                continue;
            }
            snprintf(line, sizeof(line), "JOIN %s\n", o->nick);
            if (send_now(c, line) < 0) {
                return;
            }
        }
    } else {
        snprintf(line, sizeof(line), "LEAVE %s\n", old);
        broadcast(c, line, 1);
        if (c->dead) {
            return;
        }
    }

    snprintf(line, sizeof(line), "JOIN %s\n", c->nick);
    broadcast(c, line, 1);
    fprintf(stderr, "[server] fd=%d nick=%s\n", c->fd, c->nick);
}

/* Broadcast a chat line as "MSG <nick> <text>".
 * Unregistered, empty, or too-long text is refused with ERR.
 * The sender is included so every client renders the same stream. */
static void handle_msg(Client *c, const char *text) {
    char out[IN_SIZE + NICK_MAX + 16];
    int n;

    if (!c->registered) {
        send_now(c, "ERR send NICK <name> first\n");
        return;
    }
    if (text == NULL || text[0] == '\0') {
        send_now(c, "ERR empty message\n");
        return;
    }

    n = snprintf(out, sizeof(out), "MSG %s %s\n", c->nick, text);
    if (n < 0 || (size_t)n >= sizeof(out)) {
        send_now(c, "ERR message too long\n");        /* would have been truncated */
        return;
    }
    /* Include the sender, so every client renders the same stream. */
    broadcast(NULL, out, 1);
}

/* Reply to the asker with "WHO nick nick ...".
 * Unregistered callers are refused. A list that would not fit is
 * refused rather than sent truncated. */
static void handle_who(Client *c) {
    char line[MAX_CLIENTS * (NICK_MAX + 1) + 8];
    size_t used;
    int i;
    int n;

    if (!c->registered) {
        send_now(c, "ERR send NICK <name> first\n");
        return;
    }

    n = snprintf(line, sizeof(line), "WHO");
    if (n < 0) {
        return;
    }
    used = (size_t)n;

    for (i = 0; i < MAX_CLIENTS; i++) {
        Client *o = &clients[i];
        int m;
        if (o->fd < 0 || o->dead || !o->registered) {
            continue;                    /* not in the room */
        }
        m = snprintf(line + used, sizeof(line) - used, " %s", o->nick);
        if (m < 0 || (size_t)m >= sizeof(line) - used) {
            send_now(c, "ERR who list too long\n");
            return;
        }
        used += (size_t)m;
    }

    if (used + 2 > sizeof(line)) {         /* room for '\n' and '\0' */
        send_now(c, "ERR who list too long\n");
        return;
    }
    line[used++] = '\n';
    line[used] = '\0';
    send_now(c, line);            /* only the asker */
}

static void handle_quit(Client *c) {
    send_now(c, "OK bye\n");
    /* Stop reading. The main loop drops us once outbuf has drained. */
    c->closing = 1;
}

/* If line begins with cmd as a whole word, return the argument
 * (empty string if there is none). Otherwise NULL.
 * The pointer aliases line; the caller does not free it. */
static const char *after_cmd(const char *line, const char *cmd) {
    size_t n = strlen(cmd);
    if (strncmp(line, cmd, n) != 0) {
        return NULL;
    }
    if (line[n] == '\0') {
        return "";                /* bare command, no argument */
    }
    if (line[n] != ' ') {
        return NULL;              /* NICK must not match NICKNAME */
    }
    line += n;
    while (*line == ' ') {
        line++;                   /* skip extra spaces */
    }
    return line;
}

/* Dispatch one complete line.
 * NICK/QUIT/WHO/MSG are commands. Any other line is chat, but only
 * after NICK succeeds. "WHO" is the command; chat it with "MSG WHO". */
static void handle_line(Client *c, char *line) {
    const char *arg;

    if (c->closing || c->dead) {
        return;            /* already leaving */
    }

    arg = after_cmd(line, "NICK");
    if (arg != NULL) {
        handle_nick(c, arg);
        return;
    }
    if (after_cmd(line, "QUIT") != NULL) {
        handle_quit(c);
        return;
    }
    if (after_cmd(line, "WHO") != NULL) {
        handle_who(c);
        return;
    }
    arg = after_cmd(line, "MSG");
    if (arg != NULL) {
        handle_msg(c, arg);
        return;
    }

    /* Not a command: shorthand for MSG, once registered. */
    if (!c->registered) {
        send_now(c, "ERR send NICK <name> first\n");
        return;
    }
    handle_msg(c, line);
}

/*
 * REPORT: read loop / partial reads.
 * inbuf holds bytes that do not yet form a full line. read() may return
 * one byte, several lines, or a line split in the middle. We only call
 * handle_line once a '\n' has arrived. A buffer that fills with no newline
 * is an error (the peer is not speaking the protocol).
 * Returns 0 if the client is still usable, -1 if it should be dropped.
 */
static int extract_lines(Client *c) {
    while (!c->dead && !c->closing) {
        char *nl;
        size_t linelen;
        size_t consumed;

        nl = memchr(c->inbuf, '\n', c->inlen);
        if (nl == NULL) {
            if (c->inlen >= IN_SIZE) {
                send_now(c, "ERR line too long\n");
                return -1;                /* no newline, buffer full */
            }
            return 0;                    /* need more bytes */
        }

        *nl = '\0';
        linelen = (size_t)(nl - c->inbuf);
        if (linelen > 0 && c->inbuf[linelen - 1] == '\r') {
            c->inbuf[linelen - 1] = '\0';        /* strip \r from \r\n */
        }

        handle_line(c, c->inbuf);

        consumed = linelen + 1;
        c->inlen -= consumed;
        memmove(c->inbuf, c->inbuf + consumed, c->inlen);
    }
    return c->dead ? -1 : 0;
}

/*
 * REPORT: disconnect detection.
 * Append whatever read() returns to inbuf, then extract complete lines.
 * read() == 0: peer closed (orderly shutdown) -> drop.
 * read() < 0: EAGAIN means nothing ready yet (return 0); anything else
 * after EINTR is a hard failure -> drop.
 * Returns 0 if usable, -1 if the client should be dropped.
 */
static int read_client(Client *c) {
    ssize_t n;

    if (c->inlen >= IN_SIZE) {
        send_now(c, "ERR line too long\n");
        return -1;
    }

    do {
        n = read(c->fd, c->inbuf + c->inlen, IN_SIZE - c->inlen);
    } while (n < 0 && errno == EINTR);            /* signal, retry */

    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;                        /* non-blocking, nothing yet */
        }
        fprintf(stderr, "[server] read fd=%d: %s\n", c->fd, strerror(errno));
        return -1;                            /* ECONNRESET, EPIPE, ... */
    }
    if (n == 0) {
        fprintf(stderr, "[server] eof fd=%d\n", c->fd);
        return -1;                        /* orderly close */
    }

    c->inlen += (size_t)n;
    return extract_lines(c);
}

/* Close every client marked dead. Announce LEAVE for anyone who had
 * joined. Broadcasting may mark further clients dead (their outbuf
 * filled), so repeat until the table is stable. */
static void sweep(void) {
    int progress = 1;
    while (progress) {
        int i;
        progress = 0;
        for (i = 0; i < MAX_CLIENTS; i++) {
            Client *c = &clients[i];
            char msg[NICK_MAX + 16];

            if (c->fd < 0 || !c->dead) {
                continue;
            }

            if (c->registered && c->nick[0] != '\0') {
                snprintf(msg, sizeof(msg), "LEAVE %s\n", c->nick);
                c->registered = 0;                /* do not announce them twice */
                broadcast(c, msg, 1);            /* may mark more clients dead */
            }
            /* Best-effort flush of OK bye / ERR before we drop the fd. */
            if (c->outlen > 0) {
                (void)flush_out(c);
            }
            fprintf(stderr, "[server] close fd=%d\n", c->fd);
            clear_client(c);
            progress = 1;
            break;                        /* table changed; rescan */
        }
    }
}

/* First empty slot (fd < 0), or NULL if the table is full. */
static Client *find_free_slot(void) {
    int i;
    for (i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].fd < 0) {
            return &clients[i];
        }
    }
    return NULL;
}

/* Best-effort write of a fixed error string on a still-blocking socket.
 * Used only to reject a connection we are about to close. Gives up on
 * any error other than EINTR. */
static void write_all_ignoring_errors(int fd, const char *msg) {
    size_t left = strlen(msg);
    const char *p = msg;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) {
                continue;                    /* signal, retry */
            }
            return;                          /* peer gone; we are closing anyway */
        }
        if (n == 0) {
            return;
        }
        p += n;
        left -= (size_t)n;                    /* short write: send the rest */
    }
}

/*
 * REPORT: accept loop.
 * The listening socket is non-blocking. select() said it was readable,
 * but accept() can still fail with EAGAIN if the connection vanished,
 * or ECONNABORTED. We loop until accept() reports that the backlog is
 * empty. A full client table is an error we report to that one peer,
 * then we close them; other clients are unaffected.
 */
static void accept_clients(int listen_fd) {
    for (;;) {
        struct sockaddr_in addr;
        socklen_t addrlen = sizeof(addr);
        int fd;
        Client *slot;
        char ip[INET_ADDRSTRLEN];

        fd = accept(listen_fd, (struct sockaddr *)&addr, &addrlen);
        if (fd < 0) {
            if (errno == EINTR) {
                continue;                        /* signal, retry */
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK ||
                errno == ECONNABORTED) {
                return;                          /* backlog empty, or handshake died */
            }
            perror("accept");
            return;
        }

        slot = find_free_slot();
        if (slot == NULL) {
            write_all_ignoring_errors(fd, "ERR server full\n");
            close(fd);                    /* reject this peer only */
            fprintf(stderr, "[server] rejected connection: server full\n");
            continue;
        }

        if (fd >= FD_SETSIZE || set_nonblocking(fd) < 0) {
            close(fd);                /* select() cannot watch it */
            fprintf(stderr, "[server] rejected connection: cannot watch fd\n");
            continue;
        }

        memset(slot, 0, sizeof(*slot));
        slot->fd = fd;

        if (inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip)) == NULL) {
            strncpy(ip, "?", sizeof(ip));
            ip[sizeof(ip) - 1] = '\0';
        }
        fprintf(stderr, "[server] connect fd=%d from %s:%d\n",
                fd, ip, ntohs(addr.sin_port));

        send_now(slot, "OK welcome. Send NICK <name>\n");
    }
}

/*
 * REPORT: socket setup.
 * socket / setsockopt(SO_REUSEADDR) / bind / listen. SO_REUSEADDR lets us
 * restart the server without waiting out TIME_WAIT on the same port.
 * The listening socket is non-blocking so accept() cannot stall select().
 */
static int setup_listener(int port) {
    int fd;
    int opt = 1;
    struct sockaddr_in addr;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        exit(1);
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");            /* reuse the port after a restart */
        exit(1);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);     /* every local interface */
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");                /* port in use, or no permission */
        exit(1);
    }
    if (listen(fd, 16) < 0) {
        perror("listen");
        exit(1);
    }
    if (set_nonblocking(fd) < 0) {
        perror("fcntl");            /* accept() must not block select() */
        exit(1);
    }
    return fd;
}

/* Port from a string, or -1 if it is not a whole number in 1..65535. */
static int parse_port(const char *s) {
    char *end = NULL;
    long v;

    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v < 1 || v > 65535) {
        return -1;
    }
    return (int)v;
}

int main(int argc, char **argv) {
    int port = DEFAULT_PORT;
    int listen_fd;

    if (argc == 2) {
        port = parse_port(argv[1]);
        if (port < 0) {
            fprintf(stderr, "usage: %s [port]\n", argv[0]);
            return 1;
        }
    } else if (argc > 2) {
        fprintf(stderr, "usage: %s [port]\n", argv[0]);
        return 1;
    }

    /* A write to a client who already left must not kill the process. */
    signal(SIGPIPE, SIG_IGN);

    init_clients();
    listen_fd = setup_listener(port);
    fprintf(stderr, "[server] listening on port %d\n", port);

    /*
     * REPORT: select loop.
     * Each iteration watches the listening socket plus every client.
     * A client is in the read set unless they have sent QUIT. A client is
     * in the write set only when their output buffer is non-empty, which
     * is how a slow peer waits without blocking anyone else.
     * Three (or twenty) clients overlap because they are all in this one
     * fd_set at the same time; we never finish one client before accepting
     * the next.
     */
    for (;;) {
        fd_set readfds;
        fd_set writefds;
        int maxfd = listen_fd;
        int i;
        int ready;

        FD_ZERO(&readfds);
        FD_ZERO(&writefds);
        FD_SET(listen_fd, &readfds);

        for (i = 0; i < MAX_CLIENTS; i++) {
            Client *c = &clients[i];
            if (c->fd < 0) {
                continue;
            }
            if (!c->closing) {
                FD_SET(c->fd, &readfds);        /* still accepting input */
            }
            if (c->outlen > 0) {
                FD_SET(c->fd, &writefds);       /* has bytes waiting to go */
            }
            if (c->fd > maxfd) {
                maxfd = c->fd;
            }
        }

        ready = select(maxfd + 1, &readfds, &writefds, NULL, NULL);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            return 1;
        }

        if (FD_ISSET(listen_fd, &readfds)) {
            accept_clients(listen_fd);
        }

        for (i = 0; i < MAX_CLIENTS; i++) {
            Client *c = &clients[i];
            if (c->fd < 0 || c->dead) {
                continue;
            }
            /* Drain pending output first so a full buffer can accept the
             * reply we are about to generate from a read. */
            if (c->outlen > 0 && FD_ISSET(c->fd, &writefds)) {
                if (flush_out(c) < 0) {
                    /* EPIPE / ECONNRESET: the peer is gone. Do not stall the others. */
                    fprintf(stderr, "[server] write failed, dropping fd=%d: %s\n",
                            c->fd, strerror(errno));
                    c->dead = 1;
                    continue;
                }
            }
            if (c->dead || c->closing || c->fd < 0) {
                continue;
            }
            if (FD_ISSET(c->fd, &readfds)) {
                if (read_client(c) < 0) {
                    c->dead = 1;
                }
            }
        }

        /* QUIT sets closing. Once the goodbye has been written, drop them
         * so LEAVE is announced and the slot can be reused. */
        for (i = 0; i < MAX_CLIENTS; i++) {
            Client *c = &clients[i];
            if (c->fd >= 0 && c->closing && c->outlen == 0) {
                c->dead = 1;
            }
        }

        sweep();
    }
}

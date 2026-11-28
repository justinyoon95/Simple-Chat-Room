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










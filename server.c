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

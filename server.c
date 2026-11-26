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
 * Academic integrity
 * ------------------
 * Written with assistance from Grok (xAI), November 2026.
 * Cite this in the submission. Every group member must be able to explain
 * every part of this file.
 */

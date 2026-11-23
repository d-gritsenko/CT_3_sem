#define _DEFAULT_SOURCE

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "protocol.h"

#define BACKLOG_FILE "chat_backlog.dat"
#define BACKLOG_TMP  "chat_backlog.dat.tmp"
#define BACKLOG_MAGIC 0x51424B4CU
#define BACKLOG_VERSION 1U

#define CLR_RESET   "\033[0m"
#define CLR_BOLD    "\033[1m"
#define CLR_DIM     "\033[2m"
#define CLR_RED     "\033[31m"
#define CLR_GREEN   "\033[32m"
#define CLR_YELLOW  "\033[33m"
#define CLR_BLUE    "\033[34m"
#define CLR_MAGENTA "\033[35m"
#define CLR_CYAN    "\033[36m"

struct client_entry {
    int used;
    int active;
    long id;
    long reply_type;
    char name[MAX_NAME];
    time_t last_seen;
};

struct pending_msg {
    int used;
    long msg_id;
    char from[MAX_NAME];
    char to[MAX_NAME];
    char text[MAX_TEXT];
    time_t created_at;
};

struct backlog_header {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
};

static volatile sig_atomic_t running = 1;

static int g_msqid = -1;
static long g_my_id = 0;
static long g_reply_type = 0;
static long g_next_msg_id = 1;
static char g_my_name[MAX_NAME];
static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_io_mtx = PTHREAD_MUTEX_INITIALIZER;

static void on_signal(int sig) {
    (void)sig;
    running = 0;
}

static void get_timestamp(char *buf, size_t size) {
    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    strftime(buf, size, "%H:%M:%S", &tm_now);
}

static void print_prompt_unlocked(const char *name) {
    if (name != NULL && name[0] != '\0') {
        printf(CLR_BOLD CLR_GREEN "[%s]" CLR_RESET CLR_DIM " > " CLR_RESET, name);
    } else {
        printf(CLR_DIM "[guest] > " CLR_RESET);
    }
    fflush(stdout);
}

static void print_prompt(void) {
    char cur_name[MAX_NAME];
    pthread_mutex_lock(&g_mtx);
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    pthread_mutex_lock(&g_io_mtx);
    print_prompt_unlocked(cur_name);
    pthread_mutex_unlock(&g_io_mtx);
}

static void print_client_banner(void) {
    pthread_mutex_lock(&g_io_mtx);
    printf(CLR_CYAN "╔══════════════════════════════════════════════╗\n");
    printf("║" CLR_BOLD "           System V Queue Messenger           " CLR_RESET CLR_CYAN "║\n");
    printf("╠══════════════════════════════════════════════╣\n");
    printf("║  " CLR_GREEN "#<name>" CLR_RESET "        — register                   " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "<name>#<text>" CLR_RESET "  — send message               " CLR_CYAN "║\n");
    printf("║  " CLR_CYAN "? / /who" CLR_RESET "       — online users               " CLR_CYAN "║\n");
    printf("║  " CLR_YELLOW "##" CLR_RESET "             — logout                     " CLR_CYAN "║\n");
    printf("║  " CLR_DIM "Ctrl+D" CLR_RESET "         — exit                       " CLR_CYAN "║\n");
    printf("╚══════════════════════════════════════════════╝\n" CLR_RESET);
    fflush(stdout);
    pthread_mutex_unlock(&g_io_mtx);
}

static void print_arbiter_banner(int msqid, int restored) {
    printf(CLR_MAGENTA "╔══════════════════════════════════════════════╗\n");
    printf("║" CLR_BOLD "            Queue Messenger Arbiter           " CLR_RESET CLR_MAGENTA "║\n");
    printf("╚══════════════════════════════════════════════╝\n" CLR_RESET);
    char ts[16];
    get_timestamp(ts, sizeof(ts));
    printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "started" CLR_RESET " (msqid=%d, heartbeat=%ds, offline_ttl=%ds, restored=%d)\n",
           ts, msqid, HEARTBEAT_TIMEOUT, OFFLINE_TTL, restored);
    fflush(stdout);
}

static void drain_type(int msqid, long mtype) {
    struct qmsg tmp;
    while (msgrcv(msqid, &tmp, MSG_SIZE, mtype, IPC_NOWAIT) >= 0) {
    }
}

static void save_backlog(const struct pending_msg *pending) {
    uint32_t count = 0;
    for (int i = 0; i < MAX_PENDING; i++) {
        if (pending[i].used) {
            count++;
        }
    }

    if (count == 0) {
        unlink(BACKLOG_FILE);
        return;
    }

    FILE *fp = fopen(BACKLOG_TMP, "wb");
    if (fp == NULL) {
        return;
    }

    struct backlog_header hdr;
    hdr.magic = BACKLOG_MAGIC;
    hdr.version = BACKLOG_VERSION;
    hdr.count = count;

    int ok = (fwrite(&hdr, sizeof(hdr), 1, fp) == 1);
    for (int i = 0; ok && i < MAX_PENDING; i++) {
        if (pending[i].used) {
            if (fwrite(&pending[i], sizeof(struct pending_msg), 1, fp) != 1) {
                ok = 0;
            }
        }
    }

    if (fclose(fp) != 0) {
        ok = 0;
    }

    if (!ok || rename(BACKLOG_TMP, BACKLOG_FILE) != 0) {
        unlink(BACKLOG_TMP);
    }
}

static int load_backlog(struct pending_msg *pending) {
    FILE *fp = fopen(BACKLOG_FILE, "rb");
    if (fp == NULL) {
        return 0;
    }

    struct backlog_header hdr;
    if (fread(&hdr, sizeof(hdr), 1, fp) != 1 ||
        hdr.magic != BACKLOG_MAGIC ||
        hdr.version != BACKLOG_VERSION ||
        hdr.count > MAX_PENDING) {
        fclose(fp);
        return 0;
    }

    int loaded = 0;
    for (uint32_t i = 0; i < hdr.count; i++) {
        struct pending_msg item;
        if (fread(&item, sizeof(item), 1, fp) != 1) {
            break;
        }
        item.used = 1;
        item.from[MAX_NAME - 1] = '\0';
        item.to[MAX_NAME - 1] = '\0';
        item.text[MAX_TEXT - 1] = '\0';
        pending[loaded++] = item;
    }

    fclose(fp);
    return loaded;
}

static void store_pending(struct pending_msg *pending, long msg_id, const char *from, const char *to, const char *text) {
    int slot = -1;
    int oldest = 0;

    for (int i = 0; i < MAX_PENDING; i++) {
        if (!pending[i].used) {
            slot = i;
            break;
        }
        if (pending[i].created_at < pending[oldest].created_at) {
            oldest = i;
        }
    }

    if (slot < 0) {
        slot = oldest;
    }

    pending[slot].used = 1;
    pending[slot].msg_id = msg_id;
    snprintf(pending[slot].from, sizeof(pending[slot].from), "%.*s", MAX_NAME - 1, from);
    snprintf(pending[slot].to, sizeof(pending[slot].to), "%.*s", MAX_NAME - 1, to);
    snprintf(pending[slot].text, sizeof(pending[slot].text), "%.*s", MAX_TEXT - 1, text);
    pending[slot].created_at = time(NULL);
}

static int salvage_messages(int msqid, long mtype, const char *target_name, struct pending_msg *pending) {
    struct qmsg tmp;
    int salvaged = 0;
    while (msgrcv(msqid, &tmp, MSG_SIZE, mtype, IPC_NOWAIT) >= 0) {
        if (tmp.cmd == CMD_CHAT) {
            tmp.sender_name[MAX_NAME - 1] = '\0';
            tmp.text[MAX_TEXT - 1] = '\0';
            store_pending(pending, tmp.msg_id, tmp.sender_name, target_name, tmp.text);
            salvaged++;
        }
    }
    return salvaged;
}

static int expire_pending(struct pending_msg *pending, time_t now) {
    int expired = 0;
    for (int i = 0; i < MAX_PENDING; i++) {
        if (pending[i].used && (now - pending[i].created_at) > OFFLINE_TTL) {
            pending[i].used = 0;
            expired++;
        }
    }
    return expired;
}

static int deliver_pending(int msqid, struct pending_msg *pending, const char *name, long target_id) {
    int delivered = 0;
    for (int i = 0; i < MAX_PENDING; i++) {
        if (pending[i].used && strcmp(pending[i].to, name) == 0) {
            struct qmsg chat;
            memset(&chat, 0, sizeof(chat));
            chat.mtype = target_id;
            chat.cmd = CMD_CHAT;
            chat.msg_id = pending[i].msg_id;
            snprintf(chat.sender_name, sizeof(chat.sender_name), "%.*s", MAX_NAME - 1, pending[i].from);
            snprintf(chat.text, sizeof(chat.text), "%.*s", MAX_TEXT - 1, pending[i].text);
            if (msgsnd(msqid, &chat, MSG_SIZE, 0) == 0) {
                pending[i].used = 0;
                delivered++;
            }
        }
    }
    if (delivered > 0) {
        char ts[16];
        get_timestamp(ts, sizeof(ts));
        printf(CLR_DIM "[%s] " CLR_RESET CLR_CYAN "-> delivered %d stored msg(s) to %s\n" CLR_RESET,
               ts, delivered, name);
        fflush(stdout);
    }
    return delivered;
}

static int send_reply(int msqid, long reply_type, long target_id, const char *text) {
    struct qmsg rep;
    memset(&rep, 0, sizeof(rep));
    rep.mtype = reply_type;
    rep.cmd = CMD_REPLY;
    rep.target_id = target_id;
    if (text != NULL) {
        snprintf(rep.text, sizeof(rep.text), "%.*s", MAX_TEXT - 1, text);
    }
    return msgsnd(msqid, &rep, MSG_SIZE, 0);
}

static int run_arbiter(int msqid) {
    struct client_entry clients[MAX_CLIENTS];
    struct pending_msg pending[MAX_PENDING];
    memset(clients, 0, sizeof(clients));
    memset(pending, 0, sizeof(pending));
    long next_id = 2;

    int restored = load_backlog(pending);
    int dropped = expire_pending(pending, time(NULL));
    if (dropped > 0) {
        restored -= dropped;
        save_backlog(pending);
    }

    print_arbiter_banner(msqid, restored);

    while (running) {
        time_t now = time(NULL);
        if (expire_pending(pending, now) > 0) {
            save_backlog(pending);
        }

        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].active && (now - clients[i].last_seen) > HEARTBEAT_TIMEOUT) {
                char ts[16];
                get_timestamp(ts, sizeof(ts));
                printf(CLR_DIM "[%s] " CLR_RESET CLR_RED "- timeout: %s (id=%ld)\n" CLR_RESET,
                       ts, clients[i].name, clients[i].id);
                fflush(stdout);
                if (salvage_messages(msqid, clients[i].id, clients[i].name, pending) > 0) {
                    save_backlog(pending);
                }
                drain_type(msqid, clients[i].reply_type);
                clients[i].active = 0;
            }
        }

        struct qmsg msg;
        ssize_t n = msgrcv(msqid, &msg, MSG_SIZE, ARBITER_TYPE, IPC_NOWAIT);
        if (n < 0) {
            if (errno == ENOMSG) {
                usleep(50000);
                continue;
            }
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        if (msg.cmd == CMD_REGISTER) {
            int match_idx = -1;
            int free_idx = -1;
            int oldest_offline = -1;

            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].used && strcmp(clients[i].name, msg.name) == 0) {
                    match_idx = i;
                    break;
                }
                if (!clients[i].used && free_idx < 0) {
                    free_idx = i;
                } else if (clients[i].used && !clients[i].active) {
                    if (oldest_offline < 0 || clients[i].last_seen < clients[oldest_offline].last_seen) {
                        oldest_offline = i;
                    }
                }
            }

            if (msg.name[0] == '\0') {
                send_reply(msqid, msg.reply_type, -1, "invalid name");
            } else if (match_idx >= 0) {
                if (clients[match_idx].active) {
                    send_reply(msqid, msg.reply_type, -1, "name already taken");
                } else {
                    clients[match_idx].active = 1;
                    clients[match_idx].reply_type = msg.reply_type;
                    clients[match_idx].last_seen = time(NULL);
                    send_reply(msqid, msg.reply_type, clients[match_idx].id, "ok");

                    char ts[16];
                    get_timestamp(ts, sizeof(ts));
                    printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "+ reconnected: %s (id=%ld)\n" CLR_RESET,
                           ts, clients[match_idx].name, clients[match_idx].id);
                    fflush(stdout);
                    if (deliver_pending(msqid, pending, clients[match_idx].name, clients[match_idx].id) > 0) {
                        save_backlog(pending);
                    }
                }
            } else {
                int idx = (free_idx >= 0) ? free_idx : oldest_offline;
                if (idx < 0) {
                    send_reply(msqid, msg.reply_type, -1, "server full");
                } else {
                    clients[idx].used = 1;
                    clients[idx].active = 1;
                    clients[idx].id = next_id++;
                    clients[idx].reply_type = msg.reply_type;
                    snprintf(clients[idx].name, sizeof(clients[idx].name), "%.*s", MAX_NAME - 1, msg.name);
                    clients[idx].last_seen = time(NULL);
                    send_reply(msqid, msg.reply_type, clients[idx].id, "ok");

                    char ts[16];
                    get_timestamp(ts, sizeof(ts));
                    printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "+ registered: %s (id=%ld)\n" CLR_RESET,
                           ts, clients[idx].name, clients[idx].id);
                    fflush(stdout);
                    if (deliver_pending(msqid, pending, clients[idx].name, clients[idx].id) > 0) {
                        save_backlog(pending);
                    }
                }
            }
        } else if (msg.cmd == CMD_HEARTBEAT) {
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active && clients[i].id == msg.sender_id) {
                    clients[i].last_seen = time(NULL);
                    break;
                }
            }
        } else if (msg.cmd == CMD_LOOKUP) {
            long found_id = -1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active) {
                    if (clients[i].id == msg.sender_id) {
                        clients[i].last_seen = time(NULL);
                    }
                    if (strcmp(clients[i].name, msg.name) == 0) {
                        found_id = clients[i].id;
                    }
                }
            }
            send_reply(msqid, msg.reply_type, found_id, found_id > 0 ? "ok" : "offline");
        } else if (msg.cmd == CMD_WHO) {
            char list[MAX_TEXT];
            size_t pos = 0;
            long count = 0;
            list[0] = '\0';

            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active) {
                    if (clients[i].id == msg.sender_id) {
                        clients[i].last_seen = time(NULL);
                    }
                    int written = snprintf(list + pos, sizeof(list) - pos, "%s%s",
                                           count > 0 ? ", " : "", clients[i].name);
                    if (written > 0 && pos + (size_t)written < sizeof(list)) {
                        pos += (size_t)written;
                    }
                    count++;
                }
            }
            send_reply(msqid, msg.reply_type, count, list);
        } else if (msg.cmd == CMD_STORE) {
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active && clients[i].id == msg.sender_id) {
                    clients[i].last_seen = time(NULL);
                    break;
                }
            }
            msg.sender_name[MAX_NAME - 1] = '\0';
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';
            store_pending(pending, msg.msg_id, msg.sender_name, msg.name, msg.text);
            save_backlog(pending);

            char ts[16];
            get_timestamp(ts, sizeof(ts));
            printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "~ stored offline: %s -> %s (#%ld)\n" CLR_RESET,
                   ts, msg.sender_name, msg.name, msg.msg_id);
            fflush(stdout);
        } else if (msg.cmd == CMD_ACK) {
            msg.name[MAX_NAME - 1] = '\0';
            msg.sender_name[MAX_NAME - 1] = '\0';
            long sender_target = -1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active) {
                    if (clients[i].id == msg.sender_id) {
                        clients[i].last_seen = time(NULL);
                    }
                    if (strcmp(clients[i].name, msg.name) == 0) {
                        sender_target = clients[i].id;
                    }
                }
            }
            if (sender_target > 0) {
                struct qmsg ack;
                memset(&ack, 0, sizeof(ack));
                ack.mtype = sender_target;
                ack.cmd = CMD_ACK;
                ack.msg_id = msg.msg_id;
                snprintf(ack.sender_name, sizeof(ack.sender_name), "%.*s", MAX_NAME - 1, msg.sender_name);
                msgsnd(msqid, &ack, MSG_SIZE, IPC_NOWAIT);
            }

            char ts[16];
            get_timestamp(ts, sizeof(ts));
            printf(CLR_DIM "[%s] " CLR_RESET CLR_BLUE "✓ ack: %s -> %s (#%ld)\n" CLR_RESET,
                   ts, msg.sender_name, msg.name, msg.msg_id);
            fflush(stdout);
        } else if (msg.cmd == CMD_LOGOUT) {
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active && clients[i].id == msg.sender_id) {
                    char ts[16];
                    get_timestamp(ts, sizeof(ts));
                    printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "- logout: %s (id=%ld)\n" CLR_RESET,
                           ts, clients[i].name, clients[i].id);
                    fflush(stdout);
                    if (salvage_messages(msqid, clients[i].id, clients[i].name, pending) > 0) {
                        save_backlog(pending);
                    }
                    drain_type(msqid, clients[i].reply_type);
                    clients[i].active = 0;
                    break;
                }
            }
        }
    }

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active) {
            salvage_messages(msqid, clients[i].id, clients[i].name, pending);
        }
    }
    save_backlog(pending);

    msgctl(msqid, IPC_RMID, NULL);
    char ts[16];
    get_timestamp(ts, sizeof(ts));
    printf("\n" CLR_DIM "[%s] " CLR_RESET CLR_RED "stopped, backlog saved, queue removed\n" CLR_RESET, ts);
    return 0;
}

static void *recv_worker(void *arg) {
    (void)arg;
    struct qmsg msg;

    while (running) {
        char cur_name[MAX_NAME];
        pthread_mutex_lock(&g_mtx);
        long my_id = g_my_id;
        snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
        pthread_mutex_unlock(&g_mtx);

        if (my_id <= 0) {
            usleep(50000);
            continue;
        }

        ssize_t n = msgrcv(g_msqid, &msg, MSG_SIZE, my_id, IPC_NOWAIT);
        if (n >= 0) {
            char ts[16];
            get_timestamp(ts, sizeof(ts));

            if (msg.cmd == CMD_CHAT) {
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';

                pthread_mutex_lock(&g_io_mtx);
                printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_BOLD CLR_CYAN "%s" CLR_RESET ": %s\n",
                       ts, msg.sender_name, msg.text);
                print_prompt_unlocked(cur_name);
                pthread_mutex_unlock(&g_io_mtx);

                if (msg.msg_id > 0 && msg.sender_name[0] != '\0') {
                    struct qmsg ack;
                    memset(&ack, 0, sizeof(ack));
                    ack.mtype = ARBITER_TYPE;
                    ack.cmd = CMD_ACK;
                    ack.sender_id = my_id;
                    ack.msg_id = msg.msg_id;
                    snprintf(ack.name, sizeof(ack.name), "%.*s", MAX_NAME - 1, msg.sender_name);
                    snprintf(ack.sender_name, sizeof(ack.sender_name), "%.*s", MAX_NAME - 1, cur_name);
                    msgsnd(g_msqid, &ack, MSG_SIZE, IPC_NOWAIT);
                }
            } else if (msg.cmd == CMD_ACK) {
                msg.sender_name[MAX_NAME - 1] = '\0';

                pthread_mutex_lock(&g_io_mtx);
                printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_BLUE "✓ delivered to %s (#%ld)\n" CLR_RESET,
                       ts, msg.sender_name, msg.msg_id);
                print_prompt_unlocked(cur_name);
                pthread_mutex_unlock(&g_io_mtx);
            }
        } else {
            if (errno == ENOMSG) {
                usleep(50000);
            } else if (errno == EINTR) {
                continue;
            } else {
                pthread_mutex_lock(&g_io_mtx);
                printf("\r\033[K" CLR_RED "Connection lost (arbiter stopped).\n" CLR_RESET);
                fflush(stdout);
                pthread_mutex_unlock(&g_io_mtx);
                running = 0;
                break;
            }
        }
    }
    return NULL;
}

static void *heartbeat_worker(void *arg) {
    (void)arg;
    struct qmsg hb;
    int ticks = 0;

    while (running) {
        usleep(100000);
        ticks++;
        if (ticks < HEARTBEAT_INTERVAL * 10) {
            continue;
        }
        ticks = 0;

        pthread_mutex_lock(&g_mtx);
        long my_id = g_my_id;
        pthread_mutex_unlock(&g_mtx);

        if (my_id > 0) {
            memset(&hb, 0, sizeof(hb));
            hb.mtype = ARBITER_TYPE;
            hb.cmd = CMD_HEARTBEAT;
            hb.sender_id = my_id;
            if (msgsnd(g_msqid, &hb, MSG_SIZE, 0) < 0 && errno != EINTR) {
                running = 0;
                break;
            }
        }
    }
    return NULL;
}

static void send_logout(void) {
    pthread_mutex_lock(&g_mtx);
    long my_id = g_my_id;
    g_my_id = 0;
    g_my_name[0] = '\0';
    pthread_mutex_unlock(&g_mtx);

    if (my_id > 0) {
        struct qmsg msg;
        memset(&msg, 0, sizeof(msg));
        msg.mtype = ARBITER_TYPE;
        msg.cmd = CMD_LOGOUT;
        msg.sender_id = my_id;
        msg.reply_type = g_reply_type;
        msgsnd(g_msqid, &msg, MSG_SIZE, 0);
    }
}

static int run_client(int msqid) {
    g_msqid = msqid;
    g_reply_type = REPLY_BASE + (long)getpid() * 2L;
    int interactive = isatty(STDIN_FILENO);

    pthread_t t_recv;
    pthread_t t_hb;
    if (pthread_create(&t_recv, NULL, recv_worker, NULL) != 0) {
        perror("pthread_create");
        return 1;
    }
    if (pthread_create(&t_hb, NULL, heartbeat_worker, NULL) != 0) {
        running = 0;
        pthread_join(t_recv, NULL);
        perror("pthread_create");
        return 1;
    }

    print_client_banner();
    print_prompt();

    char line[MAX_NAME + MAX_TEXT + 4];
    while (running && fgets(line, sizeof(line), stdin) != NULL) {
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0') {
            print_prompt();
            continue;
        }

        if (strcmp(line, "?") == 0 || strcasecmp(line, "/who") == 0) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_WHO;
            req.sender_id = cur_id;
            req.reply_type = g_reply_type;

            if (msgsnd(msqid, &req, MSG_SIZE, 0) < 0) {
                perror("msgsnd");
                break;
            }

            struct qmsg rep;
            if (msgrcv(msqid, &rep, MSG_SIZE, g_reply_type, 0) < 0) {
                perror("msgrcv");
                break;
            }

            rep.text[MAX_TEXT - 1] = '\0';
            pthread_mutex_lock(&g_io_mtx);
            printf(CLR_CYAN "* Online (%ld): %s\n" CLR_RESET,
                   rep.target_id, rep.text[0] != '\0' ? rep.text : "none");
            pthread_mutex_unlock(&g_io_mtx);
            print_prompt();
            continue;
        }

        if (strcmp(line, "##") == 0) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id > 0) {
                send_logout();
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_YELLOW "* Logged out\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
            } else {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Not registered\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
            }
            print_prompt();
            continue;
        }

        if (line[0] == '#') {
            const char *name = line + 1;
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id > 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Already registered, use ## first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }
            if (name[0] == '\0' || strchr(name, '#') != NULL || strchr(name, ',') != NULL || strlen(name) >= MAX_NAME) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Invalid name\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_REGISTER;
            req.reply_type = g_reply_type;
            snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, name);

            if (msgsnd(msqid, &req, MSG_SIZE, 0) < 0) {
                perror("msgsnd");
                break;
            }

            struct qmsg rep;
            if (msgrcv(msqid, &rep, MSG_SIZE, g_reply_type, 0) < 0) {
                perror("msgrcv");
                break;
            }

            if (rep.target_id > 0) {
                pthread_mutex_lock(&g_mtx);
                g_my_id = rep.target_id;
                g_next_msg_id = 1;
                snprintf(g_my_name, sizeof(g_my_name), "%.*s", MAX_NAME - 1, name);
                pthread_mutex_unlock(&g_mtx);

                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_GREEN "* Registered as %s (id=%ld)\n" CLR_RESET, name, rep.target_id);
                pthread_mutex_unlock(&g_io_mtx);
            } else {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Registration failed: %s\n" CLR_RESET, rep.text);
                pthread_mutex_unlock(&g_io_mtx);
            }
            print_prompt();
            continue;
        }

        char *sep = strchr(line, '#');
        if (sep != NULL && sep != line) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            char cur_name[MAX_NAME];
            snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Register first with #<name>\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            *sep = '\0';
            const char *target_name = line;
            const char *text = sep + 1;

            if (strcmp(target_name, cur_name) == 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Cannot send message to yourself\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_LOOKUP;
            req.sender_id = cur_id;
            req.reply_type = g_reply_type;
            snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, target_name);

            if (msgsnd(msqid, &req, MSG_SIZE, 0) < 0) {
                perror("msgsnd");
                break;
            }

            struct qmsg rep;
            if (msgrcv(msqid, &rep, MSG_SIZE, g_reply_type, 0) < 0) {
                perror("msgrcv");
                break;
            }

            long msg_id = g_next_msg_id++;
            char ts[16];
            get_timestamp(ts, sizeof(ts));

            if (rep.target_id > 0) {
                struct qmsg chat;
                memset(&chat, 0, sizeof(chat));
                chat.mtype = rep.target_id;
                chat.cmd = CMD_CHAT;
                chat.sender_id = cur_id;
                chat.msg_id = msg_id;
                snprintf(chat.sender_name, sizeof(chat.sender_name), "%.*s", MAX_NAME - 1, cur_name);
                snprintf(chat.text, sizeof(chat.text), "%.*s", MAX_TEXT - 1, text);

                if (msgsnd(msqid, &chat, MSG_SIZE, 0) < 0) {
                    perror("msgsnd");
                    break;
                }

                pthread_mutex_lock(&g_io_mtx);
                if (interactive) {
                    printf("\033[1A\r\033[K");
                }
                printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "you -> %s (#%ld)" CLR_RESET ": %s\n",
                       ts, target_name, msg_id, text);
                pthread_mutex_unlock(&g_io_mtx);
            } else {
                struct qmsg st;
                memset(&st, 0, sizeof(st));
                st.mtype = ARBITER_TYPE;
                st.cmd = CMD_STORE;
                st.sender_id = cur_id;
                st.msg_id = msg_id;
                snprintf(st.name, sizeof(st.name), "%.*s", MAX_NAME - 1, target_name);
                snprintf(st.sender_name, sizeof(st.sender_name), "%.*s", MAX_NAME - 1, cur_name);
                snprintf(st.text, sizeof(st.text), "%.*s", MAX_TEXT - 1, text);

                if (msgsnd(msqid, &st, MSG_SIZE, 0) < 0) {
                    perror("msgsnd");
                    break;
                }

                pthread_mutex_lock(&g_io_mtx);
                if (interactive) {
                    printf("\033[1A\r\033[K");
                }
                printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "you -> %s (#%ld, offline, queued %ds)" CLR_RESET ": %s\n",
                       ts, target_name, msg_id, OFFLINE_TTL, text);
                pthread_mutex_unlock(&g_io_mtx);
            }
            print_prompt();
            continue;
        }

        pthread_mutex_lock(&g_io_mtx);
        printf(CLR_YELLOW "! Format: #<name> | <name>#<message> | ? | ##\n" CLR_RESET);
        pthread_mutex_unlock(&g_io_mtx);
        print_prompt();
    }

    send_logout();
    running = 0;
    pthread_join(t_recv, NULL);
    pthread_join(t_hb, NULL);
    printf("\n");
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 2 || (strcasecmp(argv[1], "arbiter") != 0 && strcasecmp(argv[1], "client") != 0)) {
        fprintf(stderr, "Usage: %s <arbiter|client>\n", argv[0]);
        return 1;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    key_t key = ftok(QUEUE_KEY_PATH, QUEUE_KEY_PROJ);
    if (key == (key_t)-1) {
        perror("ftok");
        return 1;
    }

    int is_arbiter = (strcasecmp(argv[1], "arbiter") == 0);
    int flags = is_arbiter ? (0666 | IPC_CREAT) : 0666;
    int msqid = msgget(key, flags);
    if (msqid < 0) {
        if (!is_arbiter && errno == ENOENT) {
            fprintf(stderr, "Arbiter is not running. Start '%s arbiter' first.\n", argv[0]);
        } else {
            perror("msgget");
        }
        return 1;
    }

    return is_arbiter ? run_arbiter(msqid) : run_client(msqid);
}
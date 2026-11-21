#define _DEFAULT_SOURCE

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MAX_NAME 32
#define MAX_TEXT 255
#define MAX_CLIENTS 64
#define MAX_PENDING 128
#define ARBITER_TYPE 1L
#define REPLY_BASE 100000L
#define HEARTBEAT_TIMEOUT 10
#define HEARTBEAT_INTERVAL 4
#define OFFLINE_TTL 60

#define CLR_RESET   "\033[0m"
#define CLR_BOLD    "\033[1m"
#define CLR_DIM     "\033[2m"
#define CLR_RED     "\033[31m"
#define CLR_GREEN   "\033[32m"
#define CLR_YELLOW  "\033[33m"
#define CLR_BLUE    "\033[34m"
#define CLR_MAGENTA "\033[35m"
#define CLR_CYAN    "\033[36m"

enum msg_cmd {
    CMD_REGISTER = 1,
    CMD_HEARTBEAT = 2,
    CMD_LOOKUP = 3,
    CMD_LOGOUT = 4,
    CMD_CHAT = 5,
    CMD_REPLY = 6,
    CMD_STORE = 7
};

struct qmsg {
    long mtype;
    int cmd;
    long sender_id;
    long reply_type;
    long target_id;
    char name[MAX_NAME];
    char sender_name[MAX_NAME];
    char text[MAX_TEXT];
};

#define MSG_SIZE (sizeof(struct qmsg) - sizeof(long))

struct client_entry {
    int active;
    long id;
    long reply_type;
    char name[MAX_NAME];
    time_t last_seen;
};

struct pending_msg {
    int used;
    char from[MAX_NAME];
    char to[MAX_NAME];
    char text[MAX_TEXT];
    time_t created_at;
};

static volatile sig_atomic_t running = 1;

static int g_msqid = -1;
static long g_my_id = 0;
static long g_reply_type = 0;
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
    printf("║  " CLR_YELLOW "##" CLR_RESET "             — logout                     " CLR_CYAN "║\n");
    printf("║  " CLR_DIM "Ctrl+D" CLR_RESET "         — exit                       " CLR_CYAN "║\n");
    printf("╚══════════════════════════════════════════════╝\n" CLR_RESET);
    fflush(stdout);
    pthread_mutex_unlock(&g_io_mtx);
}

static void print_arbiter_banner(int msqid) {
    printf(CLR_MAGENTA "╔══════════════════════════════════════════════╗\n");
    printf("║" CLR_BOLD "            Queue Messenger Arbiter           " CLR_RESET CLR_MAGENTA "║\n");
    printf("╚══════════════════════════════════════════════╝\n" CLR_RESET);
    char ts[16];
    get_timestamp(ts, sizeof(ts));
    printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "started" CLR_RESET " (msqid=%d, heartbeat=%ds, offline_ttl=%ds)\n",
           ts, msqid, HEARTBEAT_TIMEOUT, OFFLINE_TTL);
    fflush(stdout);
}

static void drain_type(int msqid, long mtype) {
    struct qmsg tmp;
    while (msgrcv(msqid, &tmp, MSG_SIZE, mtype, IPC_NOWAIT) >= 0) {
    }
}

static void store_pending(struct pending_msg *pending, const char *from, const char *to, const char *text) {
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
    snprintf(pending[slot].from, sizeof(pending[slot].from), "%.*s", MAX_NAME - 1, from);
    snprintf(pending[slot].to, sizeof(pending[slot].to), "%.*s", MAX_NAME - 1, to);
    snprintf(pending[slot].text, sizeof(pending[slot].text), "%.*s", MAX_TEXT - 1, text);
    pending[slot].created_at = time(NULL);
}

static void salvage_messages(int msqid, long mtype, const char *target_name, struct pending_msg *pending) {
    struct qmsg tmp;
    while (msgrcv(msqid, &tmp, MSG_SIZE, mtype, IPC_NOWAIT) >= 0) {
        if (tmp.cmd == CMD_CHAT) {
            tmp.sender_name[MAX_NAME - 1] = '\0';
            tmp.text[MAX_TEXT - 1] = '\0';
            store_pending(pending, tmp.sender_name, target_name, tmp.text);
        }
    }
}

static void expire_pending(struct pending_msg *pending, time_t now) {
    for (int i = 0; i < MAX_PENDING; i++) {
        if (pending[i].used && (now - pending[i].created_at) > OFFLINE_TTL) {
            pending[i].used = 0;
        }
    }
}

static void deliver_pending(int msqid, struct pending_msg *pending, const char *name, long target_id) {
    int delivered = 0;
    for (int i = 0; i < MAX_PENDING; i++) {
        if (pending[i].used && strcmp(pending[i].to, name) == 0) {
            struct qmsg chat;
            memset(&chat, 0, sizeof(chat));
            chat.mtype = target_id;
            chat.cmd = CMD_CHAT;
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

    print_arbiter_banner(msqid);

    while (running) {
        time_t now = time(NULL);
        expire_pending(pending, now);

        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].active && (now - clients[i].last_seen) > HEARTBEAT_TIMEOUT) {
                char ts[16];
                get_timestamp(ts, sizeof(ts));
                printf(CLR_DIM "[%s] " CLR_RESET CLR_RED "- timeout: %s (id=%ld)\n" CLR_RESET,
                       ts, clients[i].name, clients[i].id);
                fflush(stdout);
                salvage_messages(msqid, clients[i].id, clients[i].name, pending);
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
            int exists = 0;
            int free_idx = -1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active) {
                    if (strcmp(clients[i].name, msg.name) == 0) {
                        exists = 1;
                    }
                } else if (free_idx < 0) {
                    free_idx = i;
                }
            }

            if (exists || free_idx < 0 || msg.name[0] == '\0') {
                send_reply(msqid, msg.reply_type, -1, exists ? "name already taken" : "server full");
            } else {
                clients[free_idx].active = 1;
                clients[free_idx].id = next_id++;
                clients[free_idx].reply_type = msg.reply_type;
                snprintf(clients[free_idx].name, sizeof(clients[free_idx].name), "%.*s", MAX_NAME - 1, msg.name);
                clients[free_idx].last_seen = time(NULL);
                send_reply(msqid, msg.reply_type, clients[free_idx].id, "ok");

                char ts[16];
                get_timestamp(ts, sizeof(ts));
                printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "+ registered: %s (id=%ld)\n" CLR_RESET,
                       ts, clients[free_idx].name, clients[free_idx].id);
                fflush(stdout);
                deliver_pending(msqid, pending, clients[free_idx].name, clients[free_idx].id);
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
            store_pending(pending, msg.sender_name, msg.name, msg.text);

            char ts[16];
            get_timestamp(ts, sizeof(ts));
            printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "~ stored offline: %s -> %s\n" CLR_RESET,
                   ts, msg.sender_name, msg.name);
            fflush(stdout);
        } else if (msg.cmd == CMD_LOGOUT) {
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active && clients[i].id == msg.sender_id) {
                    char ts[16];
                    get_timestamp(ts, sizeof(ts));
                    printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "- logout: %s (id=%ld)\n" CLR_RESET,
                           ts, clients[i].name, clients[i].id);
                    fflush(stdout);
                    salvage_messages(msqid, clients[i].id, clients[i].name, pending);
                    drain_type(msqid, clients[i].reply_type);
                    clients[i].active = 0;
                    break;
                }
            }
        }
    }

    msgctl(msqid, IPC_RMID, NULL);
    char ts[16];
    get_timestamp(ts, sizeof(ts));
    printf("\n" CLR_DIM "[%s] " CLR_RESET CLR_RED "stopped, queue removed\n" CLR_RESET, ts);
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
            if (msg.cmd == CMD_CHAT) {
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';
                char ts[16];
                get_timestamp(ts, sizeof(ts));

                pthread_mutex_lock(&g_io_mtx);
                printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_BOLD CLR_CYAN "%s" CLR_RESET ": %s\n",
                       ts, msg.sender_name, msg.text);
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
    g_reply_type = REPLY_BASE + (long)getpid();
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
            if (name[0] == '\0' || strchr(name, '#') != NULL || strlen(name) >= MAX_NAME) {
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

            char ts[16];
            get_timestamp(ts, sizeof(ts));

            if (rep.target_id > 0) {
                struct qmsg chat;
                memset(&chat, 0, sizeof(chat));
                chat.mtype = rep.target_id;
                chat.cmd = CMD_CHAT;
                chat.sender_id = cur_id;
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
                printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "you -> %s" CLR_RESET ": %s\n",
                       ts, target_name, text);
                pthread_mutex_unlock(&g_io_mtx);
            } else {
                struct qmsg st;
                memset(&st, 0, sizeof(st));
                st.mtype = ARBITER_TYPE;
                st.cmd = CMD_STORE;
                st.sender_id = cur_id;
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
                printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "you -> %s (offline, queued %ds)" CLR_RESET ": %s\n",
                       ts, target_name, OFFLINE_TTL, text);
                pthread_mutex_unlock(&g_io_mtx);
            }
            print_prompt();
            continue;
        }

        pthread_mutex_lock(&g_io_mtx);
        printf(CLR_YELLOW "! Format: #<name> | <name>#<message> | ##\n" CLR_RESET);
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

    key_t key = ftok(".", 'Q');
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
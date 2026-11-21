#define _DEFAULT_SOURCE

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MAX_NAME 32
#define MAX_TEXT 255
#define MAX_CLIENTS 64
#define ARBITER_TYPE 1L
#define REPLY_BASE 100000L
#define HEARTBEAT_TIMEOUT 10
#define HEARTBEAT_INTERVAL 4

enum msg_cmd {
    CMD_REGISTER = 1,
    CMD_HEARTBEAT = 2,
    CMD_LOOKUP = 3,
    CMD_LOGOUT = 4,
    CMD_CHAT = 5,
    CMD_REPLY = 6
};

struct qmsg {
    long mtype;
    int cmd;
    long sender_id;
    long reply_type;
    long target_id;
    char name[MAX_NAME];
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

static volatile sig_atomic_t running = 1;

static int g_msqid = -1;
static long g_my_id = 0;
static long g_reply_type = 0;
static char g_my_name[MAX_NAME];
static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;

static void on_signal(int sig) {
    (void)sig;
    running = 0;
}

static void drain_type(int msqid, long mtype) {
    struct qmsg tmp;
    while (msgrcv(msqid, &tmp, MSG_SIZE, mtype, IPC_NOWAIT) >= 0) {
    }
}

static int send_reply(int msqid, long reply_type, long target_id, const char *text) {
    struct qmsg rep;
    memset(&rep, 0, sizeof(rep));
    rep.mtype = reply_type;
    rep.cmd = CMD_REPLY;
    rep.target_id = target_id;
    if (text != NULL) {
        snprintf(rep.text, sizeof(rep.text), "%s", text);
    }
    return msgsnd(msqid, &rep, MSG_SIZE, 0);
}

static int run_arbiter(int msqid) {
    struct client_entry clients[MAX_CLIENTS];
    memset(clients, 0, sizeof(clients));
    long next_id = 2;

    printf("[arbiter] started (msqid=%d)\n", msqid);
    fflush(stdout);

    while (running) {
        time_t now = time(NULL);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].active && (now - clients[i].last_seen) > HEARTBEAT_TIMEOUT) {
                printf("[arbiter] timeout: %s (id=%ld)\n", clients[i].name, clients[i].id);
                fflush(stdout);
                drain_type(msqid, clients[i].id);
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
                snprintf(clients[free_idx].name, sizeof(clients[free_idx].name), "%s", msg.name);
                clients[free_idx].last_seen = time(NULL);
                send_reply(msqid, msg.reply_type, clients[free_idx].id, "ok");
                printf("[arbiter] registered: %s (id=%ld)\n", clients[free_idx].name, clients[free_idx].id);
                fflush(stdout);
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
            send_reply(msqid, msg.reply_type, found_id, found_id > 0 ? "ok" : "not found");
        } else if (msg.cmd == CMD_LOGOUT) {
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].active && clients[i].id == msg.sender_id) {
                    printf("[arbiter] logout: %s (id=%ld)\n", clients[i].name, clients[i].id);
                    fflush(stdout);
                    drain_type(msqid, clients[i].id);
                    drain_type(msqid, clients[i].reply_type);
                    clients[i].active = 0;
                    break;
                }
            }
        }
    }

    msgctl(msqid, IPC_RMID, NULL);
    printf("\n[arbiter] stopped, queue removed\n");
    return 0;
}

static void *recv_worker(void *arg) {
    (void)arg;
    struct qmsg msg;

    while (running) {
        pthread_mutex_lock(&g_mtx);
        long my_id = g_my_id;
        pthread_mutex_unlock(&g_mtx);

        if (my_id <= 0) {
            usleep(50000);
            continue;
        }

        ssize_t n = msgrcv(g_msqid, &msg, MSG_SIZE, my_id, IPC_NOWAIT);
        if (n >= 0) {
            if (msg.cmd == CMD_CHAT) {
                msg.name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';
                printf("\n[%s]: %s\n", msg.name, msg.text);
                fflush(stdout);
            }
        } else {
            if (errno == ENOMSG) {
                usleep(50000);
            } else if (errno == EINTR) {
                continue;
            } else {
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

    char line[MAX_NAME + MAX_TEXT + 4];
    while (running && fgets(line, sizeof(line), stdin) != NULL) {
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0') {
            continue;
        }

        if (strcmp(line, "##") == 0) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id > 0) {
                send_logout();
                printf("logged out\n");
            } else {
                printf("not registered\n");
            }
            continue;
        }

        if (line[0] == '#') {
            const char *name = line + 1;
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id > 0) {
                printf("already registered, use ## first\n");
                continue;
            }
            if (name[0] == '\0' || strchr(name, '#') != NULL || strlen(name) >= MAX_NAME) {
                printf("invalid name\n");
                continue;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_REGISTER;
            req.reply_type = g_reply_type;
            snprintf(req.name, sizeof(req.name), "%s", name);

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
                snprintf(g_my_name, sizeof(g_my_name), "%s", name);
                pthread_mutex_unlock(&g_mtx);
                printf("registered as %s (id=%ld)\n", name, rep.target_id);
            } else {
                printf("registration failed: %s\n", rep.text);
            }
            continue;
        }

        char *sep = strchr(line, '#');
        if (sep != NULL && sep != line) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            char cur_name[MAX_NAME];
            snprintf(cur_name, sizeof(cur_name), "%s", g_my_name);
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                printf("register first with #<name>\n");
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
            snprintf(req.name, sizeof(req.name), "%s", target_name);

            if (msgsnd(msqid, &req, MSG_SIZE, 0) < 0) {
                perror("msgsnd");
                break;
            }

            struct qmsg rep;
            if (msgrcv(msqid, &rep, MSG_SIZE, g_reply_type, 0) < 0) {
                perror("msgrcv");
                break;
            }

            if (rep.target_id <= 0) {
                printf("user %s not found\n", target_name);
                continue;
            }

            struct qmsg chat;
            memset(&chat, 0, sizeof(chat));
            chat.mtype = rep.target_id;
            chat.cmd = CMD_CHAT;
            chat.sender_id = cur_id;
            snprintf(chat.name, sizeof(chat.name), "%s", cur_name);
            snprintf(chat.text, sizeof(chat.text), "%s", text);

            if (msgsnd(msqid, &chat, MSG_SIZE, 0) < 0) {
                perror("msgsnd");
                break;
            }
            continue;
        }

        printf("format: #<name> | <name>#<message> | ##\n");
    }

    send_logout();
    running = 0;
    pthread_join(t_recv, NULL);
    pthread_join(t_hb, NULL);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 2 || (strcmp(argv[1], "arbiter") != 0 && strcmp(argv[1], "client") != 0)) {
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

    int is_arbiter = (strcmp(argv[1], "arbiter") == 0);
    int flags = is_arbiter ? (0666 | IPC_CREAT) : 0666;
    int msqid = msgget(key, flags);
    if (msqid < 0) {
        perror("msgget");
        return 1;
    }

    return is_arbiter ? run_arbiter(msqid) : run_client(msqid);
}
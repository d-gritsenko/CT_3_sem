#define _DEFAULT_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/file.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "db.h"
#include "protocol.h"

#define BACKLOG_FILE "chat_backlog.dat"
#define BACKLOG_TMP  "chat_backlog.dat.tmp"
#define BACKLOG_MAGIC 0x51424B4CU
#define BACKLOG_VERSION 1U

#define ARBITER_LOG_PATH "arbiter.log"

#define MAX_HISTORY_LOAD 64
#define MAX_ROOM_HISTORY_LOAD 32

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
    int is_debug;
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


struct room_info {
    int used;
    char name[MAX_NAME];
    char owner[MAX_NAME];
    char topic[MAX_TEXT];
    char pinned[MAX_TEXT];
    long members[MAX_CLIENTS];
    int member_count;
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

static int find_client_by_id(struct client_entry *clients, long id) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].used && clients[i].id == id) {
            return i;
        }
    }
    return -1;
}

static int find_client_by_name(struct client_entry *clients, const char *name) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].used && strcmp(clients[i].name, name) == 0) {
            return i;
        }
    }
    return -1;
}


static int find_room_by_name(const struct room_info *rooms, const char *name) {
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (rooms[i].used && strcmp(rooms[i].name, name) == 0) {
            return i;
        }
    }
    return -1;
}

static int find_room_member_pos(const struct room_info *r, long client_id) {
    for (int i = 0; i < r->member_count; i++) {
        if (r->members[i] == client_id) {
            return i;
        }
    }
    return -1;
}

static void remove_room_member_at(struct room_info *r, int pos) {
    if (pos < 0 || pos >= r->member_count) {
        return;
    }
    for (int i = pos; i < r->member_count - 1; i++) {
        r->members[i] = r->members[i + 1];
    }
    r->member_count--;
}

static int alloc_client_slot(struct client_entry *clients) {
    int free_idx = -1;
    int oldest_offline = -1;
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i].used && free_idx < 0) {
            free_idx = i;
        } else if (clients[i].used && !clients[i].active) {
            if (oldest_offline < 0 || clients[i].last_seen < clients[oldest_offline].last_seen) {
                oldest_offline = i;
            }
        }
    }
    return (free_idx >= 0) ? free_idx : oldest_offline;
}


static unsigned long g_file_seq = 0;

static int activate_client_slot(struct client_entry *clients, int match_idx, long *next_id,
                                int is_debug, long reply_type, const char *name) {
    int idx = (match_idx >= 0) ? match_idx : alloc_client_slot(clients);
    if (idx < 0) {
        return -1;
    }
    if (match_idx < 0) {
        clients[idx].id = (*next_id)++;
    }
    clients[idx].used = 1;
    clients[idx].active = 1;
    clients[idx].is_debug = is_debug;
    clients[idx].reply_type = reply_type;
    snprintf(clients[idx].name, sizeof(clients[idx].name), "%.*s", MAX_NAME - 1, name);
    clients[idx].last_seen = time(NULL);
    return idx;
}

static int send_msg_retry(int msqid, const struct qmsg *msg) {
    for (int i = 0; i < 10; i++) {
        if (msgsnd(msqid, msg, MSG_SIZE, IPC_NOWAIT) == 0) {
            return 0;
        }
        if (errno == EAGAIN) {
            usleep(2000);
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        return -1;
    }
    return -1;
}

static void load_room_meta_into_memory(struct room_info *r, const char *fallback_owner) {
    if (r == NULL || r->name[0] == '\0') {
        return;
    }
    struct db_room_meta meta;
    if (db_room_get_meta(r->name, &meta) == 0) {
        if (meta.owner[0] != '\0') {
            snprintf(r->owner, sizeof(r->owner), "%.*s", MAX_NAME - 1, meta.owner);
        }
        copy_utf8_safe(r->topic, sizeof(r->topic), meta.topic);
        copy_utf8_safe(r->pinned, sizeof(r->pinned), meta.pinned_text);
    }
    if (r->owner[0] == '\0' && fallback_owner != NULL && fallback_owner[0] != '\0') {
        snprintf(r->owner, sizeof(r->owner), "%.*s", MAX_NAME - 1, fallback_owner);
        db_room_ensure_meta(r->name, r->owner);
    }
}

static void send_room_meta_to_client(int msqid, long client_id, const struct room_info *r) {
    if (r == NULL || client_id <= 0) {
        return;
    }
    struct qmsg info;
    memset(&info, 0, sizeof(info));
    info.mtype = client_id;
    info.cmd = CMD_ROOM_INFO;
    info.msg_id = 0;
    snprintf(info.name, sizeof(info.name), "%.*s", MAX_NAME - 1, r->name);
    snprintf(info.sender_name, sizeof(info.sender_name), "%.*s", MAX_NAME - 1, r->owner);
    copy_utf8_safe(info.text, sizeof(info.text), r->topic);
    send_msg_retry(msqid, &info);

    memset(&info, 0, sizeof(info));
    info.mtype = client_id;
    info.cmd = CMD_ROOM_INFO;
    info.msg_id = 1;
    snprintf(info.name, sizeof(info.name), "%.*s", MAX_NAME - 1, r->name);
    snprintf(info.sender_name, sizeof(info.sender_name), "%.*s", MAX_NAME - 1, r->owner);
    copy_utf8_safe(info.text, sizeof(info.text), r->pinned);
    send_msg_retry(msqid, &info);
}

static void broadcast_room_meta(int msqid, struct client_entry *clients, const struct room_info *r, long field_type) {
    if (r == NULL) {
        return;
    }
    for (int i = 0; i < r->member_count; i++) {
        long mid = r->members[i];
        int midx = find_client_by_id(clients, mid);
        if (midx >= 0 && clients[midx].active) {
            struct qmsg info;
            memset(&info, 0, sizeof(info));
            info.mtype = mid;
            info.cmd = CMD_ROOM_INFO;
            info.msg_id = field_type;
            snprintf(info.name, sizeof(info.name), "%.*s", MAX_NAME - 1, r->name);
            snprintf(info.sender_name, sizeof(info.sender_name), "%.*s", MAX_NAME - 1, r->owner);
            copy_utf8_safe(info.text, sizeof(info.text), field_type == 0 ? r->topic : r->pinned);
            msgsnd(msqid, &info, MSG_SIZE, IPC_NOWAIT);
        }
    }
}

static int build_room_members_list(struct client_entry *clients, const struct room_info *r,
                                   char *out, size_t out_sz) {
    out[0] = '\0';
    if (r == NULL) {
        return 0;
    }
    char names[MAX_CLIENTS][MAX_NAME];
    int ncount = db_room_load_members(r->name, names, MAX_CLIENTS);

    for (int i = 0; i < r->member_count && ncount < MAX_CLIENTS; i++) {
        int cidx = find_client_by_id(clients, r->members[i]);
        if (cidx < 0) {
            continue;
        }
        int dup = 0;
        for (int j = 0; j < ncount; j++) {
            if (strcmp(names[j], clients[cidx].name) == 0) {
                dup = 1;
                break;
            }
        }
        if (!dup) {
            snprintf(names[ncount], MAX_NAME, "%.*s", MAX_NAME - 1, clients[cidx].name);
            ncount++;
        }
    }

    size_t pos = 0;
    for (int i = 0; i < ncount; i++) {
        int tidx = find_client_by_name(clients, names[i]);
        int is_on = (tidx >= 0 && clients[tidx].active) ? 1 : 0;
        int w = snprintf(out + pos, out_sz - pos, "%s%d %s", i > 0 ? "," : "", is_on, names[i]);
        if (w > 0 && pos + (size_t)w < out_sz) {
            pos += (size_t)w;
        }
    }
    return ncount;
}


static void restore_user_rooms_in_memory(struct room_info *rooms, const char *username, long client_id) {
    char saved_rooms[MAX_ROOMS][MAX_NAME];
    int nrooms = db_load_user_rooms(username, saved_rooms, MAX_ROOMS);
    for (int r = 0; r < nrooms; r++) {
        int ridx = -1;
        int free_idx = -1;
        for (int i = 0; i < MAX_ROOMS; i++) {
            if (rooms[i].used && strcmp(rooms[i].name, saved_rooms[r]) == 0) {
                ridx = i;
                break;
            }
            if (!rooms[i].used && free_idx < 0) {
                free_idx = i;
            }
        }
        if (ridx < 0) {
            if (free_idx < 0) {
                continue;
            }

            ridx = free_idx;
            memset(&rooms[ridx], 0, sizeof(rooms[ridx]));
            rooms[ridx].used = 1;
            rooms[ridx].member_count = 0;
            snprintf(rooms[ridx].name, sizeof(rooms[ridx].name), "%.*s", MAX_NAME - 2, saved_rooms[r]);
            load_room_meta_into_memory(&rooms[ridx], username);
        }
        int already = 0;
        for (int j = 0; j < rooms[ridx].member_count; j++) {
            if (rooms[ridx].members[j] == client_id) {
                already = 1;
                break;
            }
        }
        if (!already && rooms[ridx].member_count < MAX_CLIENTS) {
            rooms[ridx].members[rooms[ridx].member_count++] = client_id;
        }
    }
}

static void send_history_records(int msqid, long target_id, const char *viewer_name,
                                 const struct db_msg_record *recs, int count) {
    for (int i = 0; i < count; i++) {
        struct qmsg item;
        memset(&item, 0, sizeof(item));
        item.mtype = target_id;
        item.cmd = CMD_HISTORY_ITEM;

        item.target_id = recs[i].is_room ? 1 : 0;
        long hflags = 0;
        if (strcmp(recs[i].sender, viewer_name) == 0) {
            hflags |= 1;
        }
        if (recs[i].is_unread) {
            hflags |= 2;
        }
        if (recs[i].is_edited) {
            hflags |= 4;
        }
        item.sender_id = hflags;
        item.msg_id = recs[i].id;
        item.timestamp = recs[i].timestamp;
        snprintf(item.name, sizeof(item.name), "%.*s", MAX_NAME - 1, recs[i].peer);
        snprintf(item.sender_name, sizeof(item.sender_name), "%.*s", MAX_NAME - 1, recs[i].sender);
        copy_utf8_safe(item.text, sizeof(item.text), recs[i].text);
        send_msg_retry(msqid, &item);
    }
}

static int broadcast_to_room(int msqid, struct client_entry *clients, struct room_info *room,
                             long exclude_id, const char *sender_name, long msg_id, time_t ts, const char *text) {
    int delivered = 0;
    for (int i = 0; i < room->member_count; i++) {
        long mid = room->members[i];
        if (mid == exclude_id) {
            continue;
        }
        int cidx = find_client_by_id(clients, mid);
        if (cidx >= 0 && clients[cidx].active) {
            struct qmsg out;
            memset(&out, 0, sizeof(out));
            out.mtype = mid;
            out.cmd = CMD_ROOM_MSG;
            out.sender_id = exclude_id;
            out.msg_id = msg_id;
            out.timestamp = (long)ts;
            snprintf(out.name, sizeof(out.name), "%.*s", MAX_NAME - 1, room->name);
            snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, sender_name);
            copy_utf8_safe(out.text, sizeof(out.text), text);
            if (msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT) == 0) {
                delivered++;
            }
        }
    }
    return delivered;
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
    printf(CLR_CYAN "╔══════════════════════════════════════════════════╗\n");
    printf("║" CLR_BOLD "                    DeepSpeak                     " CLR_RESET CLR_CYAN "║\n");
    printf("╠══════════════════════════════════════════════════╣\n");
    printf("║  " CLR_GREEN "#<name> <pass>" CLR_RESET " — register account              " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "@<name> <pass>" CLR_RESET " — login (user* = debug)         " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "<name>#<text>" CLR_RESET "  — send message                  " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "/join <room>" CLR_RESET "   — join/create room              " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "/leave <room>" CLR_RESET "  — leave room                    " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "/rooms" CLR_RESET "         — list active rooms             " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "/r <room> <tx>" CLR_RESET " — send to room                  " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "/history [u|#r]" CLR_RESET "— load chat history             " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "/delchat <u|#r>" CLR_RESET "— delete chat or room history   " CLR_CYAN "║\n");

    printf("║  " CLR_GREEN "/sendfile <t> <p>" CLR_RESET "— send file (DM or #room)      " CLR_CYAN "║\n");
    printf("║  " CLR_GREEN "/getfile <id> [p]" CLR_RESET "— download file                " CLR_CYAN "║\n");
    printf("║  " CLR_CYAN "? / /who" CLR_RESET "       — online users                  " CLR_CYAN "║\n");
    printf("║  " CLR_YELLOW "##" CLR_RESET "             — logout                        " CLR_CYAN "║\n");
    printf("║  " CLR_DIM "Ctrl+D" CLR_RESET "         — exit                          " CLR_CYAN "║\n");
    printf("╚══════════════════════════════════════════════════╝\n" CLR_RESET);
    fflush(stdout);
    pthread_mutex_unlock(&g_io_mtx);
}

static void print_arbiter_banner(int msqid, int restored) {
    printf(CLR_MAGENTA "╔══════════════════════════════════════════════════╗\n");
    printf("║" CLR_BOLD "                DeepSpeak Arbiter                 " CLR_RESET CLR_MAGENTA "║\n");
    printf("╚══════════════════════════════════════════════════╝\n" CLR_RESET);
    char ts[16];
    get_timestamp(ts, sizeof(ts));
    printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "started" CLR_RESET
           " (msqid=%d, db=%s, heartbeat=%ds, offline_ttl=%ds, restored=%d)\n",
           ts, msqid, USERS_DB_PATH, HEARTBEAT_TIMEOUT, OFFLINE_TTL, restored);
    fflush(stdout);
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
            if (db_is_debug_name(tmp.sender_name) || db_is_debug_name(target_name)) {
                store_pending(pending, tmp.msg_id, tmp.sender_name, target_name, tmp.text);
                salvaged++;
            }
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
            chat.timestamp = (long)pending[i].created_at;
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

static int send_reply_id(int msqid, long reply_type, long target_id, long msg_id, const char *text) {
    struct qmsg rep;
    memset(&rep, 0, sizeof(rep));
    rep.mtype = reply_type;
    rep.cmd = CMD_REPLY;
    rep.target_id = target_id;
    rep.msg_id = msg_id;
    if (text != NULL) {
        snprintf(rep.text, sizeof(rep.text), "%.*s", MAX_TEXT - 1, text);
    }
    return msgsnd(msqid, &rep, MSG_SIZE, 0);
}

static int send_auth_reply(int msqid, long reply_type, long target_id, long is_debug, long theme_id, const char *text) {
    struct qmsg rep;
    memset(&rep, 0, sizeof(rep));
    rep.mtype = reply_type;
    rep.cmd = CMD_REPLY;
    rep.target_id = target_id;
    rep.sender_id = is_debug;
    rep.msg_id = theme_id;
    if (text != NULL) {
        snprintf(rep.text, sizeof(rep.text), "%.*s", MAX_TEXT - 1, text);
    }
    return msgsnd(msqid, &rep, MSG_SIZE, 0);
}

static int run_arbiter(int msqid) {
    if (db_init(USERS_DB_PATH) != 0) {
        fprintf(stderr, CLR_RED "! Failed to initialize SQLite database (%s)\n" CLR_RESET, USERS_DB_PATH);
        return 1;
    }

    struct client_entry clients[MAX_CLIENTS];
    struct pending_msg pending[MAX_PENDING];
    struct room_info rooms[MAX_ROOMS];
    memset(rooms, 0, sizeof(rooms));
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
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';

            char err[128] = "";
            int match_idx = find_client_by_name(clients, msg.name);


            if (!is_valid_ident(msg.name, MAX_NAME)) {
                send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "invalid username");
            } else if (match_idx >= 0 && clients[match_idx].active) {
                send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "already online");
            } else if (db_register_user(msg.name, msg.text, err, sizeof(err)) != 0) {
                send_auth_reply(msqid, msg.reply_type, -1, 0, 0, err);
            } else {

                int idx = activate_client_slot(clients, match_idx, &next_id, 0, msg.reply_type, msg.name);

                if (idx < 0) {
                    send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "server full");
                } else {
                    send_auth_reply(msqid, msg.reply_type, clients[idx].id, 0, 0, "ok");

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
        } else if (msg.cmd == CMD_LOGIN || msg.cmd == CMD_RECONNECT) {
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';

            int match_idx = find_client_by_name(clients, msg.name);


            if (!is_valid_ident(msg.name, MAX_NAME)) {
                send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "invalid username");
            } else if (strcasecmp(msg.name, "arbiter") == 0) {
                send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "reserved name");
            } else if (match_idx >= 0 && clients[match_idx].active) {
                send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "already online");
            } else if (db_is_debug_name(msg.name)) {
                if (!db_check_debug_pass(msg.text)) {
                    send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "wrong debug password");
                } else {

                    int idx = activate_client_slot(clients, match_idx, &next_id, 1, msg.reply_type, msg.name);

                    if (idx < 0) {
                        send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "server full");
                    } else {
                        send_auth_reply(msqid, msg.reply_type, clients[idx].id, 1, 0, "ok");

                        char ts[16];
                        get_timestamp(ts, sizeof(ts));
                        printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "[DEBUG] + login: %s (id=%ld)\n" CLR_RESET,
                               ts, clients[idx].name, clients[idx].id);
                        fflush(stdout);

                        if (deliver_pending(msqid, pending, clients[idx].name, clients[idx].id) > 0) {
                            save_backlog(pending);
                        }
                    }
                }
            } else {
                int theme_id = 0;
                char err[128] = "";
                if (db_verify_user(msg.name, msg.text, &theme_id, err, sizeof(err)) != 0) {
                    send_auth_reply(msqid, msg.reply_type, -1, 0, 0, err);
                } else {

                    int idx = activate_client_slot(clients, match_idx, &next_id, 0, msg.reply_type, msg.name);

                    if (idx < 0) {
                        send_auth_reply(msqid, msg.reply_type, -1, 0, 0, "server full");
                    } else {
                        restore_user_rooms_in_memory(rooms, clients[idx].name, clients[idx].id);
                        send_auth_reply(msqid, msg.reply_type, clients[idx].id, 0, theme_id, "ok");

                        char ts[16];
                        get_timestamp(ts, sizeof(ts));
                        printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "+ login: %s (id=%ld, theme=%d)\n" CLR_RESET,
                               ts, clients[idx].name, clients[idx].id, theme_id);
                        fflush(stdout);

                        if (deliver_pending(msqid, pending, clients[idx].name, clients[idx].id) > 0) {
                            save_backlog(pending);
                        }
                    }
                }
            }
        } else if (msg.cmd == CMD_CHAT) {
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';

            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx < 0 || !clients[cidx].active) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "not logged in");
                }
            } else if (msg.name[0] == '\0' || strcmp(msg.name, clients[cidx].name) == 0 ||
                       strcasecmp(msg.name, "arbiter") == 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "invalid recipient");
                }
            } else {
                clients[cidx].last_seen = time(NULL);
                time_t msg_ts = time(NULL);


                long db_id = db_save_dm(clients[cidx].name, msg.name, msg.text, msg_ts);
                long eff_id = (db_id > 0) ? db_id : msg.msg_id;

                int tidx = find_client_by_name(clients, msg.name);
                if (tidx >= 0 && clients[tidx].active) {
                    struct qmsg out;
                    memset(&out, 0, sizeof(out));
                    out.mtype = clients[tidx].id;
                    out.cmd = CMD_CHAT;
                    out.sender_id = clients[cidx].id;

                    out.msg_id = eff_id;
                    out.timestamp = (long)msg_ts;
                    snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                    copy_utf8_safe(out.text, sizeof(out.text), msg.text);
                    msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT);

                    if (msg.reply_type > 0) {

                        send_reply_id(msqid, msg.reply_type, clients[tidx].id, eff_id, "ok");
                    }
                } else {
                    if (clients[cidx].is_debug || db_is_debug_name(msg.name)) {
                        store_pending(pending, msg.msg_id, clients[cidx].name, msg.name, msg.text);
                        save_backlog(pending);
                    }
                    if (msg.reply_type > 0) {

                        send_reply_id(msqid, msg.reply_type, 0, eff_id, "offline");
                    }
                    char ts[16];
                    get_timestamp(ts, sizeof(ts));

                    printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "~ offline msg: %s -> %s (#%ld)\n" CLR_RESET,

                           ts, clients[cidx].name, msg.name, eff_id);
                    fflush(stdout);
                }
            }
        } else if (msg.cmd == CMD_HISTORY) {
            msg.name[MAX_NAME - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx >= 0 && clients[cidx].active && !clients[cidx].is_debug) {
                clients[cidx].last_seen = time(NULL);
                struct db_msg_record recs[MAX_HISTORY_LOAD];

                if (msg.name[0] == '#') {
                    const char *rname = msg.name + 1;
                    int cnt = db_load_room_history(rname, clients[cidx].name, recs, MAX_ROOM_HISTORY_LOAD);
                    send_history_records(msqid, clients[cidx].id, clients[cidx].name, recs, cnt);
                } else if (msg.name[0] != '\0') {
                    int cnt = db_load_dm_history(clients[cidx].name, msg.name, recs, MAX_HISTORY_LOAD);
                    send_history_records(msqid, clients[cidx].id, clients[cidx].name, recs, cnt);
                } else {
                    int cnt = db_load_dm_history(clients[cidx].name, NULL, recs, MAX_HISTORY_LOAD);
                    send_history_records(msqid, clients[cidx].id, clients[cidx].name, recs, cnt);

                    char saved_rooms[MAX_ROOMS][MAX_NAME];
                    int nrooms = db_load_user_rooms(clients[cidx].name, saved_rooms, MAX_ROOMS);
                    for (int r = 0; r < nrooms; r++) {
                        int rcnt = db_load_room_history(saved_rooms[r], clients[cidx].name, recs, MAX_ROOM_HISTORY_LOAD);
                        send_history_records(msqid, clients[cidx].id, clients[cidx].name, recs, rcnt);



                        int ri = find_room_by_name(rooms, saved_rooms[r]);

                        if (ri >= 0) {

                            send_room_meta_to_client(msqid, clients[cidx].id, &rooms[ri]);
                            send_room_meta_to_client(msqid, clients[cidx].id, &rooms[ri]);

                        }

                    }

                }

            }

        } else if (msg.cmd == CMD_DELETE_CHAT) {

            msg.name[MAX_NAME - 1] = '\0';

            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx < 0 || !clients[cidx].active) {
                send_reply(msqid, msg.reply_type, -1, "not logged in");
            } else if (msg.name[0] == '\0') {
                send_reply(msqid, msg.reply_type, -1, "invalid peer");
            } else {
                clients[cidx].last_seen = time(NULL);
                db_delete_dm_for_user(clients[cidx].name, msg.name);
                send_reply(msqid, msg.reply_type, 1, "deleted");
                char ts[16];
                get_timestamp(ts, sizeof(ts));
                printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "x chat deleted: %s <-> %s\n" CLR_RESET,
                       ts, clients[cidx].name, msg.name);
                fflush(stdout);
            }
        } else if (msg.cmd == CMD_DELETE_ROOM) {
            msg.name[MAX_NAME - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx < 0 || !clients[cidx].active) {
                send_reply(msqid, msg.reply_type, -1, "not logged in");
            } else if (msg.name[0] == '\0') {
                send_reply(msqid, msg.reply_type, -1, "invalid room");
            } else {
                clients[cidx].last_seen = time(NULL);


                int remaining = 0;
                int ridx = find_room_by_name(rooms, msg.name);

                if (ridx >= 0) {

                    int mpos = find_room_member_pos(&rooms[ridx], msg.sender_id);

                    if (mpos >= 0) {

                        remove_room_member_at(&rooms[ridx], mpos);

                        if (rooms[ridx].member_count > 0) {

                            char note[MAX_TEXT];

                            snprintf(note, sizeof(note), "%s left #%s", clients[cidx].name, rooms[ridx].name);

                            broadcast_to_room(msqid, clients, &rooms[ridx], msg.sender_id, "arbiter", 0, time(NULL), note);

                        } else {

                            rooms[ridx].used = 0;

                        }

                    }

                    remaining = rooms[ridx].used ? rooms[ridx].member_count : 0;

                }
                db_delete_room_for_user(clients[cidx].name, msg.name, remaining);
                send_reply(msqid, msg.reply_type, 1, "deleted");
                char ts[16];
                get_timestamp(ts, sizeof(ts));
                printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "x room deleted for %s: #%s\n" CLR_RESET,
                       ts, clients[cidx].name, msg.name);
                fflush(stdout);
            }
        } else if (msg.cmd == CMD_SET_THEME) {
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx >= 0 && clients[cidx].active) {
                clients[cidx].last_seen = time(NULL);
                if (!clients[cidx].is_debug) {
                    db_set_theme(clients[cidx].name, (int)msg.msg_id);
                }
            }

        } else if (msg.cmd == CMD_MARK_READ) {
            msg.name[MAX_NAME - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx >= 0 && clients[cidx].active && !clients[cidx].is_debug && msg.name[0] != '\0') {
                clients[cidx].last_seen = time(NULL);
                if (msg.target_id != 0) {
                    db_mark_room_read(clients[cidx].name, msg.name);

                } else {
                    int updated = db_mark_dm_read(clients[cidx].name, msg.name);
                    if (updated > 0) {
                        int tidx = find_client_by_name(clients, msg.name);
                        if (tidx >= 0 && clients[tidx].active) {
                            struct qmsg ack;
                            memset(&ack, 0, sizeof(ack));
                            ack.mtype = clients[tidx].id;
                            ack.cmd = CMD_ACK;
                            ack.msg_id = 0;
                            snprintf(ack.sender_name, sizeof(ack.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                            msgsnd(msqid, &ack, MSG_SIZE, IPC_NOWAIT);
                        }
                    }
                }
            }

        } else if (msg.cmd == CMD_TYPING) {
            msg.name[MAX_NAME - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx >= 0 && clients[cidx].active && msg.name[0] != '\0') {
                clients[cidx].last_seen = time(NULL);

                if (msg.target_id != 0) {

                    int ridx = find_room_by_name(rooms, msg.name);

                    if (ridx >= 0 && find_room_member_pos(&rooms[ridx], msg.sender_id) >= 0) {

                        for (int j = 0; j < rooms[ridx].member_count; j++) {

                            long mid = rooms[ridx].members[j];

                            if (mid == msg.sender_id) {

                                continue;

                            }

                            int midx = find_client_by_id(clients, mid);

                            if (midx >= 0 && clients[midx].active) {

                                struct qmsg out;

                                memset(&out, 0, sizeof(out));

                                out.mtype = mid;

                                out.cmd = CMD_TYPING;

                                out.target_id = 1;

                                snprintf(out.name, sizeof(out.name), "%.*s", MAX_NAME - 1, rooms[ridx].name);

                                snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);

                                msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT);

                            }

                        }

                    }
                } else {
                    int tidx = find_client_by_name(clients, msg.name);
                    if (tidx >= 0 && clients[tidx].active && tidx != cidx) {
                        struct qmsg out;
                        memset(&out, 0, sizeof(out));
                        out.mtype = clients[tidx].id;
                        out.cmd = CMD_TYPING;
                        out.target_id = 0;
                        snprintf(out.name, sizeof(out.name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                        snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                        msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT);
                    }
                }
            }

        } else if (msg.cmd == CMD_MSG_EDIT) {
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';
            int is_room = (msg.target_id != 0);
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx < 0 || !clients[cidx].active) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "not logged in");
                }
            } else if (clients[cidx].is_debug || msg.msg_id <= 0 || msg.text[0] == '\0') {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "cannot edit message");
                }
            } else {
                clients[cidx].last_seen = time(NULL);
                int changed = db_edit_message(is_room, msg.msg_id, clients[cidx].name, msg.text);
                if (changed <= 0) {
                    if (msg.reply_type > 0) {
                        send_reply(msqid, msg.reply_type, -1, "edit forbidden or not found");
                    }
                } else {
                    if (msg.reply_type > 0) {
                        send_reply(msqid, msg.reply_type, 1, "ok");
                    }
                    if (is_room) {

                        int ridx = find_room_by_name(rooms, msg.name);

                        if (ridx >= 0) {

                            for (int j = 0; j < rooms[ridx].member_count; j++) {

                                long mid = rooms[ridx].members[j];

                                if (mid == msg.sender_id) {

                                    continue;

                                }

                                int midx = find_client_by_id(clients, mid);

                                if (midx >= 0 && clients[midx].active) {

                                    struct qmsg out;

                                    memset(&out, 0, sizeof(out));

                                    out.mtype = mid;

                                    out.cmd = CMD_MSG_EDIT;

                                    out.target_id = 1;

                                    out.msg_id = msg.msg_id;

                                    snprintf(out.name, sizeof(out.name), "%.*s", MAX_NAME - 1, rooms[ridx].name);

                                    snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);

                                    copy_utf8_safe(out.text, sizeof(out.text), msg.text);

                                    msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT);

                                }

                            }

                        }
                    } else {
                        int tidx = find_client_by_name(clients, msg.name);
                        if (tidx >= 0 && clients[tidx].active && tidx != cidx) {
                            struct qmsg out;
                            memset(&out, 0, sizeof(out));
                            out.mtype = clients[tidx].id;
                            out.cmd = CMD_MSG_EDIT;
                            out.target_id = 0;
                            out.msg_id = msg.msg_id;
                            snprintf(out.name, sizeof(out.name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                            snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                            copy_utf8_safe(out.text, sizeof(out.text), msg.text);
                            msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT);
                        }
                    }
                }
            }
        } else if (msg.cmd == CMD_MSG_DELETE) {
            msg.name[MAX_NAME - 1] = '\0';
            int is_room = (msg.target_id != 0);
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx < 0 || !clients[cidx].active) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "not logged in");
                }
            } else if (clients[cidx].is_debug || msg.msg_id <= 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "cannot delete message");
                }
            } else {
                clients[cidx].last_seen = time(NULL);
                int changed = db_delete_message(is_room, msg.msg_id, clients[cidx].name);
                if (changed <= 0) {
                    if (msg.reply_type > 0) {
                        send_reply(msqid, msg.reply_type, -1, "delete forbidden or not found");
                    }
                } else {
                    if (msg.reply_type > 0) {
                        send_reply(msqid, msg.reply_type, 1, "ok");
                    }
                    if (is_room) {

                        int ridx = find_room_by_name(rooms, msg.name);

                        if (ridx >= 0) {

                            for (int j = 0; j < rooms[ridx].member_count; j++) {

                                long mid = rooms[ridx].members[j];

                                if (mid == msg.sender_id) {

                                    continue;

                                }

                                int midx = find_client_by_id(clients, mid);

                                if (midx >= 0 && clients[midx].active) {

                                    struct qmsg out;

                                    memset(&out, 0, sizeof(out));

                                    out.mtype = mid;

                                    out.cmd = CMD_MSG_DELETE;

                                    out.target_id = 1;

                                    out.msg_id = msg.msg_id;

                                    snprintf(out.name, sizeof(out.name), "%.*s", MAX_NAME - 1, rooms[ridx].name);

                                    snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);

                                    msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT);

                                }

                            }

                        }
                    } else {
                        int tidx = find_client_by_name(clients, msg.name);
                        if (tidx >= 0 && clients[tidx].active && tidx != cidx) {
                            struct qmsg out;
                            memset(&out, 0, sizeof(out));
                            out.mtype = clients[tidx].id;
                            out.cmd = CMD_MSG_DELETE;
                            out.target_id = 0;
                            out.msg_id = msg.msg_id;
                            snprintf(out.name, sizeof(out.name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                            snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                            msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT);
                        }
                    }
                }
            }

        } else if (msg.cmd == CMD_FILE_SEND) {
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';
            int shmid = (int)msg.target_id;
            long fsize = msg.timestamp;

            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx < 0 || !clients[cidx].active) {
                if (shmid >= 0) {
                    shmctl(shmid, IPC_RMID, NULL);
                }
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "not logged in");
                }
            } else if (shmid < 0 || fsize <= 0 || fsize > MAX_FILE_SIZE) {
                if (shmid >= 0) {
                    shmctl(shmid, IPC_RMID, NULL);
                }
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "invalid file size or shm");
                }
            } else {
                clients[cidx].last_seen = time(NULL);
                int is_room = (msg.name[0] == '#');
                const char *target = is_room ? (msg.name + 1) : msg.name;

                int ridx = -1;
                if (is_room) {

                    int ri = find_room_by_name(rooms, target);

                    if (ri >= 0 && find_room_member_pos(&rooms[ri], msg.sender_id) >= 0) {

                        ridx = ri;

                    }
                    if (ridx < 0) {
                        shmctl(shmid, IPC_RMID, NULL);
                        if (msg.reply_type > 0) {
                            send_reply(msqid, msg.reply_type, -1, "not in room");
                        }
                        continue;
                    }
                } else if (target[0] == '\0' || strcmp(target, clients[cidx].name) == 0 ||
                           strcasecmp(target, "arbiter") == 0) {
                    shmctl(shmid, IPC_RMID, NULL);
                    if (msg.reply_type > 0) {
                        send_reply(msqid, msg.reply_type, -1, "invalid recipient");
                    }
                    continue;
                }

                mkdir(UPLOADS_DIR, 0755);
                char orig_clean[64];
                char safe_suf[32];
                sanitize_filename(msg.text, orig_clean, sizeof(orig_clean));
                make_safe_storage_suffix(orig_clean, safe_suf, sizeof(safe_suf));

                time_t msg_ts = time(NULL);
                char stored_name[96];
                snprintf(stored_name, sizeof(stored_name), "%ld_%lu_%s",
                         (long)msg_ts, ++g_file_seq, safe_suf);

                char disk_path[256];
                snprintf(disk_path, sizeof(disk_path), "%s/%s", UPLOADS_DIR, stored_name);

                if (shm_save_to_file(shmid, fsize, disk_path) != 0) {
                    if (msg.reply_type > 0) {
                        send_reply(msqid, msg.reply_type, -1, "failed to store file");
                    }
                    continue;
                }

                char marker[MAX_TEXT];
                snprintf(marker, sizeof(marker), "[[FILE:%s|%ld|%s]]", stored_name, fsize, orig_clean);

                char ts[16];
                get_timestamp(ts, sizeof(ts));
                printf(CLR_DIM "[%s] " CLR_RESET CLR_CYAN "@ file: %s -> %s (%s, %ld B)\n" CLR_RESET,
                       ts, clients[cidx].name, msg.name, orig_clean, fsize);
                fflush(stdout);

                if (is_room) {

                    long db_id = db_save_room_msg(rooms[ridx].name, clients[cidx].name, marker, msg_ts);
                    long eff_id = (db_id > 0) ? db_id : msg.msg_id;
                    int cnt = broadcast_to_room(msqid, clients, &rooms[ridx],
                                                msg.sender_id, clients[cidx].name, eff_id, msg_ts, marker);
                    if (msg.reply_type > 0) {
                        send_reply_id(msqid, msg.reply_type, cnt, eff_id, marker);
                    }
                } else {

                    long db_id = db_save_dm(clients[cidx].name, target, marker, msg_ts);
                    long eff_id = (db_id > 0) ? db_id : msg.msg_id;
                    int tidx = find_client_by_name(clients, target);
                    if (tidx >= 0 && clients[tidx].active) {
                        struct qmsg out;
                        memset(&out, 0, sizeof(out));
                        out.mtype = clients[tidx].id;
                        out.cmd = CMD_CHAT;
                        out.sender_id = clients[cidx].id;

                        out.msg_id = eff_id;
                        out.timestamp = (long)msg_ts;
                        snprintf(out.sender_name, sizeof(out.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                        copy_utf8_safe(out.text, sizeof(out.text), marker);
                        msgsnd(msqid, &out, MSG_SIZE, IPC_NOWAIT);

                        if (msg.reply_type > 0) {

                            send_reply_id(msqid, msg.reply_type, clients[tidx].id, eff_id, marker);
                        }
                    } else {
                        if (clients[cidx].is_debug || db_is_debug_name(target)) {
                            store_pending(pending, msg.msg_id, clients[cidx].name, target, marker);
                            save_backlog(pending);
                        }
                        if (msg.reply_type > 0) {

                            send_reply_id(msqid, msg.reply_type, 0, eff_id, marker);
                        }
                    }
                }
            }
        } else if (msg.cmd == CMD_FILE_GET) {
            msg.text[MAX_TEXT - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx < 0 || !clients[cidx].active) {
                send_reply(msqid, msg.reply_type, -1, "not logged in");
            } else if (!is_safe_stored_name(msg.text)) {
                send_reply(msqid, msg.reply_type, -1, "invalid file id");
            } else {
                clients[cidx].last_seen = time(NULL);
                char disk_path[256];

                snprintf(disk_path, sizeof(disk_path), "%s/%.95s", UPLOADS_DIR, msg.text);

                long fsize = 0;
                char err[64] = "";
                int shmid = shm_alloc_from_file(disk_path, &fsize, err, sizeof(err));
                if (shmid < 0) {
                    send_reply(msqid, msg.reply_type, -1, err);
                } else {
                    struct qmsg rep;
                    memset(&rep, 0, sizeof(rep));
                    rep.mtype = msg.reply_type;
                    rep.cmd = CMD_REPLY;
                    rep.target_id = shmid;
                    rep.timestamp = fsize;
                    snprintf(rep.text, sizeof(rep.text), "ok");
                    if (msgsnd(msqid, &rep, MSG_SIZE, 0) < 0) {
                        shmctl(shmid, IPC_RMID, NULL);
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
        } else if (msg.cmd == CMD_ROOM_JOIN) {
            int cidx = find_client_by_id(clients, msg.sender_id);
            if (cidx < 0 || !clients[cidx].active) {
                send_reply(msqid, msg.reply_type, -1, "not logged in");
            } else if (msg.name[0] == '\0' || strchr(msg.name, '#') != NULL ||
                       strchr(msg.name, ',') != NULL || strchr(msg.name, ' ') != NULL ||
                       strlen(msg.name) >= MAX_NAME - 1) {
                send_reply(msqid, msg.reply_type, -1, "invalid room name");
            } else {
                int ridx = -1;
                int free_idx = -1;
                for (int i = 0; i < MAX_ROOMS; i++) {
                    if (rooms[i].used && strcmp(rooms[i].name, msg.name) == 0) {
                        ridx = i;
                        break;
                    }
                    if (!rooms[i].used && free_idx < 0) {
                        free_idx = i;
                    }
                }
                if (ridx < 0) {
                    if (free_idx < 0) {
                        send_reply(msqid, msg.reply_type, -1, "too many rooms");
                    } else {

                        ridx = free_idx;
                        memset(&rooms[ridx], 0, sizeof(rooms[ridx]));
                        rooms[ridx].used = 1;
                        rooms[ridx].member_count = 0;
                        snprintf(rooms[ridx].name, sizeof(rooms[ridx].name), "%.*s", MAX_NAME - 2, msg.name);
                        rooms[ridx].members[rooms[ridx].member_count++] = msg.sender_id;
                        if (!clients[cidx].is_debug) {
                            db_room_add_member(rooms[ridx].name, clients[cidx].name);
                            db_room_ensure_meta(rooms[ridx].name, clients[cidx].name);
                            load_room_meta_into_memory(&rooms[ridx], clients[cidx].name);
                        } else {
                            snprintf(rooms[ridx].owner, sizeof(rooms[ridx].owner), "%.*s", MAX_NAME - 1, clients[cidx].name);
                        }
                        send_reply(msqid, msg.reply_type, rooms[ridx].member_count, "created");
                        send_room_meta_to_client(msqid, clients[cidx].id, &rooms[ridx]);

                        if (!clients[cidx].is_debug) {
                            struct db_msg_record recs[MAX_ROOM_HISTORY_LOAD];
                            int rcnt = db_load_room_history(rooms[ridx].name, clients[cidx].name, recs, MAX_ROOM_HISTORY_LOAD);
                            send_history_records(msqid, clients[cidx].id, clients[cidx].name, recs, rcnt);
                        }

                        char ts[16];
                        get_timestamp(ts, sizeof(ts));
                        printf(CLR_DIM "[%s] " CLR_RESET CLR_CYAN "# room created: #%s by %s\n" CLR_RESET,
                               ts, rooms[ridx].name, clients[cidx].name);
                        fflush(stdout);
                    }
                } else {

                    int already = (find_room_member_pos(&rooms[ridx], msg.sender_id) >= 0);
                    if (!already && rooms[ridx].member_count < MAX_CLIENTS) {
                        rooms[ridx].members[rooms[ridx].member_count++] = msg.sender_id;
                        if (!clients[cidx].is_debug) {
                            db_room_add_member(rooms[ridx].name, clients[cidx].name);
                        }
                        char note[MAX_TEXT];
                        snprintf(note, sizeof(note), "%s joined #%s", clients[cidx].name, rooms[ridx].name);
                        broadcast_to_room(msqid, clients, &rooms[ridx], msg.sender_id, "arbiter", 0, time(NULL), note);
                    }
                    send_reply(msqid, msg.reply_type, rooms[ridx].member_count, "joined");

                    send_room_meta_to_client(msqid, clients[cidx].id, &rooms[ridx]);
                    if (!already && !clients[cidx].is_debug) {
                        struct db_msg_record recs[MAX_ROOM_HISTORY_LOAD];
                        int rcnt = db_load_room_history(rooms[ridx].name, clients[cidx].name, recs, MAX_ROOM_HISTORY_LOAD);
                        send_history_records(msqid, clients[cidx].id, clients[cidx].name, recs, rcnt);
                    }
                }
            }
        } else if (msg.cmd == CMD_ROOM_LEAVE) {
            int cidx = find_client_by_id(clients, msg.sender_id);

            int ridx = find_room_by_name(rooms, msg.name);
            if (ridx < 0) {
                send_reply(msqid, msg.reply_type, -1, "room not found");
            } else {

                int mpos = find_room_member_pos(&rooms[ridx], msg.sender_id);
                if (mpos < 0) {
                    send_reply(msqid, msg.reply_type, -1, "not in room");
                } else {

                    remove_room_member_at(&rooms[ridx], mpos);
                    if (cidx >= 0 && !clients[cidx].is_debug) {
                        db_room_remove_member(rooms[ridx].name, clients[cidx].name);
                    }
                    if (cidx >= 0 && rooms[ridx].member_count > 0) {
                        char note[MAX_TEXT];
                        snprintf(note, sizeof(note), "%s left #%s", clients[cidx].name, rooms[ridx].name);
                        broadcast_to_room(msqid, clients, &rooms[ridx], msg.sender_id, "arbiter", 0, time(NULL), note);
                    }
                    if (rooms[ridx].member_count == 0) {
                        rooms[ridx].used = 0;
                    }
                    send_reply(msqid, msg.reply_type, 1, "left");
                }
            }
        } else if (msg.cmd == CMD_ROOM_MSG) {
            int cidx = find_client_by_id(clients, msg.sender_id);

            int ridx = find_room_by_name(rooms, msg.name);
            if (cidx < 0 || !clients[cidx].active) {
                send_reply(msqid, msg.reply_type, -1, "not logged in");
            } else if (ridx < 0) {
                send_reply(msqid, msg.reply_type, -1, "room not found");
            } else {

                int in_room = (find_room_member_pos(&rooms[ridx], msg.sender_id) >= 0);
                if (!in_room) {
                    send_reply(msqid, msg.reply_type, -1, "not in room");
                } else {

                    time_t msg_ts = time(NULL);
                    long db_id = db_save_room_msg(rooms[ridx].name, clients[cidx].name, msg.text, msg_ts);
                    long eff_id = (db_id > 0) ? db_id : msg.msg_id;
                    int cnt = broadcast_to_room(msqid, clients, &rooms[ridx],
                                                msg.sender_id, clients[cidx].name, eff_id, msg_ts, msg.text);
                    send_reply_id(msqid, msg.reply_type, cnt, eff_id, "ok");
                }
            }
        } else if (msg.cmd == CMD_ROOM_LIST) {
            char list[MAX_TEXT];
            list[0] = '\0';
            size_t pos = 0;
            long count = 0;
            for (int i = 0; i < MAX_ROOMS; i++) {
                if (!rooms[i].used) {
                    continue;
                }

                int is_member = (find_room_member_pos(&rooms[i], msg.sender_id) >= 0);
                int written = snprintf(list + pos, sizeof(list) - pos, "%s%s%s(%d)",
                                       count > 0 ? ", " : "",
                                       is_member ? "*" : "",
                                       rooms[i].name,
                                       rooms[i].member_count);
                if (written > 0 && pos + (size_t)written < sizeof(list)) {
                    pos += (size_t)written;
                }
                count++;
            }
            send_reply(msqid, msg.reply_type, count, list);

        } else if (msg.cmd == CMD_ROOM_INFO) {
            msg.name[MAX_NAME - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);

            int ridx = find_room_by_name(rooms, msg.name);
            if (cidx < 0 || !clients[cidx].active) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "not logged in");
                }
            } else if (ridx < 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "room not found");
                }
            } else {
                clients[cidx].last_seen = time(NULL);
                send_room_meta_to_client(msqid, clients[cidx].id, &rooms[ridx]);
                if (msg.reply_type > 0) {
                    char mlist[MAX_TEXT];
                    int mcnt = build_room_members_list(clients, &rooms[ridx], mlist, sizeof(mlist));
                    struct qmsg rep;
                    memset(&rep, 0, sizeof(rep));
                    rep.mtype = msg.reply_type;
                    rep.cmd = CMD_REPLY;
                    rep.target_id = mcnt;
                    snprintf(rep.sender_name, sizeof(rep.sender_name), "%.*s", MAX_NAME - 1, rooms[ridx].owner);
                    copy_utf8_safe(rep.text, sizeof(rep.text), mlist);
                    msgsnd(msqid, &rep, MSG_SIZE, 0);
                }
            }
        } else if (msg.cmd == CMD_ROOM_KICK) {
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);

            int ridx = find_room_by_name(rooms, msg.name);
            if (cidx < 0 || !clients[cidx].active) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "not logged in");
                }
            } else if (ridx < 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "room not found");
                }
            } else if (rooms[ridx].owner[0] == '\0' || strcmp(rooms[ridx].owner, clients[cidx].name) != 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "only room owner can kick");
                }
            } else if (msg.text[0] == '\0' || strcmp(msg.text, clients[cidx].name) == 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "invalid target");
                }
            } else {
                clients[cidx].last_seen = time(NULL);
                int tidx = find_client_by_name(clients, msg.text);

                long target_id = (tidx >= 0) ? clients[tidx].id : -1;

                if (target_id > 0) {
                    remove_room_member_at(&rooms[ridx], find_room_member_pos(&rooms[ridx], target_id));
                }

                db_room_remove_member(rooms[ridx].name, msg.text);

                if (tidx >= 0 && clients[tidx].active) {
                    struct qmsg kmsg;
                    memset(&kmsg, 0, sizeof(kmsg));
                    kmsg.mtype = clients[tidx].id;
                    kmsg.cmd = CMD_ROOM_KICK;
                    snprintf(kmsg.name, sizeof(kmsg.name), "%.*s", MAX_NAME - 1, rooms[ridx].name);
                    snprintf(kmsg.sender_name, sizeof(kmsg.sender_name), "%.*s", MAX_NAME - 1, clients[cidx].name);
                    msgsnd(msqid, &kmsg, MSG_SIZE, IPC_NOWAIT);
                }

                char note[MAX_TEXT];

                snprintf(note, sizeof(note), "%.*s was kicked by %.*s",

                         MAX_NAME - 1, msg.text, MAX_NAME - 1, clients[cidx].name);
                broadcast_to_room(msqid, clients, &rooms[ridx], -1, "arbiter", 0, time(NULL), note);

                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, 1, "kicked");
                }
                char ts[16];
                get_timestamp(ts, sizeof(ts));
                printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "! kick: %s removed %s from #%s\n" CLR_RESET,
                       ts, clients[cidx].name, msg.text, rooms[ridx].name);
                fflush(stdout);
            }
        } else if (msg.cmd == CMD_ROOM_PIN) {
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);

            int ridx = find_room_by_name(rooms, msg.name);
            if (cidx < 0 || !clients[cidx].active) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "not logged in");
                }
            } else if (ridx < 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "room not found");
                }
            } else if (rooms[ridx].owner[0] == '\0' || strcmp(rooms[ridx].owner, clients[cidx].name) != 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "only room owner can pin");
                }
            } else {
                clients[cidx].last_seen = time(NULL);
                copy_utf8_safe(rooms[ridx].pinned, sizeof(rooms[ridx].pinned), msg.text);
                if (!clients[cidx].is_debug) {
                    db_room_set_pin(rooms[ridx].name, rooms[ridx].pinned);
                }
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, 1, "ok");
                }
                broadcast_room_meta(msqid, clients, &rooms[ridx], 1);
                char note[MAX_TEXT];
                snprintf(note, sizeof(note), "%s %s", clients[cidx].name,
                         rooms[ridx].pinned[0] != '\0' ? "pinned a message" : "unpinned the message");
                broadcast_to_room(msqid, clients, &rooms[ridx], -1, "arbiter", 0, time(NULL), note);
            }
        } else if (msg.cmd == CMD_ROOM_TOPIC) {
            msg.name[MAX_NAME - 1] = '\0';
            msg.text[MAX_TEXT - 1] = '\0';
            int cidx = find_client_by_id(clients, msg.sender_id);

            int ridx = find_room_by_name(rooms, msg.name);
            if (cidx < 0 || !clients[cidx].active) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "not logged in");
                }
            } else if (ridx < 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "room not found");
                }
            } else if (rooms[ridx].owner[0] == '\0' || strcmp(rooms[ridx].owner, clients[cidx].name) != 0) {
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, -1, "only room owner can change topic");
                }
            } else {
                clients[cidx].last_seen = time(NULL);
                copy_utf8_safe(rooms[ridx].topic, sizeof(rooms[ridx].topic), msg.text);
                if (!clients[cidx].is_debug) {
                    db_room_set_topic(rooms[ridx].name, rooms[ridx].topic);
                }
                if (msg.reply_type > 0) {
                    send_reply(msqid, msg.reply_type, 1, "ok");
                }
                broadcast_room_meta(msqid, clients, &rooms[ridx], 0);
                char note[MAX_TEXT];
                snprintf(note, sizeof(note), "%s updated room topic", clients[cidx].name);
                broadcast_to_room(msqid, clients, &rooms[ridx], -1, "arbiter", 0, time(NULL), note);
            }
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
    db_close();
    msgctl(msqid, IPC_RMID, NULL);

    char ts[16];
    get_timestamp(ts, sizeof(ts));
    printf("\n" CLR_DIM "[%s] " CLR_RESET CLR_RED "stopped, backlog saved, db closed, queue removed\n" CLR_RESET, ts);
    return 0;
}


static void format_cli_body(const char *text, char *out, size_t out_size) {
    char stored[96];
    char orig[96];
    long fsize = 0;

    if (parse_file_marker(text, stored, sizeof(stored), &fsize, orig, sizeof(orig))) {
        char sz_str[32];
        format_file_size(fsize, sz_str, sizeof(sz_str));
        snprintf(out, out_size, "[FILE] %s (%s) — /getfile %s", orig, sz_str, stored);
        return;
    }
    char rauthor[MAX_NAME];
    char rsnip[64];
    const char *rbody = NULL;
    if (parse_reply_prefix(text, rauthor, sizeof(rauthor), rsnip, sizeof(rsnip), &rbody)) {
        snprintf(out, out_size, "[↳ %s: %s] %s", rauthor, rsnip, rbody);
        return;
    }

    copy_utf8_safe(out, out_size, text);
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
            char ts[24];
            if (msg.timestamp > 0) {
                format_timestamp_from((time_t)msg.timestamp, ts, sizeof(ts));
            } else {
                get_timestamp(ts, sizeof(ts));
            }

            if (msg.cmd == CMD_CHAT) {
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';
                pthread_mutex_lock(&g_io_mtx);

                char body[MAX_TEXT + 64];
                format_cli_body(msg.text, body, sizeof(body));
                printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_BOLD CLR_CYAN "%s" CLR_RESET ": %s\n",

                       ts, msg.sender_name, body);
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
            } else if (msg.cmd == CMD_HISTORY_ITEM) {
                msg.name[MAX_NAME - 1] = '\0';
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';
                pthread_mutex_lock(&g_io_mtx);

                int is_out = (msg.sender_id & 1) != 0;
                const char *ed = ((msg.sender_id & 4) != 0) ? " (edited)" : "";
                char body[MAX_TEXT + 64];
                format_cli_body(msg.text, body, sizeof(body));
                if (msg.target_id != 0) {
                    printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "[#%s] " CLR_CYAN "%s" CLR_RESET ": %s%s\n",
                           ts, msg.name, is_out ? "you" : msg.sender_name, body, ed);
                } else {
                    if (is_out) {
                        printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_GREEN "you -> %s" CLR_RESET ": %s%s\n",
                               ts, msg.name, body, ed);
                    } else {
                        printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_CYAN "%s" CLR_RESET ": %s%s\n",
                               ts, msg.sender_name, body, ed);
                    }
                }
                print_prompt_unlocked(cur_name);
                pthread_mutex_unlock(&g_io_mtx);
            } else if (msg.cmd == CMD_ACK) {
                msg.sender_name[MAX_NAME - 1] = '\0';
                pthread_mutex_lock(&g_io_mtx);

                if (msg.msg_id == 0) {
                    printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_BLUE "✓✓ %s read your messages\n" CLR_RESET,
                           ts, msg.sender_name);
                } else {
                    printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_BLUE "✓ delivered to %s (#%ld)\n" CLR_RESET,
                           ts, msg.sender_name, msg.msg_id);
                }
                print_prompt_unlocked(cur_name);
                pthread_mutex_unlock(&g_io_mtx);
            } else if (msg.cmd == CMD_ROOM_MSG) {
                msg.name[MAX_NAME - 1] = '\0';
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';
                pthread_mutex_lock(&g_io_mtx);
                if (strcmp(msg.sender_name, "arbiter") == 0) {
                    printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_CYAN "[#%s] * %s\n" CLR_RESET,
                           ts, msg.name, msg.text);
                } else {

                    char body[MAX_TEXT + 64];
                    format_cli_body(msg.text, body, sizeof(body));
                    printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "[#%s] " CLR_CYAN "%s" CLR_RESET ": %s\n",

                           ts, msg.name, msg.sender_name, body);
                }
                print_prompt_unlocked(cur_name);
                pthread_mutex_unlock(&g_io_mtx);

                if (msg.msg_id > 0 && strcmp(msg.sender_name, "arbiter") != 0) {
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
            } else if (msg.cmd == CMD_ROOM_KICK) {
                msg.name[MAX_NAME - 1] = '\0';
                msg.sender_name[MAX_NAME - 1] = '\0';
                pthread_mutex_lock(&g_io_mtx);
                printf("\r\033[K" CLR_DIM "[%s] " CLR_RESET CLR_RED "! You were kicked from #%s by %s\n" CLR_RESET,
                       ts, msg.name, msg.sender_name);
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

        if (strcasecmp(line, "/history") == 0 || strncasecmp(line, "/history ", 9) == 0) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Login first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            const char *filter = (strlen(line) > 9) ? (line + 9) : "";
            while (*filter == ' ') {
                filter++;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_HISTORY;
            req.sender_id = cur_id;
            req.reply_type = 0;
            snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, filter);
            msgsnd(msqid, &req, MSG_SIZE, 0);
            print_prompt();
            continue;
        }

        if (strncasecmp(line, "/delchat ", 9) == 0) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Login first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            const char *target = line + 9;
            while (*target == ' ') {
                target++;
            }
            if (*target == '\0') {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_YELLOW "! Format: /delchat <user|#room>\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            int is_room = (*target == '#');
            if (is_room) {
                target++;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = is_room ? CMD_DELETE_ROOM : CMD_DELETE_CHAT;
            req.sender_id = cur_id;
            req.reply_type = g_reply_type;
            snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, target);
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
            if (rep.target_id > 0) {
                printf(CLR_YELLOW "* Deleted %s %s\n" CLR_RESET, is_room ? "room" : "chat", target);
            } else {
                printf(CLR_RED "! Delete failed: %s\n" CLR_RESET, rep.text);
            }
            pthread_mutex_unlock(&g_io_mtx);
            print_prompt();
            continue;
        }


        if (strncasecmp(line, "/sendfile ", 10) == 0) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            char cur_name[MAX_NAME];
            snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Login first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            const char *p = line + 10;
            while (*p == ' ') {
                p++;
            }
            const char *sp = strchr(p, ' ');
            if (sp == NULL || sp == p || sp[1] == '\0') {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_YELLOW "! Format: /sendfile <user|#room> <filepath>\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            char target[MAX_NAME];
            size_t tlen = (size_t)(sp - p);
            if (tlen >= MAX_NAME) {
                tlen = MAX_NAME - 1;
            }
            memcpy(target, p, tlen);
            target[tlen] = '\0';

            const char *fpath = sp + 1;
            while (*fpath == ' ') {
                fpath++;
            }

            long fsize = 0;
            char err[64] = "";
            int shmid = shm_alloc_from_file(fpath, &fsize, err, sizeof(err));
            if (shmid < 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! File error: %s\n" CLR_RESET, err);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            char fname[64];
            sanitize_filename(fpath, fname, sizeof(fname));

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_FILE_SEND;
            req.sender_id = cur_id;
            req.msg_id = g_next_msg_id++;
            req.reply_type = g_reply_type;
            req.target_id = shmid;
            req.timestamp = fsize;
            snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, target);
            snprintf(req.sender_name, sizeof(req.sender_name), "%.*s", MAX_NAME - 1, cur_name);
            snprintf(req.text, sizeof(req.text), "%.*s", MAX_TEXT - 1, fname);

            if (msgsnd(msqid, &req, MSG_SIZE, 0) < 0) {
                shmctl(shmid, IPC_RMID, NULL);
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
            if (rep.target_id < 0) {
                printf(CLR_RED "! Send file failed: %s\n" CLR_RESET, rep.text);
            } else {
                char sz_str[32];
                format_file_size(fsize, sz_str, sizeof(sz_str));
                printf(CLR_GREEN "* File sent to %s: %s (%s)\n" CLR_RESET, target, fname, sz_str);
            }
            pthread_mutex_unlock(&g_io_mtx);
            print_prompt();
            continue;
        }

        if (strncasecmp(line, "/getfile ", 9) == 0) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Login first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            const char *p = line + 9;
            while (*p == ' ') {
                p++;
            }
            if (*p == '\0') {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_YELLOW "! Format: /getfile <file_id> [dest_path]\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            char file_id[96];
            char dest_path[256];
            const char *sp = strchr(p, ' ');
            if (sp != NULL) {
                size_t flen = (size_t)(sp - p);
                if (flen >= sizeof(file_id)) {
                    flen = sizeof(file_id) - 1;
                }
                memcpy(file_id, p, flen);
                file_id[flen] = '\0';
                const char *dp = sp + 1;
                while (*dp == ' ') {
                    dp++;
                }
                snprintf(dest_path, sizeof(dest_path), "%.*s", (int)sizeof(dest_path) - 1, dp);
            } else {
                snprintf(file_id, sizeof(file_id), "%.*s", (int)sizeof(file_id) - 1, p);
                mkdir(DOWNLOADS_DIR, 0755);
                snprintf(dest_path, sizeof(dest_path), "%s/%s", DOWNLOADS_DIR, file_id);
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_FILE_GET;
            req.sender_id = cur_id;
            req.reply_type = g_reply_type;
            snprintf(req.text, sizeof(req.text), "%.*s", MAX_TEXT - 1, file_id);

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
            if (rep.target_id < 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Download failed: %s\n" CLR_RESET, rep.text);
                pthread_mutex_unlock(&g_io_mtx);
            } else {
                if (shm_save_to_file((int)rep.target_id, rep.timestamp, dest_path) == 0) {
                    char sz_str[32];
                    format_file_size(rep.timestamp, sz_str, sizeof(sz_str));
                    pthread_mutex_lock(&g_io_mtx);
                    printf(CLR_GREEN "* Saved to %s (%s)\n" CLR_RESET, dest_path, sz_str);
                    pthread_mutex_unlock(&g_io_mtx);
                } else {
                    pthread_mutex_lock(&g_io_mtx);
                    printf(CLR_RED "! Failed to write %s\n" CLR_RESET, dest_path);
                    pthread_mutex_unlock(&g_io_mtx);
                }
            }
            print_prompt();
            continue;
        }

        if (strcasecmp(line, "/rooms") == 0) {
            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Login first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_ROOM_LIST;
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
            if (rep.target_id <= 0) {
                printf(CLR_CYAN "* No active rooms\n" CLR_RESET);
            } else {
                printf(CLR_CYAN "* Rooms (%ld, * = joined): %s\n" CLR_RESET, rep.target_id, rep.text);
            }
            pthread_mutex_unlock(&g_io_mtx);
            print_prompt();
            continue;
        }

        if (strncasecmp(line, "/join ", 6) == 0 || strncasecmp(line, "/leave ", 7) == 0) {
            int is_join = (line[1] == 'j' || line[1] == 'J');
            const char *rname = line + (is_join ? 6 : 7);
            while (*rname == ' ') {
                rname++;
            }

            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Login first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = is_join ? CMD_ROOM_JOIN : CMD_ROOM_LEAVE;
            req.sender_id = cur_id;
            req.reply_type = g_reply_type;
            snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 2, rname);
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
            if (rep.target_id > 0) {
                printf(CLR_GREEN "* Room #%s: %s\n" CLR_RESET, rname, rep.text);
            } else {
                printf(CLR_RED "! Room error: %s\n" CLR_RESET, rep.text);
            }
            pthread_mutex_unlock(&g_io_mtx);
            print_prompt();
            continue;
        }

        if (strncasecmp(line, "/r ", 3) == 0) {
            const char *p = line + 3;
            while (*p == ' ') {
                p++;
            }
            const char *sp = strchr(p, ' ');
            if (sp == NULL || sp == p || sp[1] == '\0') {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_YELLOW "! Format: /r <room> <message>\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            char rname[MAX_NAME];
            size_t rlen = (size_t)(sp - p);
            if (rlen >= MAX_NAME - 1) {
                rlen = MAX_NAME - 2;
            }
            memcpy(rname, p, rlen);
            rname[rlen] = '\0';
            const char *text = sp + 1;

            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            long msg_id = g_next_msg_id++;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id <= 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Login first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_ROOM_MSG;
            req.sender_id = cur_id;
            req.msg_id = msg_id;
            req.reply_type = g_reply_type;
            snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 2, rname);
            copy_utf8_safe(req.text, sizeof(req.text), text);

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
            if (rep.target_id >= 0) {
                printf(CLR_DIM "  -> [#%s] sent (online recipients: %ld)\n" CLR_RESET, rname, rep.target_id);
            } else {
                printf(CLR_RED "! Room send failed: %s\n" CLR_RESET, rep.text);
            }
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
                printf(CLR_RED "! Not logged in\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
            }
            print_prompt();
            continue;
        }

        if (line[0] == '#' || line[0] == '@') {
            int is_login = (line[0] == '@');
            const char *p = line + 1;

            pthread_mutex_lock(&g_mtx);
            long cur_id = g_my_id;
            pthread_mutex_unlock(&g_mtx);

            if (cur_id > 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Already logged in, use ## first\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            const char *sp = strchr(p, ' ');
            if (sp == NULL || sp == p || sp[1] == '\0') {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_YELLOW "! Format: #<name> <pass> | @<name> <pass>\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            char name[MAX_NAME];
            size_t nlen = (size_t)(sp - p);
            if (nlen >= MAX_NAME) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Invalid username\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }
            memcpy(name, p, nlen);
            name[nlen] = '\0';

            const char *pass = sp + 1;
            if (strchr(name, '#') != NULL || strchr(name, ',') != NULL || strlen(pass) >= MAX_PASS) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Invalid username or password\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = is_login ? CMD_LOGIN : CMD_REGISTER;
            req.reply_type = g_reply_type;
            snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, name);
            snprintf(req.text, sizeof(req.text), "%.*s", MAX_PASS - 1, pass);

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

            if (rep.target_id > 0) {
                pthread_mutex_lock(&g_mtx);
                g_my_id = rep.target_id;
                g_next_msg_id = 1;
                snprintf(g_my_name, sizeof(g_my_name), "%.*s", MAX_NAME - 1, name);
                pthread_mutex_unlock(&g_mtx);

                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_GREEN "* %s as %s%s (id=%ld)\n" CLR_RESET,
                       is_login ? "Logged in" : "Registered",
                       name,
                       rep.sender_id != 0 ? " [DEBUG]" : "",
                       rep.target_id);
                pthread_mutex_unlock(&g_io_mtx);
            } else {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! %s failed: %s\n" CLR_RESET,
                       is_login ? "Login" : "Registration", rep.text);
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
                printf(CLR_RED "! Login first with @<name> <pass> or #<name> <pass>\n" CLR_RESET);
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
            if (strcasecmp(target_name, "arbiter") == 0) {
                pthread_mutex_lock(&g_io_mtx);
                printf(CLR_RED "! Cannot send message to arbiter\n" CLR_RESET);
                pthread_mutex_unlock(&g_io_mtx);
                print_prompt();
                continue;
            }

            long msg_id = g_next_msg_id++;
            char ts[16];
            get_timestamp(ts, sizeof(ts));

            struct qmsg chat;
            memset(&chat, 0, sizeof(chat));
            chat.mtype = ARBITER_TYPE;
            chat.cmd = CMD_CHAT;
            chat.sender_id = cur_id;
            chat.msg_id = msg_id;
            chat.reply_type = g_reply_type;
            snprintf(chat.name, sizeof(chat.name), "%.*s", MAX_NAME - 1, target_name);
            snprintf(chat.sender_name, sizeof(chat.sender_name), "%.*s", MAX_NAME - 1, cur_name);
            copy_utf8_safe(chat.text, sizeof(chat.text), text);

            if (msgsnd(msqid, &chat, MSG_SIZE, 0) < 0) {
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
            if (rep.target_id < 0) {
                printf(CLR_RED "! Send failed: %s\n" CLR_RESET, rep.text);
            } else {
                if (interactive) {
                    printf("\033[1A\r\033[K");
                }
                if (rep.target_id > 0) {
                    printf(CLR_DIM "[%s] " CLR_RESET CLR_GREEN "you -> %s (#%ld)" CLR_RESET ": %s\n",
                           ts, target_name, msg_id, text);
                } else {
                    printf(CLR_DIM "[%s] " CLR_RESET CLR_YELLOW "you -> %s (#%ld, offline, saved)" CLR_RESET ": %s\n",
                           ts, target_name, msg_id, text);
                }
            }
            pthread_mutex_unlock(&g_io_mtx);
            print_prompt();
            continue;
        }

        pthread_mutex_lock(&g_io_mtx);
        printf(CLR_YELLOW "! Format: #<name> <pass> | @<name> <pass> | <name>#<message> | /history | /delchat | ? | ##\n" CLR_RESET);
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



static void write_pid_to_lock(int lock_fd) {

    if (ftruncate(lock_fd, 0) == 0) {
        lseek(lock_fd, 0, SEEK_SET);
        char buf[32];
        int len = snprintf(buf, sizeof(buf), "%ld\n", (long)getpid());
        if (len > 0) {
            ssize_t w = write(lock_fd, buf, (size_t)len);
            (void)w;
        }
    }
}

static long read_pid_from_lock(int fd) {
    char buf[32];
    lseek(fd, 0, SEEK_SET);
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    if (n <= 0) {
        return -1;
    }
    buf[n] = '\0';
    long pid = strtol(buf, NULL, 10);
    return (pid > 1) ? pid : -1;
}

static int cmd_status(void) {
    int fd = open(ARBITER_LOCK_PATH, O_RDONLY);
    if (fd < 0) {
        printf("Arbiter is not running.\n");
        return 1;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) == 0) {
        flock(fd, LOCK_UN);
        close(fd);
        printf("Arbiter is not running.\n");
        return 1;
    }
    long pid = read_pid_from_lock(fd);
    close(fd);
    if (pid > 0) {
        printf(CLR_GREEN "Arbiter is running (pid=%ld).\n" CLR_RESET, pid);
    } else {
        printf(CLR_GREEN "Arbiter is running.\n" CLR_RESET);
    }
    return 0;
}

static int cmd_stop(void) {
    int fd = open(ARBITER_LOCK_PATH, O_RDONLY);
    if (fd < 0) {
        printf("Arbiter is not running.\n");
        return 1;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) == 0) {
        flock(fd, LOCK_UN);
        close(fd);
        printf("Arbiter is not running.\n");
        return 1;
    }
    long pid = read_pid_from_lock(fd);
    close(fd);
    if (pid <= 0) {
        fprintf(stderr, CLR_RED "! Cannot read arbiter PID.\n" CLR_RESET);
        return 1;
    }
    if (kill((pid_t)pid, SIGTERM) < 0) {
        perror("kill");
        return 1;
    }
    for (int i = 0; i < 40; i++) {
        if (kill((pid_t)pid, 0) < 0 && errno == ESRCH) {
            printf(CLR_YELLOW "Arbiter stopped (pid=%ld).\n" CLR_RESET, pid);
            return 0;
        }
        usleep(50000);
    }
    fprintf(stderr, CLR_YELLOW "! Sent SIGTERM to pid=%ld, still shutting down.\n" CLR_RESET, pid);
    return 0;
}

static int daemonize_and_run(key_t key) {
    int pfd[2];
    if (pipe(pfd) < 0) {
        perror("pipe");
        return 1;
    }

    pid_t p1 = fork();
    if (p1 < 0) {
        perror("fork");
        close(pfd[0]);
        close(pfd[1]);
        return 1;
    }

    if (p1 > 0) {
        close(pfd[1]);
        long dpid = -1;
        ssize_t r = read(pfd[0], &dpid, sizeof(dpid));
        close(pfd[0]);
        waitpid(p1, NULL, 0);
        if (r == (ssize_t)sizeof(dpid) && dpid > 0) {
            printf(CLR_GREEN "DeepSpeak daemon started (pid=%ld, log=%s)\n" CLR_RESET,
                   dpid, ARBITER_LOG_PATH);
            return 0;
        }
        fprintf(stderr, CLR_RED "! Failed to start daemon (already running or IPC error).\n" CLR_RESET);
        return 1;
    }

    close(pfd[0]);
    if (setsid() < 0) {
        long fail = -1;
        ssize_t w = write(pfd[1], &fail, sizeof(fail));
        (void)w;
        close(pfd[1]);
        _exit(1);
    }

    signal(SIGHUP, SIG_IGN);

    pid_t p2 = fork();
    if (p2 < 0) {
        long fail = -1;
        ssize_t w = write(pfd[1], &fail, sizeof(fail));
        (void)w;
        close(pfd[1]);
        _exit(1);
    }
    if (p2 > 0) {
        close(pfd[1]);
        _exit(0);
    }

    umask(0027);

    int lock_fd = open(ARBITER_LOCK_PATH, O_CREAT | O_RDWR, 0644);
    if (lock_fd < 0 || flock(lock_fd, LOCK_EX | LOCK_NB) < 0) {
        if (lock_fd >= 0) {
            close(lock_fd);
        }
        long fail = -1;
        ssize_t w = write(pfd[1], &fail, sizeof(fail));
        (void)w;
        close(pfd[1]);
        _exit(1);
    }
    write_pid_to_lock(lock_fd);

    int msqid = msgget(key, 0666 | IPC_CREAT);
    if (msqid < 0) {
        flock(lock_fd, LOCK_UN);
        close(lock_fd);
        long fail = -1;
        ssize_t w = write(pfd[1], &fail, sizeof(fail));
        (void)w;
        close(pfd[1]);
        _exit(1);
    }

    int devnull = open("/dev/null", O_RDONLY);
    if (devnull >= 0) {
        dup2(devnull, STDIN_FILENO);
        if (devnull > STDERR_FILENO) {
            close(devnull);
        }
    }

    int log_fd = open(ARBITER_LOG_PATH, O_CREAT | O_WRONLY | O_APPEND, 0640);
    if (log_fd >= 0) {
        dup2(log_fd, STDOUT_FILENO);
        dup2(log_fd, STDERR_FILENO);
        if (log_fd > STDERR_FILENO) {
            close(log_fd);
        }
    }

    long dpid = (long)getpid();
    ssize_t w = write(pfd[1], &dpid, sizeof(dpid));
    (void)w;
    close(pfd[1]);

    int rc = run_arbiter(msqid);

    flock(lock_fd, LOCK_UN);
    close(lock_fd);
    unlink(ARBITER_LOCK_PATH);
    _exit(rc);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <arbiter|daemon|stop|status|client>\n", argv[0]);
        return 1;
    }

    if (strcasecmp(argv[1], "status") == 0) {
        return cmd_status();
    }
    if (strcasecmp(argv[1], "stop") == 0) {
        return cmd_stop();
    }

    int is_arbiter = (strcasecmp(argv[1], "arbiter") == 0);
    int is_daemon = (strcasecmp(argv[1], "daemon") == 0);
    int is_client = (strcasecmp(argv[1], "client") == 0);
    if (!is_arbiter && !is_daemon && !is_client) {
        fprintf(stderr, "Usage: %s <arbiter|daemon|stop|status|client>\n", argv[0]);
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

    if (is_daemon) {
        return daemonize_and_run(key);
    }

    int lock_fd = -1;
    if (is_arbiter) {
        lock_fd = open(ARBITER_LOCK_PATH, O_CREAT | O_RDWR, 0644);
        if (lock_fd < 0) {
            perror("open lock file");
            return 1;
        }
        if (flock(lock_fd, LOCK_EX | LOCK_NB) < 0) {
            if (errno == EWOULDBLOCK) {
                fprintf(stderr, CLR_RED "! Arbiter is already running.\n" CLR_RESET);
            } else {
                perror("flock");
            }
            close(lock_fd);
            return 1;
        }
        write_pid_to_lock(lock_fd);
    }

    int flags = is_arbiter ? (0666 | IPC_CREAT) : 0666;
    int msqid = msgget(key, flags);
    if (msqid < 0) {
        if (!is_arbiter && errno == ENOENT) {
            fprintf(stderr, "Arbiter is not running. Start '%s arbiter' or '%s daemon' first.\n", argv[0], argv[0]);
        } else {
            perror("msgget");
        }
        if (lock_fd >= 0) {
            flock(lock_fd, LOCK_UN);
            close(lock_fd);
        }
        return 1;
    }

    int rc = is_arbiter ? run_arbiter(msqid) : run_client(msqid);

    if (lock_fd >= 0) {
        flock(lock_fd, LOCK_UN);
        close(lock_fd);
        unlink(ARBITER_LOCK_PATH);
    }

    return rc;
}
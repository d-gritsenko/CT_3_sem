#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <ctype.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <time.h>

#define QUEUE_KEY_PATH "/tmp"
#define ARBITER_LOCK_PATH "/tmp/queue_arbiter.lock"
#define QUEUE_KEY_PROJ 'Q'

#define USERS_DB_PATH "users.db"
#define DEBUG_PASS_DEFAULT "debug123"
#define UPLOADS_DIR "uploads"
#define DOWNLOADS_DIR "downloads"
#define MAX_FILE_SIZE (8 * 1024 * 1024)

#define MAX_NAME 32
#define MAX_PASS 64
#define MAX_TEXT 255
#define MAX_CLIENTS 64
#define MAX_PENDING 128
#define MAX_ROOMS 16

#define ARBITER_TYPE 1L
#define REPLY_BASE 100000L

#define HEARTBEAT_TIMEOUT 10
#define HEARTBEAT_INTERVAL 4
#define OFFLINE_TTL 60

enum msg_cmd {
    CMD_REGISTER = 1,
    CMD_HEARTBEAT = 2,
    CMD_LOOKUP = 3,
    CMD_LOGOUT = 4,
    CMD_CHAT = 5,
    CMD_REPLY = 6,
    CMD_STORE = 7,
    CMD_ACK = 8,
    CMD_WHO = 9,
    CMD_RECONNECT = 10,
    CMD_ROOM_JOIN = 11,
    CMD_ROOM_LEAVE = 12,
    CMD_ROOM_MSG = 13,
    CMD_ROOM_LIST = 14,
    CMD_LOGIN = 15,
    CMD_SET_THEME = 16,
    CMD_HISTORY = 17,
    CMD_HISTORY_ITEM = 18,
    CMD_DELETE_CHAT = 19,
    CMD_DELETE_ROOM = 20,
    CMD_MARK_READ = 21,
    CMD_FILE_SEND = 22,
    CMD_FILE_GET = 23,
    CMD_TYPING = 24,
    CMD_MSG_EDIT = 25,
    CMD_MSG_DELETE = 26
};

struct qmsg {
    long mtype;
    int cmd;
    long sender_id;
    long reply_type;
    long target_id;
    long msg_id;
    long timestamp;
    char name[MAX_NAME];
    char sender_name[MAX_NAME];
    char text[MAX_TEXT];
};

#define MSG_SIZE (sizeof(struct qmsg) - sizeof(long))

static inline void copy_utf8_safe(char *dst, size_t dst_size, const char *src) {
    if (dst == NULL || dst_size == 0) {
        return;
    }
    if (src == NULL) {
        dst[0] = '\0';
        return;
    }
    size_t len = 0;
    while (src[len] != '\0' && len < dst_size - 1) {
        len++;
    }
    if (src[len] != '\0' && len > 0) {
        if (((unsigned char)src[len] & 0xC0) == 0x80) {
            while (len > 0 && ((unsigned char)src[len - 1] & 0xC0) == 0x80) {
                len--;
            }
            if (len > 0) {
                len--;
            }
        }
    }
    memcpy(dst, src, len);
    dst[len] = '\0';
}

static inline void get_timestamp(char *buf, size_t size) {
    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    strftime(buf, size, "%H:%M:%S", &tm_now);
}

static inline void format_timestamp_from(time_t ts, char *buf, size_t size) {
    time_t now = time(NULL);
    if (ts <= 0) {
        ts = now;
    }
    struct tm tm_ts;
    struct tm tm_now;
    localtime_r(&ts, &tm_ts);
    localtime_r(&now, &tm_now);
    if (tm_ts.tm_year == tm_now.tm_year && tm_ts.tm_yday == tm_now.tm_yday) {
        strftime(buf, size, "%H:%M:%S", &tm_ts);
    } else {
        strftime(buf, size, "%d.%m %H:%M", &tm_ts);
    }
}

static inline void drain_type(int msqid, long mtype) {
    struct qmsg tmp;
    while (msgrcv(msqid, &tmp, MSG_SIZE, mtype, IPC_NOWAIT) >= 0) {
    }
}

static inline int is_valid_ident(const char *s, size_t max_len) {
    if (s == NULL || s[0] == '\0' || strlen(s) >= max_len) {
        return 0;
    }
    if (strchr(s, '#') != NULL || strchr(s, ',') != NULL || strchr(s, ' ') != NULL) {
        return 0;
    }
    return 1;
}

static inline void format_file_size(long bytes, char *out, size_t out_size) {
    if (bytes < 1024) {
        snprintf(out, out_size, "%ld B", bytes);
    } else if (bytes < 1024 * 1024) {
        snprintf(out, out_size, "%.1f KB", (double)bytes / 1024.0);
    } else {
        snprintf(out, out_size, "%.2f MB", (double)bytes / (1024.0 * 1024.0));
    }
}

static inline void sanitize_filename(const char *path, char *out, size_t out_size) {
    if (out == NULL || out_size == 0) {
        return;
    }
    if (path == NULL || path[0] == '\0') {
        snprintf(out, out_size, "file");
        return;
    }
    const char *base = strrchr(path, '/');
    base = (base != NULL) ? (base + 1) : path;
    if (base[0] == '\0') {
        base = "file";
    }
    size_t pos = 0;
    for (size_t i = 0; base[i] != '\0' && pos + 1 < out_size; i++) {
        unsigned char c = (unsigned char)base[i];
        if (c < 32 || c == '|' || c == '[' || c == ']' || c == '/' || c == '\\') {
            out[pos++] = '_';
        } else {
            out[pos++] = (char)c;
        }
    }
    out[pos] = '\0';
    if (out[0] == '\0' || strcmp(out, ".") == 0 || strcmp(out, "..") == 0) {
        snprintf(out, out_size, "file");
    }
}

static inline void make_safe_storage_suffix(const char *orig, char *out, size_t out_size) {
    if (out == NULL || out_size == 0) {
        return;
    }
    size_t pos = 0;
    for (size_t i = 0; orig != NULL && orig[i] != '\0' && pos + 1 < out_size; i++) {
        unsigned char c = (unsigned char)orig[i];
        if (isalnum(c) || c == '.' || c == '-' || c == '_') {
            out[pos++] = (char)c;
        } else {
            out[pos++] = '_';
        }
    }
    out[pos] = '\0';
    if (out[0] == '\0' || out[0] == '.') {
        snprintf(out, out_size, "file.bin");
    }
}

static inline int is_safe_stored_name(const char *s) {
    if (s == NULL || s[0] == '\0' || strlen(s) >= 96) {
        return 0;
    }
    if (strstr(s, "..") != NULL) {
        return 0;
    }
    for (size_t i = 0; s[i] != '\0'; i++) {
        unsigned char c = (unsigned char)s[i];
        if (!(isalnum(c) || c == '.' || c == '-' || c == '_')) {
            return 0;
        }
    }
    return 1;
}

static inline int parse_file_marker(const char *text, char *stored_out, size_t stored_sz,
                                    long *size_out, char *orig_out, size_t orig_sz) {
    if (text == NULL || strncmp(text, "[[FILE:", 7) != 0) {
        return 0;
    }
    size_t tlen = strlen(text);
    if (tlen < 11 || strcmp(text + tlen - 2, "]]") != 0) {
        return 0;
    }
    const char *p1 = text + 7;
    const char *bar1 = strchr(p1, '|');
    if (bar1 == NULL || bar1 == p1) {
        return 0;
    }
    const char *p2 = bar1 + 1;
    const char *bar2 = strchr(p2, '|');
    if (bar2 == NULL || bar2 == p2) {
        return 0;
    }
    const char *p3 = bar2 + 1;
    const char *end = text + tlen - 2;
    if (p3 >= end) {
        return 0;
    }

    size_t slen = (size_t)(bar1 - p1);
    if (slen >= stored_sz) {
        slen = stored_sz - 1;
    }
    memcpy(stored_out, p1, slen);
    stored_out[slen] = '\0';

    if (size_out != NULL) {
        *size_out = strtol(p2, NULL, 10);
    }

    size_t olen = (size_t)(end - p3);
    if (olen >= orig_sz) {
        olen = orig_sz - 1;
    }
    memcpy(orig_out, p3, olen);
    orig_out[olen] = '\0';
    return 1;
}

static inline int shm_alloc_from_file(const char *filepath, long *out_size, char *err_buf, size_t err_size) {
    if (filepath == NULL || filepath[0] == '\0') {
        snprintf(err_buf, err_size, "empty path");
        return -1;
    }
    struct stat st;
    if (stat(filepath, &st) != 0 || !S_ISREG(st.st_mode)) {
        snprintf(err_buf, err_size, "file not found");
        return -1;
    }
    if (st.st_size <= 0) {
        snprintf(err_buf, err_size, "file is empty");
        return -1;
    }
    if (st.st_size > MAX_FILE_SIZE) {
        snprintf(err_buf, err_size, "file too large (max %d MB)", MAX_FILE_SIZE / (1024 * 1024));
        return -1;
    }

    FILE *fp = fopen(filepath, "rb");
    if (fp == NULL) {
        snprintf(err_buf, err_size, "cannot open file");
        return -1;
    }

    int shmid = shmget(IPC_PRIVATE, (size_t)st.st_size, IPC_CREAT | 0666);
    if (shmid < 0) {
        fclose(fp);
        snprintf(err_buf, err_size, "shmget failed");
        return -1;
    }

    void *ptr = shmat(shmid, NULL, 0);
    if (ptr == (void *)-1) {
        shmctl(shmid, IPC_RMID, NULL);
        fclose(fp);
        snprintf(err_buf, err_size, "shmat failed");
        return -1;
    }

    size_t nread = fread(ptr, 1, (size_t)st.st_size, fp);
    fclose(fp);
    shmdt(ptr);

    if (nread != (size_t)st.st_size) {
        shmctl(shmid, IPC_RMID, NULL);
        snprintf(err_buf, err_size, "read error");
        return -1;
    }

    if (out_size != NULL) {
        *out_size = (long)st.st_size;
    }
    return shmid;
}

static inline int shm_save_to_file(int shmid, long fsize, const char *dest_path) {
    if (shmid < 0 || fsize <= 0 || fsize > MAX_FILE_SIZE || dest_path == NULL) {
        if (shmid >= 0) {
            shmctl(shmid, IPC_RMID, NULL);
        }
        return -1;
    }

    void *ptr = shmat(shmid, NULL, SHM_RDONLY);
    if (ptr == (void *)-1) {
        shmctl(shmid, IPC_RMID, NULL);
        return -1;
    }

    FILE *fp = fopen(dest_path, "wb");
    int ok = 0;
    if (fp != NULL) {
        ok = (fwrite(ptr, 1, (size_t)fsize, fp) == (size_t)fsize);
        if (fclose(fp) != 0) {
            ok = 0;
        }
    }

    shmdt(ptr);
    shmctl(shmid, IPC_RMID, NULL);
    return ok ? 0 : -1;
}

static inline int parse_reply_prefix(const char *text, char *author_out, size_t author_sz,
                                     char *snippet_out, size_t snippet_sz, const char **body_out) {
    if (text == NULL || strncmp(text, "[[REPLY:", 8) != 0) {
        return 0;
    }
    const char *p1 = text + 8;
    const char *bar = strchr(p1, '|');
    if (bar == NULL || bar == p1) {
        return 0;
    }
    const char *p2 = bar + 1;
    const char *end = strstr(p2, "]]");
    if (end == NULL || end < p2) {
        return 0;
    }
    if (author_out != NULL && author_sz > 0) {
        size_t alen = (size_t)(bar - p1);
        if (alen >= author_sz) {
            alen = author_sz - 1;
        }
        memcpy(author_out, p1, alen);
        author_out[alen] = '\0';
    }
    if (snippet_out != NULL && snippet_sz > 0) {
        size_t slen = (size_t)(end - p2);
        if (slen >= snippet_sz) {
            slen = snippet_sz - 1;
        }
        memcpy(snippet_out, p2, slen);
        snippet_out[slen] = '\0';
    }
    if (body_out != NULL) {
        *body_out = end + 2;
    }
    return 1;
}

#endif
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stddef.h>
#include <string.h>

#define USERS_DB_PATH "users.db"
#define DEBUG_PASS_DEFAULT "debug123"

#define QUEUE_KEY_PATH "/tmp"
#define ARBITER_LOCK_PATH "/tmp/queue_arbiter.lock"
#define QUEUE_KEY_PROJ 'Q'

#define MAX_PASS 64
#define MAX_NAME 32
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
   CMD_SET_THEME = 16
};

struct qmsg {
   long mtype;
   int cmd;
   long sender_id;
   long reply_type;
   long target_id;
   long msg_id;
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

#endif
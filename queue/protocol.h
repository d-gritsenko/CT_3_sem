#ifndef PROTOCOL_H
#define PROTOCOL_H

#define QUEUE_KEY_PATH "/tmp"
#define QUEUE_KEY_PROJ 'Q'

#define MAX_NAME 32
#define MAX_TEXT 255
#define MAX_CLIENTS 64
#define MAX_PENDING 128
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
    CMD_RECONNECT = 10
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

#endif
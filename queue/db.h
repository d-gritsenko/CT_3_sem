#ifndef DB_H
#define DB_H

#include <stddef.h>
#include <time.h>

#include "protocol.h"

struct db_msg_record {

    long id;
    int is_room;

    int is_unread;

    int is_edited;
    char peer[MAX_NAME];
    char sender[MAX_NAME];
    char text[MAX_TEXT];
    long timestamp;
};

int db_init(const char *path);
void db_close(void);

int db_is_debug_name(const char *username);
int db_check_debug_pass(const char *password);

int db_register_user(const char *username, const char *password, char *err_buf, size_t err_size);
int db_verify_user(const char *username, const char *password, int *out_theme_id, char *err_buf, size_t err_size);
int db_set_theme(const char *username, int theme_id);


long db_save_dm(const char *sender, const char *receiver, const char *text, time_t ts);
int db_load_dm_history(const char *username, const char *peer_filter,
                       struct db_msg_record *out, int max_count);
int db_delete_dm_for_user(const char *username, const char *peer);

int db_mark_dm_read(const char *username, const char *peer);

int db_room_add_member(const char *room, const char *username);
int db_room_remove_member(const char *room, const char *username);
int db_load_user_rooms(const char *username, char rooms_out[][MAX_NAME], int max_rooms);

long db_save_room_msg(const char *room, const char *sender, const char *text, time_t ts);
int db_load_room_history(const char *room, const char *username,
                         struct db_msg_record *out, int max_count);
int db_delete_room_for_user(const char *username, const char *room, int remaining_active_members);

int db_mark_room_read(const char *username, const char *room);


int db_edit_message(int is_room, long msg_id, const char *sender, const char *new_text);
int db_delete_message(int is_room, long msg_id, const char *sender);

#endif
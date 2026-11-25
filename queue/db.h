#ifndef DB_H
#define DB_H

#include <stddef.h>

int db_init(const char *path);
void db_close(void);

int db_is_debug_name(const char *username);
int db_check_debug_pass(const char *password);

int db_register_user(const char *username, const char *password, char *err_buf, size_t err_size);
int db_verify_user(const char *username, const char *password, int *out_theme_id, char *err_buf, size_t err_size);
int db_set_theme(const char *username, int theme_id);

#endif

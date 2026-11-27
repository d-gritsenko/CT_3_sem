#define _DEFAULT_SOURCE

#include "db.h"
#include "protocol.h"

#include <fcntl.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

static sqlite3 *g_db = NULL;

static uint64_t rotl64(uint64_t x, int r) {
    return (x << r) | (x >> (64 - r));
}

static uint64_t mix64(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

static void generate_salt_hex(char out_hex[33]) {
    unsigned char raw[16];
    int fd = open("/dev/urandom", O_RDONLY);
    ssize_t got = 0;
    if (fd >= 0) {
        got = read(fd, raw, sizeof(raw));
        close(fd);
    }
    if (got != (ssize_t)sizeof(raw)) {
        uint64_t seed = (uint64_t)time(NULL) ^ ((uint64_t)getpid() << 16);
        for (size_t i = 0; i < sizeof(raw); i++) {
            seed = mix64(seed + i);
            raw[i] = (unsigned char)(seed & 0xffU);
        }
    }
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < sizeof(raw); i++) {
        out_hex[i * 2] = hex[(raw[i] >> 4) & 0x0f];
        out_hex[i * 2 + 1] = hex[raw[i] & 0x0f];
    }
    out_hex[32] = '\0';
}

static void hash_password(const char *salt, const char *password, char out_hex[65]) {
    uint64_t s[4] = {
        0x243f6a8885a308d3ULL,
        0x13198a2e03707344ULL,
        0xa4093822299f31d0ULL,
        0x082efa98ec4e6c89ULL
    };

    size_t slen = strlen(salt);
    size_t plen = strlen(password);

    for (size_t i = 0; i < slen; i++) {
        s[i & 3] = mix64(s[i & 3] ^ ((uint64_t)(unsigned char)salt[i] + i * 131ULL));
    }
    for (size_t i = 0; i < plen; i++) {
        s[(i + 1) & 3] = mix64(s[(i + 1) & 3] ^ ((uint64_t)(unsigned char)password[i] + i * 257ULL));
    }

    for (int round = 0; round < 4096; round++) {
        uint64_t r = (uint64_t)round;
        s[0] = mix64(s[0] ^ rotl64(s[1], 17) ^ r);
        s[1] = mix64(s[1] ^ rotl64(s[2], 29) ^ (r * 3ULL));
        s[2] = mix64(s[2] ^ rotl64(s[3], 41) ^ (r * 7ULL));
        s[3] = mix64(s[3] ^ rotl64(s[0], 53) ^ (r * 11ULL));
    }

    snprintf(out_hex, 65, "%016llx%016llx%016llx%016llx",
             (unsigned long long)s[0], (unsigned long long)s[1],
             (unsigned long long)s[2], (unsigned long long)s[3]);
}

static int const_time_eq(const char *a, const char *b) {
    size_t la = strlen(a);
    size_t lb = strlen(b);
    if (la != lb) {
        return 0;
    }
    unsigned char diff = 0;
    for (size_t i = 0; i < la; i++) {
        diff |= (unsigned char)(a[i] ^ b[i]);
    }
    return diff == 0;
}

int db_is_debug_name(const char *username) {
    if (username == NULL) {
        return 0;
    }
    return strncasecmp(username, "user", 4) == 0;
}

int db_check_debug_pass(const char *password) {
    if (password == NULL) {
        return 0;
    }
    const char *expected = getenv("QUEUE_DEBUG_PASS");
    if (expected == NULL || expected[0] == '\0') {
        expected = DEBUG_PASS_DEFAULT;
    }
    return const_time_eq(password, expected);
}

static void seed_default_users_if_empty(void) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM users;", -1, &stmt, NULL) != SQLITE_OK) {
        return;
    }
    int count = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);

    if (count == 0) {
        char err[64];
        db_register_user("dmitry", "123456", err, sizeof(err));
        db_register_user("alice", "alice123", err, sizeof(err));
        db_register_user("bob", "bob123", err, sizeof(err));
    }
}

int db_init(const char *path) {
    if (sqlite3_open(path, &g_db) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_open(%s) failed: %s\n",
                path, g_db != NULL ? sqlite3_errmsg(g_db) : "out of memory");
        if (g_db != NULL) {
            sqlite3_close(g_db);
            g_db = NULL;
        }
        return -1;
    }

    const char *sql =
        "CREATE TABLE IF NOT EXISTS users ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  username TEXT UNIQUE NOT NULL,"
        "  salt TEXT NOT NULL,"
        "  pass_hash TEXT NOT NULL,"

        "  theme_id INTEGER NOT NULL DEFAULT 0,"
        "  pubkey TEXT NOT NULL DEFAULT '',"
        "  created_at INTEGER NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS messages ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  sender TEXT NOT NULL,"
        "  receiver TEXT NOT NULL,"
        "  text TEXT NOT NULL,"
        "  created_at INTEGER NOT NULL,"
        "  deleted_by_sender INTEGER NOT NULL DEFAULT 0,"

        "  deleted_by_receiver INTEGER NOT NULL DEFAULT 0,"


        "  is_read INTEGER NOT NULL DEFAULT 0,"

        "  is_edited INTEGER NOT NULL DEFAULT 0"

        ");"
        "CREATE INDEX IF NOT EXISTS idx_messages_pair ON messages(sender, receiver);"
        "CREATE TABLE IF NOT EXISTS room_messages ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  room TEXT NOT NULL,"
        "  sender TEXT NOT NULL,"
        "  text TEXT NOT NULL,"

        "  created_at INTEGER NOT NULL,"

        "  is_edited INTEGER NOT NULL DEFAULT 0"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_room_messages_room ON room_messages(room);"
        "CREATE TABLE IF NOT EXISTS room_members ("
        "  room TEXT NOT NULL,"
        "  username TEXT NOT NULL,"
        "  PRIMARY KEY(room, username)"
        ");"
        "CREATE TABLE IF NOT EXISTS room_cleared ("
        "  room TEXT NOT NULL,"
        "  username TEXT NOT NULL,"
        "  cleared_at INTEGER NOT NULL,"
        "  PRIMARY KEY(room, username)"

        ");"

        "CREATE TABLE IF NOT EXISTS room_read ("
        "  room TEXT NOT NULL,"
        "  username TEXT NOT NULL,"
        "  last_read_id INTEGER NOT NULL DEFAULT 0,"
        "  PRIMARY KEY(room, username)"
        ");"
        "CREATE TABLE IF NOT EXISTS room_meta ("
        "  room TEXT PRIMARY KEY,"
        "  owner TEXT NOT NULL DEFAULT '',"
        "  topic TEXT NOT NULL DEFAULT '',"
        "  pinned_text TEXT NOT NULL DEFAULT ''"
        ");";

    char *errmsg = NULL;
    if (sqlite3_exec(g_db, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec failed: %s\n", errmsg != NULL ? errmsg : "unknown");
        sqlite3_free(errmsg);
        sqlite3_close(g_db);
        g_db = NULL;
        return -1;
    }


    sqlite3_exec(g_db, "ALTER TABLE messages ADD COLUMN is_read INTEGER NOT NULL DEFAULT 0;", NULL, NULL, NULL);

    sqlite3_exec(g_db, "ALTER TABLE messages ADD COLUMN is_edited INTEGER NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(g_db, "ALTER TABLE room_messages ADD COLUMN is_edited INTEGER NOT NULL DEFAULT 0;", NULL, NULL, NULL);

    sqlite3_exec(g_db, "ALTER TABLE users ADD COLUMN pubkey TEXT NOT NULL DEFAULT '';", NULL, NULL, NULL);

    seed_default_users_if_empty();
    return 0;
}

void db_close(void) {
    if (g_db != NULL) {
        sqlite3_close(g_db);
        g_db = NULL;
    }
}

int db_register_user(const char *username, const char *password, char *err_buf, size_t err_size) {
    if (g_db == NULL) {
        snprintf(err_buf, err_size, "database unavailable");
        return -1;
    }
    if (username == NULL || username[0] == '\0' || strlen(username) >= MAX_NAME) {
        snprintf(err_buf, err_size, "invalid username");
        return -1;
    }
    if (strcasecmp(username, "arbiter") == 0) {
        snprintf(err_buf, err_size, "reserved name");
        return -1;
    }
    if (db_is_debug_name(username)) {
        snprintf(err_buf, err_size, "prefix 'user' is reserved for debug");
        return -1;
    }
    if (password == NULL || strlen(password) < 4 || strlen(password) >= MAX_PASS) {
        snprintf(err_buf, err_size, "password must be 4..63 chars");
        return -1;
    }

    char salt[33];
    char hash[65];
    generate_salt_hex(salt);
    hash_password(salt, password, hash);

    const char *sql =
        "INSERT INTO users (username, salt, pass_hash, theme_id, created_at) "
        "VALUES (?1, ?2, ?3, 0, ?4);";

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        snprintf(err_buf, err_size, "db error");
        return -1;
    }

    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, salt, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, hash, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 4, (sqlite3_int64)time(NULL));

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc == SQLITE_CONSTRAINT || rc == SQLITE_CONSTRAINT_UNIQUE) {
        snprintf(err_buf, err_size, "username already registered");
        return -1;
    }
    if (rc != SQLITE_DONE) {
        snprintf(err_buf, err_size, "registration failed");
        return -1;
    }

    return 0;
}

int db_verify_user(const char *username, const char *password, int *out_theme_id, char *err_buf, size_t err_size) {
    if (out_theme_id != NULL) {
        *out_theme_id = 0;
    }
    if (g_db == NULL) {
        snprintf(err_buf, err_size, "database unavailable");
        return -1;
    }
    if (username == NULL || username[0] == '\0' || password == NULL || password[0] == '\0') {
        snprintf(err_buf, err_size, "enter username and password");
        return -1;
    }

    const char *sql = "SELECT salt, pass_hash, theme_id FROM users WHERE username = ?1;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        snprintf(err_buf, err_size, "db error");
        return -1;
    }

    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        snprintf(err_buf, err_size, "user not found");
        return -1;
    }

    const char *salt = (const char *)sqlite3_column_text(stmt, 0);
    const char *stored_hash = (const char *)sqlite3_column_text(stmt, 1);
    int theme_id = sqlite3_column_int(stmt, 2);

    char calc_hash[65];
    hash_password(salt != NULL ? salt : "", password, calc_hash);
    int ok = (stored_hash != NULL) && const_time_eq(calc_hash, stored_hash);
    sqlite3_finalize(stmt);

    if (!ok) {
        snprintf(err_buf, err_size, "wrong password");
        return -1;
    }

    if (out_theme_id != NULL) {
        *out_theme_id = theme_id;
    }
    return 0;
}

int db_set_theme(const char *username, int theme_id) {
    if (g_db == NULL || username == NULL || db_is_debug_name(username)) {
        return 0;
    }

    const char *sql = "UPDATE users SET theme_id = ?1 WHERE username = ?2;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }

    sqlite3_bind_int(stmt, 1, theme_id);
    sqlite3_bind_text(stmt, 2, username, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? 0 : -1;
}


int db_set_pubkey(const char *username, const char *pubkey_hex) {
    if (g_db == NULL || username == NULL || pubkey_hex == NULL || db_is_debug_name(username)) {
        return 0;
    }

    const char *sql = "UPDATE users SET pubkey = ?1 WHERE username = ?2;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }

    sqlite3_bind_text(stmt, 1, pubkey_hex, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, username, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int db_get_pubkey(const char *username, char *out_hex, size_t out_size) {
    if (out_hex != NULL && out_size > 0) {
        out_hex[0] = '\0';
    }
    if (g_db == NULL || username == NULL || out_hex == NULL || out_size == 0 || db_is_debug_name(username)) {
        return -1;
    }

    const char *sql = "SELECT pubkey FROM users WHERE username = ?1;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }

    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_TRANSIENT);
    int found = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *pk = (const char *)sqlite3_column_text(stmt, 0);
        if (pk != NULL && pk[0] != '\0') {
            snprintf(out_hex, out_size, "%s", pk);
            found = 1;
        }
    }
    sqlite3_finalize(stmt);
    return found ? 0 : -1;
}


long db_save_dm(const char *sender, const char *receiver, const char *text, time_t ts) {
    if (g_db == NULL || sender == NULL || receiver == NULL || text == NULL) {
        return 0;
    }
    if (db_is_debug_name(sender) || db_is_debug_name(receiver)) {
        return 0;
    }

    const char *sql =
        "INSERT INTO messages (sender, receiver, text, created_at) "
        "VALUES (?1, ?2, ?3, ?4);";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }

    sqlite3_bind_text(stmt, 1, sender, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, receiver, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, text, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 4, (sqlite3_int64)ts);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? (long)sqlite3_last_insert_rowid(g_db) : -1;
}

int db_load_dm_history(const char *username, const char *peer_filter,
                       struct db_msg_record *out, int max_count) {
    if (g_db == NULL || username == NULL || out == NULL || max_count <= 0) {
        return 0;
    }
    if (db_is_debug_name(username)) {
        return 0;
    }

    const char *sql_all =


        "SELECT id, sender, receiver, text, created_at, is_read, is_edited FROM ("


        "  SELECT id, sender, receiver, text, created_at, is_read, is_edited FROM messages "
        "  WHERE (sender = ?1 AND deleted_by_sender = 0) "
        "     OR (receiver = ?1 AND deleted_by_receiver = 0) "
        "  ORDER BY id DESC LIMIT ?2"
        ") ORDER BY id ASC;";

    const char *sql_peer =


        "SELECT id, sender, receiver, text, created_at, is_read, is_edited FROM ("


        "  SELECT id, sender, receiver, text, created_at, is_read, is_edited FROM messages "
        "  WHERE (sender = ?1 AND receiver = ?2 AND deleted_by_sender = 0) "
        "     OR (receiver = ?1 AND sender = ?2 AND deleted_by_receiver = 0) "
        "  ORDER BY id DESC LIMIT ?3"
        ") ORDER BY id ASC;";

    int has_peer = (peer_filter != NULL && peer_filter[0] != '\0');
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, has_peer ? sql_peer : sql_all, -1, &stmt, NULL) != SQLITE_OK) {
        return 0;
    }

    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_TRANSIENT);
    if (has_peer) {
        sqlite3_bind_text(stmt, 2, peer_filter, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 3, max_count);
    } else {
        sqlite3_bind_int(stmt, 2, max_count);
    }

    int count = 0;
    while (count < max_count && sqlite3_step(stmt) == SQLITE_ROW) {

        sqlite3_int64 msg_id = sqlite3_column_int64(stmt, 0);

        const char *sender = (const char *)sqlite3_column_text(stmt, 1);

        const char *receiver = (const char *)sqlite3_column_text(stmt, 2);

        const char *text = (const char *)sqlite3_column_text(stmt, 3);

        sqlite3_int64 ts = sqlite3_column_int64(stmt, 4);

        int is_read = sqlite3_column_int(stmt, 5);

        int is_edited = sqlite3_column_int(stmt, 6);

        if (sender == NULL || receiver == NULL || text == NULL) {
            continue;
        }

        const char *peer = (strcmp(sender, username) == 0) ? receiver : sender;
        memset(&out[count], 0, sizeof(out[count]));

        out[count].id = (long)msg_id;
        out[count].is_room = 0;

        out[count].is_unread = (strcmp(receiver, username) == 0 && is_read == 0) ? 1 : 0;

        out[count].is_edited = (is_edited != 0) ? 1 : 0;
        snprintf(out[count].peer, sizeof(out[count].peer), "%.*s", MAX_NAME - 1, peer);
        snprintf(out[count].sender, sizeof(out[count].sender), "%.*s", MAX_NAME - 1, sender);
        copy_utf8_safe(out[count].text, sizeof(out[count].text), text);
        out[count].timestamp = (long)ts;
        count++;
    }

    sqlite3_finalize(stmt);
    return count;
}

int db_delete_dm_for_user(const char *username, const char *peer) {
    if (g_db == NULL || username == NULL || peer == NULL || peer[0] == '\0') {
        return 0;
    }
    if (db_is_debug_name(username)) {
        return 0;
    }

    sqlite3_stmt *stmt = NULL;

    const char *sql1 = "UPDATE messages SET deleted_by_sender = 1 WHERE sender = ?1 AND receiver = ?2;";
    if (sqlite3_prepare_v2(g_db, sql1, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, username, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, peer, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    const char *sql2 = "UPDATE messages SET deleted_by_receiver = 1 WHERE receiver = ?1 AND sender = ?2;";
    if (sqlite3_prepare_v2(g_db, sql2, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, username, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, peer, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    sqlite3_exec(g_db, "DELETE FROM messages WHERE deleted_by_sender = 1 AND deleted_by_receiver = 1;",
                 NULL, NULL, NULL);
    return 0;
}


int db_mark_dm_read(const char *username, const char *peer) {
    if (g_db == NULL || username == NULL || peer == NULL || peer[0] == '\0') {
        return 0;
    }
    if (db_is_debug_name(username)) {
        return 0;
    }

    const char *sql =
        "UPDATE messages SET is_read = 1 "
        "WHERE receiver = ?1 AND sender = ?2 AND is_read = 0;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, peer, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? sqlite3_changes(g_db) : -1;
}

int db_room_add_member(const char *room, const char *username) {
    if (g_db == NULL || room == NULL || username == NULL || db_is_debug_name(username)) {
        return 0;
    }
    const char *sql = "INSERT OR IGNORE INTO room_members (room, username) VALUES (?1, ?2);";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, username, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int db_room_remove_member(const char *room, const char *username) {
    if (g_db == NULL || room == NULL || username == NULL || db_is_debug_name(username)) {
        return 0;
    }
    const char *sql = "DELETE FROM room_members WHERE room = ?1 AND username = ?2;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, username, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int db_load_user_rooms(const char *username, char rooms_out[][MAX_NAME], int max_rooms) {
    if (g_db == NULL || username == NULL || rooms_out == NULL || max_rooms <= 0 || db_is_debug_name(username)) {
        return 0;
    }
    const char *sql = "SELECT room FROM room_members WHERE username = ?1 ORDER BY room ASC;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_TRANSIENT);
    int count = 0;
    while (count < max_rooms && sqlite3_step(stmt) == SQLITE_ROW) {
        const char *r = (const char *)sqlite3_column_text(stmt, 0);
        if (r != NULL && r[0] != '\0') {
            snprintf(rooms_out[count], MAX_NAME, "%.*s", MAX_NAME - 1, r);
            count++;
        }
    }
    sqlite3_finalize(stmt);
    return count;
}


long db_save_room_msg(const char *room, const char *sender, const char *text, time_t ts) {
    if (g_db == NULL || room == NULL || sender == NULL || text == NULL) {
        return 0;
    }
    if (db_is_debug_name(sender)) {
        return 0;
    }
    const char *sql =
        "INSERT INTO room_messages (room, sender, text, created_at) "
        "VALUES (?1, ?2, ?3, ?4);";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, sender, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, text, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 4, (sqlite3_int64)ts);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? (long)sqlite3_last_insert_rowid(g_db) : -1;
}

int db_load_room_history(const char *room, const char *username,
                         struct db_msg_record *out, int max_count) {
    if (g_db == NULL || room == NULL || username == NULL || out == NULL || max_count <= 0) {
        return 0;
    }
    if (db_is_debug_name(username)) {
        return 0;
    }

    sqlite3_int64 cleared_at = 0;
    sqlite3_stmt *cstmt = NULL;
    if (sqlite3_prepare_v2(g_db, "SELECT cleared_at FROM room_cleared WHERE room = ?1 AND username = ?2;",
                           -1, &cstmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(cstmt, 1, room, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(cstmt, 2, username, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(cstmt) == SQLITE_ROW) {
            cleared_at = sqlite3_column_int64(cstmt, 0);
        }
        sqlite3_finalize(cstmt);
    }


    sqlite3_int64 last_read_id = 0;
    sqlite3_stmt *rstmt = NULL;
    if (sqlite3_prepare_v2(g_db, "SELECT last_read_id FROM room_read WHERE room = ?1 AND username = ?2;",
                           -1, &rstmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(rstmt, 1, room, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(rstmt, 2, username, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(rstmt) == SQLITE_ROW) {
            last_read_id = sqlite3_column_int64(rstmt, 0);
        }
        sqlite3_finalize(rstmt);
    }

    const char *sql =


        "SELECT id, sender, text, created_at, is_edited FROM ("

        "  SELECT id, sender, text, created_at, is_edited FROM room_messages "
        "  WHERE room = ?1 AND created_at > ?2 "
        "  ORDER BY id DESC LIMIT ?3"
        ") ORDER BY id ASC;";

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, cleared_at);
    sqlite3_bind_int(stmt, 3, max_count);

    int count = 0;
    while (count < max_count && sqlite3_step(stmt) == SQLITE_ROW) {

        sqlite3_int64 msg_id = sqlite3_column_int64(stmt, 0);

        const char *sender = (const char *)sqlite3_column_text(stmt, 1);

        const char *text = (const char *)sqlite3_column_text(stmt, 2);

        sqlite3_int64 ts = sqlite3_column_int64(stmt, 3);

        int is_edited = sqlite3_column_int(stmt, 4);
        if (sender == NULL || text == NULL) {
            continue;
        }
        memset(&out[count], 0, sizeof(out[count]));
        out[count].is_room = 1;

        out[count].id = (long)msg_id;

        out[count].is_unread = (strcmp(sender, username) != 0 && msg_id > last_read_id) ? 1 : 0;

        out[count].is_edited = (is_edited != 0) ? 1 : 0;
        snprintf(out[count].peer, sizeof(out[count].peer), "%.*s", MAX_NAME - 1, room);
        snprintf(out[count].sender, sizeof(out[count].sender), "%.*s", MAX_NAME - 1, sender);
        copy_utf8_safe(out[count].text, sizeof(out[count].text), text);
        out[count].timestamp = (long)ts;
        count++;
    }

    sqlite3_finalize(stmt);
    return count;
}


static void db_exec_delete_by_room(const char *sql, const char *room) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
}

int db_delete_room_for_user(const char *username, const char *room, int remaining_active_members) {
    if (g_db == NULL || username == NULL || room == NULL || room[0] == '\0') {
        return 0;
    }
    if (db_is_debug_name(username)) {
        return 0;
    }

    db_room_remove_member(room, username);

    sqlite3_stmt *stmt = NULL;
    const char *sql_clr =
        "INSERT OR REPLACE INTO room_cleared (room, username, cleared_at) VALUES (?1, ?2, ?3);";
    if (sqlite3_prepare_v2(g_db, sql_clr, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, username, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 3, (sqlite3_int64)time(NULL));
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    int db_members = 0;
    if (sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM room_members WHERE room = ?1;",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            db_members = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    if (db_members == 0 && remaining_active_members <= 0) {

        db_exec_delete_by_room("DELETE FROM room_messages WHERE room = ?1;", room);

        db_exec_delete_by_room("DELETE FROM room_cleared WHERE room = ?1;", room);
        db_exec_delete_by_room("DELETE FROM room_read WHERE room = ?1;", room);

        db_exec_delete_by_room("DELETE FROM room_meta WHERE room = ?1;", room);
    }

    return 0;
}


int db_mark_room_read(const char *username, const char *room) {
    if (g_db == NULL || username == NULL || room == NULL || room[0] == '\0') {
        return 0;
    }
    if (db_is_debug_name(username)) {
        return 0;
    }

    sqlite3_int64 max_id = 0;
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, "SELECT COALESCE(MAX(id), 0) FROM room_messages WHERE room = ?1;",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            max_id = sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    const char *sql =
        "INSERT OR REPLACE INTO room_read (room, username, last_read_id) VALUES (?1, ?2, ?3);";
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, username, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, max_id);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? 0 : -1;
}


int db_edit_message(int is_room, long msg_id, const char *sender, const char *new_text) {
    if (g_db == NULL || msg_id <= 0 || sender == NULL || new_text == NULL || new_text[0] == '\0') {
        return 0;
    }
    if (db_is_debug_name(sender)) {
        return 0;
    }
    const char *sql = is_room
        ? "UPDATE room_messages SET text = ?1, is_edited = 1 WHERE id = ?2 AND sender = ?3;"
        : "UPDATE messages SET text = ?1, is_edited = 1 WHERE id = ?2 AND sender = ?3;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, new_text, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, (sqlite3_int64)msg_id);
    sqlite3_bind_text(stmt, 3, sender, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? sqlite3_changes(g_db) : -1;
}

int db_delete_message(int is_room, long msg_id, const char *sender) {
    if (g_db == NULL || msg_id <= 0 || sender == NULL) {
        return 0;
    }
    if (db_is_debug_name(sender)) {
        return 0;
    }
    const char *sql = is_room
        ? "DELETE FROM room_messages WHERE id = ?1 AND sender = ?2;"
        : "DELETE FROM messages WHERE id = ?1 AND sender = ?2;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int64(stmt, 1, (sqlite3_int64)msg_id);
    sqlite3_bind_text(stmt, 2, sender, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? sqlite3_changes(g_db) : -1;
}


int db_room_ensure_meta(const char *room, const char *default_owner) {
    if (g_db == NULL || room == NULL || room[0] == '\0') {
        return 0;
    }
    const char *owner = (default_owner != NULL) ? default_owner : "";
    sqlite3_stmt *stmt = NULL;
    const char *sql_ins =
        "INSERT OR IGNORE INTO room_meta (room, owner, topic, pinned_text) "
        "VALUES (?1, ?2, '', '');";
    if (sqlite3_prepare_v2(g_db, sql_ins, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, owner, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (owner[0] != '\0') {
        const char *sql_upd =
            "UPDATE room_meta SET owner = ?1 WHERE room = ?2 AND owner = '';";
        if (sqlite3_prepare_v2(g_db, sql_upd, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, owner, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, room, -1, SQLITE_TRANSIENT);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int db_room_get_meta(const char *room, struct db_room_meta *out) {
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (g_db == NULL || room == NULL || room[0] == '\0' || out == NULL) {
        return -1;
    }
    const char *sql = "SELECT owner, topic, pinned_text FROM room_meta WHERE room = ?1;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
    int found = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *o = (const char *)sqlite3_column_text(stmt, 0);
        const char *t = (const char *)sqlite3_column_text(stmt, 1);
        const char *p = (const char *)sqlite3_column_text(stmt, 2);
        if (o != NULL) {
            snprintf(out->owner, sizeof(out->owner), "%.*s", MAX_NAME - 1, o);
        }
        if (t != NULL) {
            copy_utf8_safe(out->topic, sizeof(out->topic), t);
        }
        if (p != NULL) {
            copy_utf8_safe(out->pinned_text, sizeof(out->pinned_text), p);
        }
        found = 1;
    }
    sqlite3_finalize(stmt);
    return found ? 0 : -1;
}

int db_room_set_topic(const char *room, const char *topic) {
    if (g_db == NULL || room == NULL || room[0] == '\0') {
        return -1;
    }
    db_room_ensure_meta(room, "");
    const char *sql = "UPDATE room_meta SET topic = ?1 WHERE room = ?2;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, topic != NULL ? topic : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, room, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int db_room_set_pin(const char *room, const char *pinned_text) {
    if (g_db == NULL || room == NULL || room[0] == '\0') {
        return -1;
    }
    db_room_ensure_meta(room, "");
    const char *sql = "UPDATE room_meta SET pinned_text = ?1 WHERE room = ?2;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, pinned_text != NULL ? pinned_text : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, room, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? 0 : -1;
}

int db_room_load_members(const char *room, char members_out[][MAX_NAME], int max_members) {
    if (g_db == NULL || room == NULL || room[0] == '\0' || members_out == NULL || max_members <= 0) {
        return 0;
    }
    const char *sql = "SELECT username FROM room_members WHERE room = ?1 ORDER BY username ASC;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(g_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(stmt, 1, room, -1, SQLITE_TRANSIENT);
    int count = 0;
    while (count < max_members && sqlite3_step(stmt) == SQLITE_ROW) {
        const char *u = (const char *)sqlite3_column_text(stmt, 0);
        if (u != NULL && u[0] != '\0') {
            snprintf(members_out[count], MAX_NAME, "%.*s", MAX_NAME - 1, u);
            count++;
        }
    }
    sqlite3_finalize(stmt);
    return count;
}
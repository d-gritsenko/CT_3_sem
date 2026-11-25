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
        "  created_at INTEGER NOT NULL"
        ");";

    char *errmsg = NULL;
    if (sqlite3_exec(g_db, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
        fprintf(stderr, "sqlite3_exec failed: %s\n", errmsg != NULL ? errmsg : "unknown");
        sqlite3_free(errmsg);
        sqlite3_close(g_db);
        g_db = NULL;
        return -1;
    }

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
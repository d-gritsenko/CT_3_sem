#define _DEFAULT_SOURCE

#include "crypto.h"

#include <errno.h>
#include <fcntl.h>
#include <sodium.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int read_exact(int fd, unsigned char *buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t r = read(fd, buf + got, len - got);
        if (r < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (r == 0) {
            return -1;
        }
        got += (size_t)r;
    }
    return 0;
}

static int write_exact(int fd, const unsigned char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t w = write(fd, buf + sent, len - sent);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        sent += (size_t)w;
    }
    return 0;
}

static int is_safe_key_user(const char *username) {

    if (username == NULL || username[0] == '\0') {

        return 0;
    }
    for (size_t i = 0; username[i] != '\0'; i++) {
        unsigned char c = (unsigned char)username[i];
        if (c == '/' || c == '\\' || c == '.' || c <= 32 || c == 127) {
            return 0;
        }
    }
    return 1;
}

int e2e_init(void) {
    return (sodium_init() >= 0) ? 0 : -1;
}

int e2e_load_or_create_keypair(const char *username,
                               unsigned char pk[E2E_PK_BYTES],
                               unsigned char sk[E2E_SK_BYTES],
                               int *was_created) {
    if (!is_safe_key_user(username) || pk == NULL || sk == NULL) {
        return -1;
    }
    if (was_created != NULL) {
        *was_created = 0;
    }

    const char *home = getenv("HOME");
    char dir_path[512];
    if (home != NULL && home[0] != '\0') {
        snprintf(dir_path, sizeof(dir_path), "%s/.deepspeak", home);
    } else {
        snprintf(dir_path, sizeof(dir_path), ".deepspeak");
    }

    if (mkdir(dir_path, 0700) < 0 && errno != EEXIST) {
        return -1;
    }
    chmod(dir_path, 0700);

    char key_path[600];
    char tmp_path[620];
    snprintf(key_path, sizeof(key_path), "%s/%s.key", dir_path, username);
    snprintf(tmp_path, sizeof(tmp_path), "%s/%s.key.tmp", dir_path, username);

    int fd = open(key_path, O_RDONLY);
    if (fd >= 0) {
        struct stat st;
        unsigned char raw[E2E_PK_BYTES + E2E_SK_BYTES];
        int ok = 0;
        if (fstat(fd, &st) == 0 &&
            st.st_size == (off_t)(E2E_PK_BYTES + E2E_SK_BYTES) &&
            read_exact(fd, raw, sizeof(raw)) == 0) {
            unsigned char derived[E2E_PK_BYTES];
            if (crypto_scalarmult_base(derived, raw + E2E_PK_BYTES) == 0 &&
                sodium_memcmp(derived, raw, E2E_PK_BYTES) == 0) {
                memcpy(pk, raw, E2E_PK_BYTES);
                memcpy(sk, raw + E2E_PK_BYTES, E2E_SK_BYTES);
                ok = 1;
            }
            sodium_memzero(derived, sizeof(derived));
        }
        sodium_memzero(raw, sizeof(raw));
        close(fd);
        if (ok) {
            chmod(key_path, 0600);
            return 0;
        }
    }

    crypto_box_keypair(pk, sk);

    unsigned char out_raw[E2E_PK_BYTES + E2E_SK_BYTES];
    memcpy(out_raw, pk, E2E_PK_BYTES);
    memcpy(out_raw + E2E_PK_BYTES, sk, E2E_SK_BYTES);

    int wfd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0600);
    if (wfd < 0) {
        sodium_memzero(out_raw, sizeof(out_raw));
        return -1;
    }
    fchmod(wfd, 0600);
    int wr = write_exact(wfd, out_raw, sizeof(out_raw));
    int cr = close(wfd);
    sodium_memzero(out_raw, sizeof(out_raw));

    if (wr != 0 || cr != 0 || rename(tmp_path, key_path) != 0) {
        unlink(tmp_path);
        return -1;
    }
    if (was_created != NULL) {
        *was_created = 1;
    }
    return 0;
}

void e2e_pk_to_hex(const unsigned char pk[E2E_PK_BYTES],
                   char out_hex[E2E_PK_HEX_SIZE]) {
    if (pk == NULL || out_hex == NULL) {
        return;
    }
    sodium_bin2hex(out_hex, E2E_PK_HEX_SIZE, pk, E2E_PK_BYTES);
}

int e2e_hex_to_pk(const char *hex, unsigned char out_pk[E2E_PK_BYTES]) {
    if (hex == NULL || out_pk == NULL) {
        return -1;
    }
    size_t hlen = strlen(hex);
    if (hlen != E2E_PK_BYTES * 2) {
        return -1;
    }
    size_t bin_len = 0;
    const char *hex_end = NULL;
    if (sodium_hex2bin(out_pk, E2E_PK_BYTES, hex, hlen, NULL, &bin_len, &hex_end) != 0) {
        return -1;
    }
    if (bin_len != E2E_PK_BYTES || hex_end != hex + hlen) {
        return -1;
    }
    return 0;
}

int e2e_is_encrypted(const char *text) {
    if (text == NULL) {
        return 0;
    }
    size_t plen = strlen(E2E_PREFIX);
    size_t slen = strlen(E2E_SUFFIX);
    size_t tlen = strlen(text);
    if (tlen <= plen + slen) {
        return 0;
    }
    return strncmp(text, E2E_PREFIX, plen) == 0 &&
           strcmp(text + tlen - slen, E2E_SUFFIX) == 0;
}

int e2e_encrypt_dm(const char *plaintext,
                   const unsigned char peer_pk[E2E_PK_BYTES],
                   const unsigned char my_sk[E2E_SK_BYTES],
                   char *out_buf, size_t out_size) {
    if (plaintext == NULL || peer_pk == NULL || my_sk == NULL ||
        out_buf == NULL || out_size == 0) {
        return -1;
    }

    size_t plen = strlen(plaintext);
    size_t cipher_len = plen + crypto_box_MACBYTES;
    size_t raw_len = crypto_box_NONCEBYTES + cipher_len;
    size_t b64_max = sodium_base64_encoded_len(raw_len, sodium_base64_VARIANT_ORIGINAL);
    size_t prefix_len = strlen(E2E_PREFIX);
    size_t suffix_len = strlen(E2E_SUFFIX);

    if (prefix_len + (b64_max - 1) + suffix_len + 1 > out_size) {
        return -1;
    }

    unsigned char *raw = malloc(raw_len);
    char *b64 = malloc(b64_max);
    if (raw == NULL || b64 == NULL) {
        free(raw);
        free(b64);
        return -1;
    }

    randombytes_buf(raw, crypto_box_NONCEBYTES);
    int rc = crypto_box_easy(raw + crypto_box_NONCEBYTES,
                             (const unsigned char *)plaintext,
                             (unsigned long long)plen,
                             raw,
                             peer_pk,
                             my_sk);
    if (rc == 0) {
        sodium_bin2base64(b64, b64_max, raw, raw_len, sodium_base64_VARIANT_ORIGINAL);
        snprintf(out_buf, out_size, "%s%s%s", E2E_PREFIX, b64, E2E_SUFFIX);
    }

    sodium_memzero(raw, raw_len);
    sodium_memzero(b64, b64_max);
    free(raw);
    free(b64);
    return (rc == 0) ? 0 : -1;
}

int e2e_decrypt_dm(const char *encoded,
                   const unsigned char peer_pk[E2E_PK_BYTES],
                   const unsigned char my_sk[E2E_SK_BYTES],
                   char *out_plain, size_t out_size) {
    if (!e2e_is_encrypted(encoded) || peer_pk == NULL || my_sk == NULL ||
        out_plain == NULL || out_size == 0) {
        return -1;
    }

    size_t prefix_len = strlen(E2E_PREFIX);
    size_t suffix_len = strlen(E2E_SUFFIX);
    size_t b64_len = strlen(encoded) - prefix_len - suffix_len;
    if (b64_len == 0) {
        return -1;
    }

    unsigned char *raw = malloc(b64_len);
    if (raw == NULL) {
        return -1;
    }

    size_t raw_len = 0;
    const char *b64_end = NULL;
    int dec = sodium_base642bin(raw, b64_len,
                                encoded + prefix_len, b64_len,
                                NULL, &raw_len, &b64_end,
                                sodium_base64_VARIANT_ORIGINAL);
    if (dec != 0 || b64_end != encoded + prefix_len + b64_len ||
        raw_len < crypto_box_NONCEBYTES + crypto_box_MACBYTES) {
        sodium_memzero(raw, b64_len);
        free(raw);
        return -1;
    }

    size_t cipher_len = raw_len - crypto_box_NONCEBYTES;
    size_t plain_len = cipher_len - crypto_box_MACBYTES;
    if (plain_len + 1 > out_size) {
        sodium_memzero(raw, b64_len);
        free(raw);
        return -1;
    }

    int rc = crypto_box_open_easy((unsigned char *)out_plain,
                                  raw + crypto_box_NONCEBYTES,
                                  (unsigned long long)cipher_len,
                                  raw,
                                  peer_pk,
                                  my_sk);
    if (rc == 0) {
        out_plain[plain_len] = '\0';
    } else {
        out_plain[0] = '\0';
    }

    sodium_memzero(raw, b64_len);
    free(raw);
    return (rc == 0) ? 0 : -1;
}

void e2e_fingerprint(const unsigned char pk_a[E2E_PK_BYTES],
                     const unsigned char pk_b[E2E_PK_BYTES],
                     char *out, size_t out_size) {
    if (pk_a == NULL || pk_b == NULL || out == NULL || out_size == 0) {
        return;
    }
    unsigned char combined[E2E_PK_BYTES * 2];
    if (memcmp(pk_a, pk_b, E2E_PK_BYTES) <= 0) {
        memcpy(combined, pk_a, E2E_PK_BYTES);
        memcpy(combined + E2E_PK_BYTES, pk_b, E2E_PK_BYTES);
    } else {
        memcpy(combined, pk_b, E2E_PK_BYTES);
        memcpy(combined + E2E_PK_BYTES, pk_a, E2E_PK_BYTES);
    }
    unsigned char hash[16];
    crypto_generichash(hash, sizeof(hash), combined, sizeof(combined), NULL, 0);
    snprintf(out, out_size, "%02X%02X %02X%02X %02X%02X %02X%02X",
             hash[0], hash[1], hash[2], hash[3],
             hash[4], hash[5], hash[6], hash[7]);
    sodium_memzero(combined, sizeof(combined));
    sodium_memzero(hash, sizeof(hash));
}

void e2e_zero(void *ptr, size_t len) {
    if (ptr != NULL && len > 0) {
        sodium_memzero(ptr, len);
    }
}   
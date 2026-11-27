#ifndef CRYPTO_H
#define CRYPTO_H

#include <stddef.h>

#define E2E_PK_BYTES 32
#define E2E_SK_BYTES 32
#define E2E_PK_HEX_SIZE 65
#define E2E_PREFIX "[[E2E:"
#define E2E_SUFFIX "]]"

int e2e_init(void);
int e2e_load_or_create_keypair(const char *username,
                               unsigned char pk[E2E_PK_BYTES],
                               unsigned char sk[E2E_SK_BYTES],
                               int *was_created);
void e2e_pk_to_hex(const unsigned char pk[E2E_PK_BYTES],
                   char out_hex[E2E_PK_HEX_SIZE]);
int e2e_hex_to_pk(const char *hex, unsigned char out_pk[E2E_PK_BYTES]);
int e2e_is_encrypted(const char *text);
int e2e_encrypt_dm(const char *plaintext,
                   const unsigned char peer_pk[E2E_PK_BYTES],
                   const unsigned char my_sk[E2E_SK_BYTES],
                   char *out_buf, size_t out_size);
int e2e_decrypt_dm(const char *encoded,
                   const unsigned char peer_pk[E2E_PK_BYTES],
                   const unsigned char my_sk[E2E_SK_BYTES],
                   char *out_plain, size_t out_size);
void e2e_fingerprint(const unsigned char pk_a[E2E_PK_BYTES],
                     const unsigned char pk_b[E2E_PK_BYTES],
                     char *out, size_t out_size);
void e2e_zero(void *ptr, size_t len);

#endif

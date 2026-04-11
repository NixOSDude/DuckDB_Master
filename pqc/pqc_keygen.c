/*
 * pqc_keygen.c — ML-DSA-65 Keypair Generator
 * DuckDB-Master PQC Signing Suite
 *
 * Generates a post-quantum ML-DSA-65 signing keypair.
 *
 * Secret key: ~/.duckpqc/signing.sec  (chmod 600 — never share)
 * Public key: ./duckpqc.pub           (share with clients for verification)
 *
 * Usage:
 *   ./pqc_keygen
 */

#include <oqs/oqs.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define PK_LEN   1952
#define SK_LEN   4032
#define PUB_FILE "duckpqc.pub"
#define SEC_DIR  ".duckpqc"
#define SEC_FILE ".duckpqc/signing.sec"

static int ensure_sec_dir(void) {
    const char *home = getenv("HOME");
    if (!home) { fprintf(stderr, "ERROR: $HOME not set\n"); return -1; }

    char path[512];
    snprintf(path, sizeof(path), "%s/%s", home, SEC_DIR);
    if (mkdir(path, 0700) != 0 && errno != EEXIST) {
        fprintf(stderr, "ERROR: cannot create %s: %s\n", path, strerror(errno));
        return -1;
    }
    return 0;
}

static int write_file(const char *path, const uint8_t *data, size_t len, mode_t mode) {
    FILE *f = fopen(path, "wb");
    if (!f) {
        fprintf(stderr, "ERROR: cannot open '%s' for write: %s\n", path, strerror(errno));
        return -1;
    }
    size_t written = fwrite(data, 1, len, f);
    fclose(f);
    if (written != len) {
        fprintf(stderr, "ERROR: short write to '%s'\n", path);
        return -1;
    }
    if (chmod(path, mode) != 0) {
        fprintf(stderr, "WARNING: chmod '%s' failed: %s\n", path, strerror(errno));
    }
    return 0;
}

int main(void) {
    printf("\n  DuckDB-Master — PQC Keygen\n");
    printf("  Algorithm: ML-DSA-65 (NIST FIPS 204)\n\n");

    /* Check existing keys */
    const char *home = getenv("HOME");
    if (!home) { fprintf(stderr, "ERROR: $HOME not set\n"); return 1; }

    char sec_path[512];
    snprintf(sec_path, sizeof(sec_path), "%s/%s", home, SEC_FILE);

    if (access(sec_path, F_OK) == 0) {
        printf("  WARNING: secret key already exists at %s\n", sec_path);
        printf("  Overwrite? [y/N]: ");
        char ans[8] = {0};
        if (!fgets(ans, sizeof(ans), stdin) || (ans[0] != 'y' && ans[0] != 'Y')) {
            printf("  Aborted. Existing key unchanged.\n\n");
            return 0;
        }
    }

    /* Generate keypair */
    printf("  Generating ML-DSA-65 keypair...\n");

    OQS_SIG *alg = OQS_SIG_new(OQS_SIG_alg_ml_dsa_65);
    if (!alg) {
        fprintf(stderr, "ERROR: OQS_SIG_new failed — liboqs not built with ML-DSA-65\n");
        return 1;
    }

    uint8_t pk[PK_LEN];
    uint8_t sk[SK_LEN];

    OQS_STATUS rc = OQS_SIG_keypair(alg, pk, sk);
    OQS_SIG_free(alg);

    if (rc != OQS_SUCCESS) {
        fprintf(stderr, "ERROR: OQS_SIG_keypair failed (status %d)\n", (int)rc);
        memset(sk, 0, sizeof(sk));
        return 1;
    }

    /* Write secret key */
    if (ensure_sec_dir() < 0) { memset(sk, 0, sizeof(sk)); return 1; }
    if (write_file(sec_path, sk, SK_LEN, 0600) < 0) {
        memset(sk, 0, sizeof(sk));
        return 1;
    }
    memset(sk, 0, sizeof(sk));   /* scrub from stack immediately */

    /* Write public key */
    if (write_file(PUB_FILE, pk, PK_LEN, 0644) < 0) return 1;

    printf("  Secret key : %s  (chmod 600 — never share)\n", sec_path);
    printf("  Public key : %s  (give this to clients)\n\n", PUB_FILE);
    printf("  Clients use duckpqc.pub + pqc_verify to authenticate your deliverables.\n");
    printf("  Keep signing.sec offline when not actively signing.\n\n");

    return 0;
}

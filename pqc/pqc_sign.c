/*
 * pqc_sign.c — ML-DSA-65 File Signing Tool
 * DuckDB-Master PQC Signing Suite
 *
 * Signs any file (Parquet, CSV, report) with your ML-DSA-65 secret key.
 * Produces a detached .sig file alongside the original.
 *
 * Signing payload (what the signature covers):
 *   <sha256-hex-of-file>|<basename>|<unix_timestamp>
 *
 * Output .sig format (human-readable):
 *   DUCKPQC-SIG-v1
 *   file=<basename>
 *   ts=<unix_timestamp>
 *   sha256=<sha256-hex>
 *   sig=<ml-dsa-65-hex-signature>
 *
 * Usage:
 *   ./pqc_sign <file> [secret_key_path]
 *   ./pqc_sign results.parquet
 *   ./pqc_sign report.csv ~/.duckpqc/signing.sec
 */

#include <oqs/oqs.h>
#include <openssl/evp.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define SK_LEN       4032
#define SIG_LEN      3309
#define HEXSIG_LEN   (SIG_LEN * 2)

/* ── Hex helpers ─────────────────────────────────────────────────────────── */

static void hex_encode(const uint8_t *in, size_t len, char *out) {
    static const char tbl[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        out[i*2]   = tbl[in[i] >> 4];
        out[i*2+1] = tbl[in[i] & 0xf];
    }
    out[len*2] = '\0';
}

/* ── SHA-256 of file (OpenSSL 3 EVP API) ─────────────────────────────────── */

static int sha256_file(const char *path, char *hex_out) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "ERROR: cannot open '%s': %s\n", path, strerror(errno));
        return -1;
    }

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    if (!ctx) { fclose(f); fprintf(stderr, "ERROR: EVP_MD_CTX_new failed\n"); return -1; }
    if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(ctx); fclose(f);
        fprintf(stderr, "ERROR: EVP_DigestInit_ex failed\n"); return -1;
    }

    uint8_t buf[65536];
    size_t  n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
        EVP_DigestUpdate(ctx, buf, n);

    int err = ferror(f);
    fclose(f);
    if (err) { EVP_MD_CTX_free(ctx); fprintf(stderr, "ERROR: read error on '%s'\n", path); return -1; }

    uint8_t digest[32];
    unsigned int dlen = 32;
    EVP_DigestFinal_ex(ctx, digest, &dlen);
    EVP_MD_CTX_free(ctx);
    hex_encode(digest, 32, hex_out);
    return 0;
}

/* ── Load secret key ─────────────────────────────────────────────────────── */

static int load_sk(const char *path, uint8_t *sk) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "ERROR: cannot open secret key '%s': %s\n",
                path, strerror(errno));
        return -1;
    }
    size_t n = fread(sk, 1, SK_LEN, f);
    uint8_t probe;
    size_t extra = fread(&probe, 1, 1, f);
    fclose(f);
    if (n != SK_LEN || extra != 0) {
        fprintf(stderr, "ERROR: secret key wrong size (%zu bytes, expected %d)\n",
                n + extra, SK_LEN);
        return -1;
    }
    return 0;
}

/* ── Basename (no libgen dependency) ─────────────────────────────────────── */

static const char *base_name(const char *path) {
    const char *p = strrchr(path, '/');
    return p ? p + 1 : path;
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(int argc, char *argv[]) {
    if (argc < 2 || argc > 3) {
        fprintf(stderr, "Usage: pqc_sign <file> [secret_key_path]\n");
        return 1;
    }

    const char *file_path = argv[1];

    /* Resolve secret key path */
    char default_sk[512];
    const char *sk_path;
    if (argc == 3) {
        sk_path = argv[2];
    } else {
        const char *home = getenv("HOME");
        if (!home) { fprintf(stderr, "ERROR: $HOME not set\n"); return 1; }
        snprintf(default_sk, sizeof(default_sk), "%s/.duckpqc/signing.sec", home);
        sk_path = default_sk;
    }

    printf("\n  DuckDB-Master — PQC Sign\n");
    printf("  File   : %s\n", file_path);
    printf("  Key    : %s\n", sk_path);

    /* SHA-256 the file */
    char sha256_hex[65];
    if (sha256_file(file_path, sha256_hex) < 0) return 1;
    printf("  SHA-256: %s\n", sha256_hex);

    /* Build signing payload */
    long long ts = (long long)time(NULL);
    const char *bname = base_name(file_path);

    char payload[1024];
    int plen = snprintf(payload, sizeof(payload), "%s|%s|%lld",
                        sha256_hex, bname, ts);
    if (plen <= 0 || plen >= (int)sizeof(payload)) {
        fprintf(stderr, "ERROR: payload too long\n");
        return 1;
    }

    /* Load secret key */
    uint8_t sk[SK_LEN];
    if (load_sk(sk_path, sk) < 0) return 1;

    /* Sign */
    OQS_SIG *alg = OQS_SIG_new(OQS_SIG_alg_ml_dsa_65);
    if (!alg) {
        fprintf(stderr, "ERROR: OQS_SIG_new failed\n");
        memset(sk, 0, sizeof(sk));
        return 1;
    }

    uint8_t sig_bytes[SIG_LEN];
    size_t  sig_len = SIG_LEN;
    OQS_STATUS rc = OQS_SIG_sign(alg,
                                  sig_bytes, &sig_len,
                                  (const uint8_t *)payload, (size_t)plen,
                                  sk);
    OQS_SIG_free(alg);
    memset(sk, 0, sizeof(sk));   /* scrub secret key immediately */

    if (rc != OQS_SUCCESS) {
        fprintf(stderr, "ERROR: OQS_SIG_sign failed (status %d)\n", (int)rc);
        return 1;
    }

    /* Hex-encode signature */
    char hex_sig[HEXSIG_LEN + 1];
    hex_encode(sig_bytes, sig_len, hex_sig);

    /* Write .sig file */
    char sig_path[512];
    snprintf(sig_path, sizeof(sig_path), "%s.sig", file_path);

    FILE *sf = fopen(sig_path, "w");
    if (!sf) {
        fprintf(stderr, "ERROR: cannot write '%s': %s\n", sig_path, strerror(errno));
        return 1;
    }
    fprintf(sf, "DUCKPQC-SIG-v1\n");
    fprintf(sf, "file=%s\n", bname);
    fprintf(sf, "ts=%lld\n", ts);
    fprintf(sf, "sha256=%s\n", sha256_hex);
    fprintf(sf, "sig=%s\n", hex_sig);
    fclose(sf);

    printf("  Signed : %s\n\n", sig_path);
    printf("  Deliver both files to the client:\n");
    printf("    %s\n", file_path);
    printf("    %s\n\n", sig_path);
    printf("  Client verifies with:\n");
    printf("    ./pqc_verify %s %s duckpqc.pub\n\n", file_path, sig_path);

    return 0;
}

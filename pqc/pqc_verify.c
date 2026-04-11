/*
 * pqc_verify.c — ML-DSA-65 File Verification Tool
 * DuckDB-Master PQC Signing Suite
 *
 * Verifies a file against its .sig file using Scott Baker's public key.
 * This tool is given to clients — they run it to authenticate deliverables.
 *
 * Checks:
 *   1. SHA-256 of file matches the hash in the .sig
 *   2. ML-DSA-65 signature over (sha256|filename|timestamp) is valid
 *   3. Timestamp is in the past (not a future-forged signature)
 *   4. Filename in .sig matches the file being verified
 *
 * Usage:
 *   ./pqc_verify <file> <file.sig> <duckpqc.pub>
 *   ./pqc_verify results.parquet results.parquet.sig duckpqc.pub
 *
 * Exit codes:
 *   0 — VALID signature
 *   1 — INVALID or tampered
 *   2 — usage / file error
 */

#include <oqs/oqs.h>
#include <openssl/evp.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define PK_LEN      1952
#define SIG_LEN     3309
#define HEXSIG_LEN  (SIG_LEN * 2)

/* ── Hex helpers ─────────────────────────────────────────────────────────── */

static void hex_encode(const uint8_t *in, size_t len, char *out) {
    static const char tbl[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        out[i*2]   = tbl[in[i] >> 4];
        out[i*2+1] = tbl[in[i] & 0xf];
    }
    out[len*2] = '\0';
}

static int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static int hex_decode(const char *hex, size_t hex_len, uint8_t *out) {
    if (hex_len % 2 != 0) return -1;
    for (size_t i = 0; i < hex_len / 2; i++) {
        int hi = hex_nibble(hex[i*2]);
        int lo = hex_nibble(hex[i*2+1]);
        if (hi < 0 || lo < 0) return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return 0;
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

/* ── Parse .sig file ─────────────────────────────────────────────────────── */

typedef struct {
    char file[256];
    long long ts;
    char sha256[65];
    char sig_hex[HEXSIG_LEN + 1];
} SigFile;

static int parse_sig(const char *path, SigFile *s) {
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "ERROR: cannot open sig file '%s': %s\n", path, strerror(errno));
        return -1;
    }

    /* Line buffer: sig line is "sig=" + HEXSIG_LEN chars + newline */
    char line[HEXSIG_LEN + 8];
    int  got_header = 0;
    memset(s, 0, sizeof(*s));

    while (fgets(line, sizeof(line), f)) {
        /* Strip newline */
        size_t len = strlen(line);
        while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r'))
            line[--len] = '\0';

        if (!got_header) {
            if (strcmp(line, "DUCKPQC-SIG-v1") != 0) {
                fprintf(stderr, "ERROR: not a DUCKPQC-SIG-v1 file\n");
                fclose(f);
                return -1;
            }
            got_header = 1;
            continue;
        }

        if (strncmp(line, "file=", 5) == 0) {
            strncpy(s->file, line + 5, sizeof(s->file) - 1);
            s->file[sizeof(s->file) - 1] = '\0';
        } else if (strncmp(line, "ts=", 3) == 0) {
            s->ts = strtoll(line + 3, NULL, 10);
        } else if (strncmp(line, "sha256=", 7) == 0) {
            strncpy(s->sha256, line + 7, sizeof(s->sha256) - 1);
            s->sha256[sizeof(s->sha256) - 1] = '\0';
        } else if (strncmp(line, "sig=", 4) == 0) {
            strncpy(s->sig_hex, line + 4, sizeof(s->sig_hex) - 1);
            s->sig_hex[sizeof(s->sig_hex) - 1] = '\0';
        }
    }
    fclose(f);

    if (!got_header || s->file[0] == '\0' || s->ts == 0 ||
        s->sha256[0] == '\0' || s->sig_hex[0] == '\0') {
        fprintf(stderr, "ERROR: incomplete or malformed .sig file\n");
        return -1;
    }
    return 0;
}

/* ── Basename ────────────────────────────────────────────────────────────── */

static const char *base_name(const char *path) {
    const char *p = strrchr(path, '/');
    return p ? p + 1 : path;
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: pqc_verify <file> <file.sig> <duckpqc.pub>\n");
        return 2;
    }

    const char *file_path = argv[1];
    const char *sig_path  = argv[2];
    const char *pub_path  = argv[3];

    printf("\n  DuckDB-Master — PQC Verify\n");
    printf("  File   : %s\n", file_path);
    printf("  Sig    : %s\n", sig_path);
    printf("  PubKey : %s\n\n", pub_path);

    int result = 0;  /* 0 = valid so far */

    /* Parse .sig file */
    SigFile sig;
    if (parse_sig(sig_path, &sig) < 0) return 1;

    /* Check filename matches */
    const char *bname = base_name(file_path);
    if (strcmp(sig.file, bname) != 0) {
        printf("  [FAIL] Filename mismatch: sig covers '%s', file is '%s'\n",
               sig.file, bname);
        result = 1;
    } else {
        printf("  [OK]   Filename matches: %s\n", sig.file);
    }

    /* Check timestamp is not in the future */
    time_t now = time(NULL);
    long long delta = (long long)now - sig.ts;
    if (delta < -60) {
        printf("  [FAIL] Timestamp is %lld seconds in the future — clock skew or forgery\n",
               -delta);
        result = 1;
    } else {
        time_t signed_at = (time_t)sig.ts;
        char tstr[64];
        struct tm *tm = gmtime(&signed_at);
        strftime(tstr, sizeof(tstr), "%Y-%m-%d %H:%M:%S UTC", tm);
        printf("  [OK]   Signed at: %s (%lld seconds ago)\n", tstr, delta);
    }

    /* SHA-256 the actual file */
    char actual_sha256[65];
    if (sha256_file(file_path, actual_sha256) < 0) return 2;

    if (strcmp(actual_sha256, sig.sha256) != 0) {
        printf("  [FAIL] SHA-256 MISMATCH — file has been tampered with\n");
        printf("         Expected : %s\n", sig.sha256);
        printf("         Actual   : %s\n", actual_sha256);
        result = 1;
    } else {
        printf("  [OK]   SHA-256 matches: %s\n", actual_sha256);
    }

    /* Load public key */
    FILE *pf = fopen(pub_path, "rb");
    if (!pf) {
        fprintf(stderr, "ERROR: cannot open public key '%s': %s\n",
                pub_path, strerror(errno));
        return 2;
    }
    uint8_t pk[PK_LEN];
    size_t  pk_n = fread(pk, 1, PK_LEN, pf);
    uint8_t probe;
    size_t  extra = fread(&probe, 1, 1, pf);
    fclose(pf);
    if (pk_n != PK_LEN || extra != 0) {
        fprintf(stderr, "ERROR: public key wrong size (%zu bytes, expected %d)\n",
                pk_n + extra, PK_LEN);
        return 2;
    }

    /* Decode hex signature */
    if (strlen(sig.sig_hex) != HEXSIG_LEN) {
        printf("  [FAIL] Signature wrong length in .sig file\n");
        return 1;
    }
    uint8_t sig_bytes[SIG_LEN];
    if (hex_decode(sig.sig_hex, HEXSIG_LEN, sig_bytes) < 0) {
        printf("  [FAIL] Signature contains invalid hex characters\n");
        return 1;
    }

    /* Reconstruct payload */
    char payload[1024];
    int plen = snprintf(payload, sizeof(payload), "%s|%s|%lld",
                        sig.sha256, sig.file, sig.ts);
    if (plen <= 0 || plen >= (int)sizeof(payload)) {
        fprintf(stderr, "ERROR: payload reconstruction failed\n");
        return 2;
    }

    /* Verify ML-DSA-65 signature */
    OQS_SIG *alg = OQS_SIG_new(OQS_SIG_alg_ml_dsa_65);
    if (!alg) {
        fprintf(stderr, "ERROR: OQS_SIG_new failed\n");
        return 2;
    }

    OQS_STATUS rc = OQS_SIG_verify(alg,
                                    (const uint8_t *)payload, (size_t)plen,
                                    sig_bytes, SIG_LEN,
                                    pk);
    OQS_SIG_free(alg);

    if (rc != OQS_SUCCESS) {
        printf("  [FAIL] ML-DSA-65 signature INVALID\n");
        result = 1;
    } else {
        printf("  [OK]   ML-DSA-65 signature VALID\n");
    }

    /* Final verdict */
    printf("\n");
    if (result == 0) {
        printf("  ╔══════════════════════════════════════════════════╗\n");
        printf("  ║  AUTHENTIC — signature verified                 ║\n");
        printf("  ║  This file was delivered by Scott Baker         ║\n");
        printf("  ║  and has not been modified since signing.       ║\n");
        printf("  ╚══════════════════════════════════════════════════╝\n\n");
    } else {
        printf("  ╔══════════════════════════════════════════════════╗\n");
        printf("  ║  VERIFICATION FAILED — do not trust this file   ║\n");
        printf("  ╚══════════════════════════════════════════════════╝\n\n");
    }

    return result;
}

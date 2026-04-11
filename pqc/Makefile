# PQC Signing Suite — Makefile
# ML-DSA-65 file signing tools for DuckDB-Master
#
# Requires: liboqs, openssl (libcrypto)
# Build all:  make
# Clean:      make clean

CC      = gcc
CFLAGS  = -O2 -Wall -Wextra -std=gnu11 $(shell pkg-config --cflags liboqs)
LDFLAGS = $(shell pkg-config --libs liboqs) -lcrypto

BINS = pqc_keygen pqc_sign pqc_verify

.PHONY: all clean

all: $(BINS)

pqc_keygen: pqc_keygen.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

pqc_sign: pqc_sign.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

pqc_verify: pqc_verify.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(BINS)

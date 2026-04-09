#!/usr/bin/env bash
# =============================================================================
# download_nyc_taxi.sh — NYC TLC Trip Record Downloader
#
# Downloads NYC Taxi & FHV Parquet files from the official TLC CDN.
# Files saved to data/nyc_taxi/parquet/<type>/
#
# Dataset types:
#   yellow  — Yellow Taxi trips         (~400MB/month compressed Parquet)
#   green   — Green Taxi trips          (~30MB/month)
#   fhv     — For-Hire Vehicle          (~150MB/month)
#   fhvhv   — High-Volume FHV           (~2.5GB/month — Uber, Lyft)
#
# Usage:
#   bash scripts/download_nyc_taxi.sh                        # all types, 2024
#   bash scripts/download_nyc_taxi.sh --type=yellow          # yellow only
#   bash scripts/download_nyc_taxi.sh --year=2023 --months=1-6
#   bash scripts/download_nyc_taxi.sh --dry-run              # show URLs only
# =============================================================================

set -euo pipefail
cd "$(dirname "$0")/.."

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'

log()  { echo -e "${CYAN}[TLC]${NC} $*"; }
ok()   { echo -e "${GREEN}[ OK ]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
die()  { echo -e "${RED}[FAIL]${NC} $*" >&2; exit 1; }

YEAR=2024
TYPES=("yellow" "green" "fhv" "fhvhv")
START_MONTH=1
END_MONTH=12
DRY_RUN=false
OUTDIR="data/nyc_taxi/parquet"
CDN="https://d37ci6vzurychx.cloudfront.net/trip-data"

for arg in "$@"; do
    case "$arg" in
        --dry-run)    DRY_RUN=true ;;
        --type=*)     IFS=',' read -ra TYPES <<< "${arg#--type=}" ;;
        --months=*)   range="${arg#--months=}"; START_MONTH="${range%-*}"; END_MONTH="${range#*-}" ;;
        --year=*)     YEAR="${arg#--year=}" ;;
        --help)
            echo "Usage: $0 [--dry-run] [--type=yellow,green,fhv,fhvhv] [--months=1-12] [--year=YYYY]"
            exit 0
            ;;
    esac
done

tlc_filename() {
    printf '%s_tripdata_%d-%02d.parquet' "$1" "$2" "$3"
}

echo -e "${BOLD}${CYAN}"
echo "  ╔══════════════════════════════════════════════════╗"
echo "  ║   NYC TLC Trip Record Downloader                ║"
echo "  ╚══════════════════════════════════════════════════╝"
echo -e "${NC}"
echo "  Year: ${YEAR}  |  Types: ${TYPES[*]}  |  Months: ${START_MONTH}–${END_MONTH}"
echo ""

[[ "$DRY_RUN" == "false" ]] && mkdir -p "${OUTDIR}/"{yellow,green,fhv,fhvhv}

DOWNLOADED=0; SKIPPED=0; FAILED=0

for type in "${TYPES[@]}"; do
    log "── ${type} ────────────────────────────────"
    for (( month=START_MONTH; month<=END_MONTH; month++ )); do
        filename=$(tlc_filename "$type" "$YEAR" "$month")
        url="${CDN}/${filename}"
        dest="${OUTDIR}/${type}/${filename}"

        [[ "$DRY_RUN" == "true" ]] && { echo "  [DRY] ${url}"; continue; }

        if [[ -f "$dest" ]] && [[ -s "$dest" ]]; then
            ok "Already exists: ${filename} ($(du -sh "$dest" | cut -f1)) — skip"
            SKIPPED=$((SKIPPED+1)); continue
        fi

        log "Downloading: ${filename}"
        if curl -fsSL --retry 3 --connect-timeout 30 -o "${dest}.tmp" "$url"; then
            mv "${dest}.tmp" "$dest"
            ok "${filename} ($(du -sh "$dest" | cut -f1))"
            DOWNLOADED=$((DOWNLOADED+1))
        else
            rm -f "${dest}.tmp"
            warn "Not available: ${filename} (TLC publishing lag — not an error)"
            FAILED=$((FAILED+1))
        fi
    done
done

echo ""
echo -e "${BOLD}${GREEN}Download complete${NC} — Downloaded: ${DOWNLOADED}  Skipped: ${SKIPPED}  Unavailable: ${FAILED}"
echo ""
[[ "$DRY_RUN" == "false" ]] && echo "  Next: bash scripts/benchmark_nyc_4year.sh"

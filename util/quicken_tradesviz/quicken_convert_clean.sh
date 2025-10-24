#!/usr/bin/env bash
# Quicken -> TradesViz converter runner
# - Uses the ticker-only, header+detail dividends Python converter.
# - Lets you set INPUT, OUTPUT, CASHFLOWS, SYMBOLS_MAP, TZ, and CONVERTER via flags or env vars.
# - Sensible defaults:
#     INPUT:       Rollover.txt
#     OUTPUT:      tradesviz_import.csv
#     CASHFLOWS:   cashflows.csv
#     SYMBOLS_MAP: symbols_map.csv
#     TZ:          America/New_York
#     CONVERTER:   latest quicken_to_tradesviz_full_divs_pairdetail_*.py in this directory (auto-detected)
#
# Examples:
#   ./quicken_convert.sh
#   ./quicken_convert.sh -i ~/exports/MyIRA.txt
#   ./quicken_convert.sh -o out/trades.csv -f out/cashflows.csv -s ~/maps/symbols_map.csv
#   INPUT=~/exports/Rollover.txt OUTPUT=tviz.csv CASHFLOWS=divs.csv ./quicken_convert.sh
#
# Notes:
# - Requires Python 3 and the converter script generated earlier.
# - If multiple versioned converter scripts are present, the newest is used by default.
# - Pass --no-cashflows if you only want the trades file.

set -euo pipefail

# Defaults (can be overridden by env or flags)
INPUT="${INPUT:-Rollover.txt}"
OUTPUT="${OUTPUT:-tradesviz_import.csv}"
CASHFLOWS="${CASHFLOWS:-cashflows.csv}"
SYMBOLS_MAP="${SYMBOLS_MAP:-symbols_map.csv}"
TZ="${TZ:-America/New_York}"
EMIT_CASHFLOWS=1
CONVERTER="${CONVERTER:-}"

usage() {
  cat <<'USAGE'
Usage: quicken_convert.sh [options]

Options:
  -i FILE   Input Quicken export (default: Rollover.txt)
  -o FILE   Output trades CSV (default: tradesviz_import.csv)
  -f FILE   Output dividends cashflows CSV (default: cashflows.csv)
  -s FILE   Symbols map CSV (default: symbols_map.csv)
  -t TZ     IANA timezone (default: America/New_York)
  -p FILE   Converter Python script path (override auto-detection)
  --no-cashflows  Do not emit cashflows.csv
  -h        Show this help

Environment overrides (same names as flags):
  INPUT, OUTPUT, CASHFLOWS, SYMBOLS_MAP, TZ, CONVERTER

Examples:
  ./quicken_convert.sh
  ./quicken_convert.sh -i ~/exports/MyIRA.txt
  ./quicken_convert.sh -o out/trades.csv -f out/cashflows.csv -s ~/maps/symbols_map.csv
  INPUT=~/exports/Rollover.txt OUTPUT=tviz.csv CASHFLOWS=divs.csv ./quicken_convert.sh
USAGE
}

# Basic getopt loop
ARGS=()
while (( "$#" )); do
  case "$1" in
    -i) INPUT="${2:-}"; shift 2;;
    -o) OUTPUT="${2:-}"; shift 2;;
    -f) CASHFLOWS="${2:-}"; shift 2;;
    -s) SYMBOLS_MAP="${2:-}"; shift 2;;
    -t) TZ="${2:-}"; shift 2;;
    -p) CONVERTER="${2:-}"; shift 2;;
    --no-cashflows) EMIT_CASHFLOWS=0; shift 1;;
    -h|--help) usage; exit 0;;
    --) shift; break;;
    -*)
      echo "Unknown option: $1" >&2
      usage; exit 2;;
    *) ARGS+=("$1"); shift;;
  esac
done

# Allow a trailing positional for INPUT if provided
if [[ ${#ARGS[@]} -gt 0 ]]; then
  INPUT="${ARGS[0]}"
fi

# Auto-detect converter if not set
if [[ -z "${CONVERTER}" ]]; then
  # prefer the newest versioned pairdetail script; fallback to any quicken_to_tradesviz*.py
  if ls -1t quicken_to_tradesviz_full_divs_pairdetail_*.py >/dev/null 2>&1; then
    CONVERTER="$(ls -1t quicken_to_tradesviz_full_divs_pairdetail_*.py | head -n1)"
  elif ls -1 quicken_to_tradesviz*.py >/dev/null 2>&1; then
    CONVERTER="$(ls -1 quicken_to_tradesviz*.py | head -n1)"
  else
    echo "ERROR: Could not find a converter script (quicken_to_tradesviz_*.py)." >&2
    echo "       Specify one with -p or CONVERTER=..." >&2
    exit 1
  fi
fi

# Sanity checks
[[ -f "${INPUT}" ]] || { echo "ERROR: INPUT not found: ${INPUT}" >&2; exit 1; }
[[ -f "${SYMBOLS_MAP}" ]] || { echo "ERROR: SYMBOLS_MAP not found: ${SYMBOLS_MAP}" >&2; exit 1; }
[[ -f "${CONVERTER}" ]] || { echo "ERROR: CONVERTER not found: ${CONVERTER}" >&2; exit 1; }

echo "Converter : ${CONVERTER}"
echo "Input     : ${INPUT}"
echo "Symbols   : ${SYMBOLS_MAP}"
echo "Output    : ${OUTPUT}"
if [[ "${EMIT_CASHFLOWS}" -eq 1 ]]; then
  echo "Cashflows : ${CASHFLOWS}"
else
  echo "Cashflows : (disabled)"
fi
echo "Timezone  : ${TZ}"
echo

set -x
if [[ "${EMIT_CASHFLOWS}" -eq 1 ]]; then
  python3 "${CONVERTER}" \
    --input "${INPUT}" \
    --output "${OUTPUT}" \
    --symbols-map "${SYMBOLS_MAP}" \
    --tz "${TZ}" \
    --emit-cashflows --cashflows-output "${CASHFLOWS}"
else
  python3 "${CONVERTER}" \
    --input "${INPUT}" \
    --output "${OUTPUT}" \
    --symbols-map "${SYMBOLS_MAP}" \
    --tz "${TZ}"
fi
set +x

echo "Done."

#!/usr/bin/env bash
# Parallel-vs-sequential file-writer download benchmark driver.
#
# For each file (standard + fragmented) and each writer (parallel default +
# sequential via HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY=1) it:
#   1. mints a FRESH xet read token before every run (HEAD resolve -> hash/size
#      once per file; GET xet-read-token per run),
#   2. runs the write_bench binary,
#   3. drops the first (warmup) run and reports stats over the remaining runs.
#
# Requires: $HF_TOKEN in the environment, curl, python3, and the write_bench
# binary (path via $BIN, default ./write_bench).
set -uo pipefail

BIN="${BIN:-./write_bench}"
HUB="${HF_ENDPOINT:-https://huggingface.co}"
REVISION="${REVISION:-main}"
WORKDIR="${WORKDIR:-$HOME/bench_work}"
WARMUP="${WARMUP:-1}"          # runs dropped at the start of each (file,writer)
MEASURED="${MEASURED:-5}"      # measured runs kept per (file,writer)
OUTFILE="$WORKDIR/bench_out.bin"

if [ -z "${HF_TOKEN:-}" ]; then echo "ERROR: HF_TOKEN not set" >&2; exit 1; fi
if [ ! -x "$BIN" ]; then echo "ERROR: benchmark binary not found/executable at $BIN" >&2; exit 1; fi
mkdir -p "$WORKDIR"

# case name | repo_id | filename | human label
CASES=(
  "standard|facebook/sam3.1|sam3.1_multiplex.pt|standard file"
  "fragmented|unsloth/gemma-4-31B-it-GGUF|mmproj-F32.gguf|fragmented file"
)

hget() { # extract a header value (case-insensitive) from a header dump on stdin
  grep -i "^$1:" | tail -n1 | sed -E "s/^[^:]*:[[:space:]]*//" | tr -d '\r'
}

# Fetch X-Xet-Hash + size for a file (stable across runs). Echoes: "<hash> <size>"
fetch_file_meta() {
  local repo="$1" file="$2" hdr
  # HEAD (no -L): read the metadata headers off the resolve 302 without pulling
  # the multi-GB body or leaking the auth header across a redirect to the CDN.
  hdr="$(curl -sS -I -H "Authorization: Bearer $HF_TOKEN" \
          "$HUB/$repo/resolve/$REVISION/$file")"
  local hash size
  hash="$(printf '%s' "$hdr" | hget 'X-Xet-Hash')"
  size="$(printf '%s' "$hdr" | hget 'X-Linked-Size')"
  [ -z "$size" ] && size="$(printf '%s' "$hdr" | hget 'Content-Length')"
  if [ -z "$hash" ]; then
    echo "ERROR: no X-Xet-Hash for $repo/$file (not xet-backed or no access)" >&2
    printf '%s\n' "$hdr" | grep -iE '^(HTTP/|x-error|x-xet|content-length|x-linked)' >&2
    return 1
  fi
  echo "$hash $size"
}

# Mint a fresh read token. Echoes: "<cas_url> <access_token> <exp>"
fetch_token() {
  local repo="$1" json
  json="$(curl -sS -H "Authorization: Bearer $HF_TOKEN" \
           "$HUB/api/models/$repo/xet-read-token/$REVISION")" || return 1
  printf '%s' "$json" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print(d["casUrl"], d["accessToken"], d["exp"])
' || { echo "ERROR: failed to parse token JSON: $json" >&2; return 1; }
}

stats() { # reads whitespace-separated numbers on stdin -> "mean median stdev min max"
  python3 -c '
import sys, statistics as st
xs = [float(x) for x in sys.stdin.read().split()]
if not xs:
    print("nan nan nan nan nan"); sys.exit()
sd = st.pstdev(xs) if len(xs) > 1 else 0.0
print(f"{st.mean(xs):.4f} {st.median(xs):.4f} {sd:.4f} {min(xs):.4f} {max(xs):.4f}")
'
}

echo "###############################################################"
echo "# xet-core file-writer download benchmark"
echo "# host: $(uname -m) $(nproc) vCPU | $(date -u +%FT%TZ)"
echo "# warmup(dropped)=$WARMUP measured=$MEASURED per (file,writer)"
echo "###############################################################"

declare -A SUMMARY   # "case|writer" -> total secs "mean median stdev min max"
declare -A WSUMMARY  # "case|writer" -> "write_secs_mean write_pct_of_total"
declare -A SIZES

for entry in "${CASES[@]}"; do
  IFS='|' read -r cname repo file label <<< "$entry"
  echo
  echo "=============================================================="
  echo "CASE: $cname  ($label)"
  echo "  repo=$repo  file=$file  rev=$REVISION"

  meta="$(fetch_file_meta "$repo" "$file")" || exit 1
  read -r HASH SIZE <<< "$meta"
  SIZES["$cname"]="$SIZE"
  size_gib="$(python3 -c "print(f'{$SIZE/1024**3:.2f}')")"
  echo "  hash=$HASH"
  echo "  size=$SIZE bytes (${size_gib} GiB)"

  for writer in parallel multi_fd sequential; do
    seq_env=0; mfd_env=0
    case "$writer" in
      sequential) seq_env=1 ;;   # single-handle in-order writes
      multi_fd)   mfd_env=1 ;;    # one fresh handle per term: open/seek/write/flush/close
      parallel)   : ;;           # shared handle + positioned (pwrite) writes -- baseline
    esac
    echo
    echo "  ----- writer=$writer (SEQUENTIALLY=$seq_env MULTI_FD=$mfd_env) -----"

    measured_secs=()
    measured_wsecs=()
    total=$(( WARMUP + MEASURED ))
    for i in $(seq 1 "$total"); do
      tok="$(fetch_token "$repo")" || exit 1
      read -r CAS_URL TOKEN EXP <<< "$tok"

      rm -f "$OUTFILE"
      line="$(HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY="$seq_env" HF_XET_RECONSTRUCT_WRITE_MULTI_FD="$mfd_env" "$BIN" \
                --cas-url "$CAS_URL" --token "$TOKEN" --token-exp "$EXP" \
                --hash "$HASH" --size "$SIZE" --out "$OUTFILE")"
      rc=$?
      rm -f "$OUTFILE"
      if [ $rc -ne 0 ] || [[ "$line" != RESULT* ]]; then
        echo "    run $i: FAILED (rc=$rc): $line" >&2
        exit 1
      fi
      secs="$(sed -E 's/.* secs=([0-9.]+).*/\1/' <<< "$line")"
      mibs="$(sed -E 's/.* mib_per_s=([0-9.]+).*/\1/' <<< "$line")"
      wsec="$(sed -E 's/.* write_secs=([0-9.]+).*/\1/' <<< "$line")"
      wfrac="$(sed -E 's/.* write_frac=([0-9.]+).*/\1/' <<< "$line")"
      wcalls="$(sed -E 's/.* write_calls=([0-9]+).*/\1/' <<< "$line")"
      wpct="$(python3 -c "print(f'{$wfrac*100:.1f}')")"

      if [ "$i" -le "$WARMUP" ]; then
        printf "    warmup %d: total %7.3fs %8s MiB/s | write %7.3fs (%s%% of total, %s calls)  (dropped)\n" \
               "$i" "$secs" "$mibs" "$wsec" "$wpct" "$wcalls"
      else
        printf "    run %d:    total %7.3fs %8s MiB/s | write %7.3fs (%s%% of total, %s calls)\n" \
               "$((i - WARMUP))" "$secs" "$mibs" "$wsec" "$wpct" "$wcalls"
        measured_secs+=("$secs")
        measured_wsecs+=("$wsec")
      fi
    done

    s="$(printf '%s\n' "${measured_secs[@]}" | stats)"
    read -r mean median stdev mn mx <<< "$s"
    ws="$(printf '%s\n' "${measured_wsecs[@]}" | stats)"
    read -r wmean wmedian wstdev wmn wmx <<< "$ws"
    mean_mibs="$(python3 -c "print(f'{($SIZE/1024**2)/$mean:.2f}')")"
    wpct_mean="$(python3 -c "print(f'{100*$wmean/$mean:.1f}' if $mean>0 else 'nan')")"
    printf "    --> total secs: mean=%s median=%s stdev=%s min=%s max=%s | mean=%s MiB/s\n" \
           "$mean" "$median" "$stdev" "$mn" "$mx" "$mean_mibs"
    printf "    --> write secs: mean=%s median=%s stdev=%s min=%s max=%s | %s%% of total time in write syscalls\n" \
           "$wmean" "$wmedian" "$wstdev" "$wmn" "$wmx" "$wpct_mean"
    SUMMARY["$cname|$writer"]="$mean $median $stdev $mn $mx"
    WSUMMARY["$cname|$writer"]="$wmean $wpct_mean"
  done
done

echo
echo "=============================================================="
echo "SUMMARY (seconds; lower is better). 'vs parallel' = mean / parallel_mean"
echo "  parallel = shared-handle pwrite (baseline) | multi_fd = fresh handle per term | sequential = single-handle in-order"
echo "=============================================================="
printf "%-11s %-11s %9s %9s %9s %9s %9s %10s %8s\n" case writer mean median stdev min max write_s write_%
for cname in standard fragmented; do
  for writer in parallel multi_fd sequential; do
    read -r mean median stdev mn mx <<< "${SUMMARY["$cname|$writer"]}"
    read -r wmean wpct <<< "${WSUMMARY["$cname|$writer"]}"
    printf "%-11s %-11s %9s %9s %9s %9s %9s %10s %8s\n" "$cname" "$writer" "$mean" "$median" "$stdev" "$mn" "$mx" "$wmean" "$wpct"
  done
  read -r pmean _ _ _ _ <<< "${SUMMARY["$cname|parallel"]}"
  read -r mmean _ _ _ _ <<< "${SUMMARY["$cname|multi_fd"]}"
  read -r smean _ _ _ _ <<< "${SUMMARY["$cname|sequential"]}"
  sz="${SIZES["$cname"]}"
  python3 -c "
pm,mm,sm,sz=$pmean,$mmean,$smean,$sz
mib=sz/1024**2
print(f'  {\"$cname\":<10} parallel  {pm:6.3f}s ({mib/pm:7.1f} MiB/s)  [baseline]')
print(f'  {\"\":<10} multi_fd  {mm:6.3f}s ({mib/mm:7.1f} MiB/s)  {mm/pm:.3f}x vs parallel')
print(f'  {\"\":<10} sequential{sm:6.3f}s ({mib/sm:7.1f} MiB/s)  {sm/pm:.3f}x vs parallel')
"
done
echo
echo "done."

#!/bin/bash
# Checks GCP/local-sync/uber_inbox.sh with fake brctl, gcloud, ls, stat and sleep, on macOS or Linux:
#   bash GCP/local-sync/test_uber_inbox.sh "$PWD/GCP/local-sync/uber_inbox.sh"
SCRIPT="$1"; ROOT="$(mktemp -d)"; FAIL=0
setup() {
  rm -rf "$ROOT"; mkdir -p "$ROOT/home/Documents/Pessoal/Gastos/Uber/inbox" "$ROOT/bin" "$ROOT/bucket" "$ROOT/home/Library/Logs"
  export HOME="$ROOT/home" PATH="$ROOT/bin:/usr/bin:/bin"
  INBOX="$HOME/Documents/Pessoal/Gastos/Uber/inbox"; PROCESSED="$HOME/Documents/Pessoal/Gastos/Uber/processed"
  cat > "$ROOT/bin/brctl" <<'B'
#!/bin/bash
echo "$*" >> "$HOME/brctl.calls"
B
  cat > "$ROOT/bin/sleep" <<'B'
#!/bin/bash
exit 0
B
  # stat -f %z FILE: the real size, or (when FILE.grow exists) one byte more on every call
  cat > "$ROOT/bin/stat" <<'B'
#!/bin/bash
f="${@: -1}"
if [[ -e "$f.grow" ]]; then n=$(cat "$f.grow"); echo $((n + 1)) > "$f.grow"; echo "$n"; else wc -c < "$f" | tr -d ' '; fi
B
  # gcloud storage cp SRC gs://finance_uber_data/inbox/NAME -> $ROOT/bucket/inbox/NAME, unless gcloud.fail exists
  cat > "$ROOT/bin/gcloud" <<'B'
#!/bin/bash
[[ -e "$HOME/gcloud.fail" ]] && { echo "ERROR: offline" >&2; exit 1; }
[[ "$1 $2" == "storage cp" ]] || exit 2
dest="${4#gs://finance_uber_data/}"; mkdir -p "$(dirname "$ROOT_BUCKET/$dest")"; cp "$3" "$ROOT_BUCKET/$dest"
B
  # ls fails as macOS does when the job may not read ~/Documents (no Full Disk Access)
  cat > "$ROOT/bin/ls" <<'B'
#!/bin/bash
[[ -e "$HOME/ls.fail" ]] && { echo "ls: Operation not permitted" >&2; exit 1; }
exec /bin/ls "$@"
B
  export ROOT_BUCKET="$ROOT/bucket"
  chmod +x "$ROOT/bin/"*
}
check() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; FAIL=1; fi; }
run() { bash "$SCRIPT"; }

setup; run
check "empty inbox: one log line, nothing uploaded" '[[ $(grep -c "0 zip" "$HOME/Library/Logs/finance-uber-sync.log") == 1 && -z "$(ls "$ROOT/bucket")" ]]'

setup; echo zipdata > "$INBOX/Uber Data Request A895E707 2.zip"; run
check "stable zip uploaded under its own name" '[[ -f "$ROOT/bucket/inbox/Uber Data Request A895E707 2.zip" ]]'
check "stable zip moved to processed" '[[ -f "$PROCESSED/Uber Data Request A895E707 2.zip" && ! -e "$INBOX/Uber Data Request A895E707 2.zip" ]]'
check "brctl download asked for it" 'grep -q "download" "$HOME/brctl.calls"'

setup; echo zipdata > "$INBOX/a.zip"; echo 100 > "$INBOX/a.zip.grow"; run
check "growing zip skipped and left in the inbox" '[[ -f "$INBOX/a.zip" && ! -e "$ROOT/bucket/inbox/a.zip" ]]'
rm "$INBOX/a.zip.grow"; run
check "the next run uploads it" '[[ -f "$ROOT/bucket/inbox/a.zip" && -f "$PROCESSED/a.zip" ]]'

setup; echo zipdata > "$INBOX/a.zip"; touch "$HOME/gcloud.fail"; run
check "failed upload leaves the zip in the inbox" '[[ -f "$INBOX/a.zip" && ! -e "$PROCESSED/a.zip" ]]'
check "failed upload is logged" 'grep -q "upload failed" "$HOME/Library/Logs/finance-uber-sync.log"'

setup; mkdir -p "$PROCESSED"; echo old > "$PROCESSED/a.zip"; echo new > "$INBOX/a.zip"; run
check "processed/ is never overwritten" '[[ $(cat "$PROCESSED/a.zip") == old && $(cat "$PROCESSED"/a\ \(*\).zip) == new ]]'

setup; touch "$INBOX/.b.zip.icloud"; run
check "an iCloud placeholder is asked to download" 'grep -q ".b.zip.icloud" "$HOME/brctl.calls"'

setup; echo zipdata > "$INBOX/a.zip"; touch "$HOME/ls.fail"; run; code=$?
check "an inbox it may not read is logged as such, not as empty" 'grep -q "Full Disk Access" "$HOME/Library/Logs/finance-uber-sync.log" && ! grep -q "0 zip" "$HOME/Library/Logs/finance-uber-sync.log"'
check "and the run fails" '[[ $code -ne 0 && -f "$INBOX/a.zip" ]]'

rm -rf "$ROOT"; exit $FAIL

#!/bin/bash
# Uploads the Uber exports saved in Gastos/Uber/inbox (iCloud) to the Uber bucket's inbox/,
# where ingest-uber loads them (finance-backend plans/uber-ingestion.md §3.2). Run by launchd
# every 30 minutes (com.finances.uber-inbox.plist).
#
# For each zip: ask iCloud to download it, wait until its size is the same on two looks
# 10 seconds apart, upload it, and only then move it to Gastos/Uber/processed/ - a file already
# there is never overwritten. A zip that is still arriving, or whose upload fails, stays in the
# inbox for the next run. One log line per zip and per run in ~/Library/Logs/finance-uber-sync.log.
#
# Install (once):
#   mkdir -p ~/Library/Scripts
#   cp GCP/local-sync/uber_inbox.sh ~/Library/Scripts/finance-uber-inbox.sh
#   cp GCP/local-sync/com.finances.uber-inbox.plist ~/Library/LaunchAgents/
#   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.finances.uber-inbox.plist
# Then give /bin/bash Full Disk Access (System Settings > Privacy & Security), or every run
# logs "Operation not permitted" on ~/Documents. Run it now and read the log:
#   launchctl kickstart -k gui/$(id -u)/com.finances.uber-inbox
#   tail ~/Library/Logs/finance-uber-sync.log
# After changing this script, copy it to ~/Library/Scripts again; launchd runs the copy.
# Uses the gcloud login of the Mac's user (gcloud auth login); no key file.

set -uo pipefail

GASTOS="$HOME/Documents/Pessoal/Gastos"
INBOX="$GASTOS/Uber/inbox"
PROCESSED="$GASTOS/Uber/processed"
BUCKET="gs://finance_uber_data"
LOG="$HOME/Library/Logs/finance-uber-sync.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG"; }

mkdir -p "$INBOX" "$PROCESSED"
# Without Full Disk Access, macOS hands a launchd job an empty listing of ~/Documents rather
# than an error, and every run would log "0 zip(s)" for ever. ls reports the refusal.
if ! ls "$INBOX" > /dev/null 2>> "$LOG"; then
  log "cannot read $INBOX: give /bin/bash Full Disk Access (System Settings > Privacy & Security)"
  exit 1
fi
shopt -s nullglob nocaseglob

# Older macOS keeps a file iCloud has not downloaded as .<name>.icloud; ask for it now, and it
# is a zip here on a later run.
for placeholder in "$INBOX"/.*.zip.icloud; do
  brctl download "$placeholder" 2>/dev/null || true
done

zips=("$INBOX"/*.zip)
log "run: ${#zips[@]} zip(s) in the inbox"
# macOS's /bin/bash (3.2) calls an empty array unbound under set -u, so the loop below must not
# see one: without this, every run with an empty inbox ended in an error.
[[ ${#zips[@]} -eq 0 ]] && exit 0

for zip in "${zips[@]}"; do
  name="$(basename "$zip")"
  brctl download "$zip" 2>/dev/null || true
  before="$(stat -f %z "$zip" 2>/dev/null || echo 0)"
  sleep 10
  after="$(stat -f %z "$zip" 2>/dev/null || echo 0)"
  if [[ "$before" != "$after" || "$after" -eq 0 ]]; then
    log "skipped $name: still arriving ($before then $after bytes)"
    continue
  fi
  if ! gcloud storage cp "$zip" "$BUCKET/inbox/$name" >> "$LOG" 2>&1; then
    log "upload failed: $name stays in the inbox"
    continue
  fi
  target="$PROCESSED/$name"
  if [[ -e "$target" ]]; then
    target="$PROCESSED/${name%.*} ($(date '+%Y-%m-%d %H%M')).zip"
  fi
  if mv "$zip" "$target"; then
    log "uploaded $name ($after bytes); moved to processed/$(basename "$target")"
  else
    log "uploaded $name, but could not move it to processed/: it will be uploaded again next run"
  fi
done

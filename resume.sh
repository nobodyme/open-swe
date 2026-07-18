#!/bin/bash

# Usage: ./resume_claude.sh HH:MM
TARGET_TIME="$1"

if [ -z "$TARGET_TIME" ]; then
  echo "Usage: $0 HH:MM (24-hour format)"
  exit 1
fi

# Convert to seconds since midnight
target_sec=$(( $(date -j -f "%H:%M" "$TARGET_TIME" +%s) ))
now_sec=$(( $(date +%s) ))

# If target time is in the future today, wait; else, exit
if [ "$target_sec" -le "$now_sec" ]; then
  echo "⏱️ $TARGET_TIME is in the past. Exiting."
  exit 1
fi

# Wait until the time comes
echo "🕒 Waiting until $TARGET_TIME..."
sleep $(( target_sec - now_sec ))

# === After waiting, run Claude automation ===

DIR=$(pwd)
STARTUP_DELAY=${CLAUDE_STARTUP_DELAY:-5}

# Keep hold of the exact tab that starts Claude. Using Terminal's `do script`
# also sends Return, so "1" is submitted instead of being joined to
# "continue" as "1continue".
osascript - "$DIR" "$STARTUP_DELAY" <<'APPLESCRIPT'
on run argv
    set workingDirectory to item 1 of argv
    set startupDelay to (item 2 of argv) as real

    tell application "Terminal"
        activate
        set claudeTab to do script "cd " & quoted form of workingDirectory & "; exec claude --resume"
    end tell

    delay startupDelay

    tell application "Terminal"
        do script "1" in claudeTab
        delay 1
        do script "continue" in claudeTab
        activate
    end tell
end run
APPLESCRIPT

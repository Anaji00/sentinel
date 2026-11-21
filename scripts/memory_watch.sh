#!/usr/bin/env bash
# Print every container above a memory share (default 85%) of its limit; exit 1 when any is. Run it by hand or from a scheduled task.
LIMIT="${1:-85}"
docker stats --no-stream --format '{{.Name}} {{.MemPerc}} {{.MemUsage}}' | awk -v lim="$LIMIT" '
{ gsub("%", "", $2); if ($2 + 0 >= lim) { printf "OVER %s%% %s %s\n", $2, $1, $3 " " $4 " " $5; bad = 1 } }
END { if (!bad) print "all containers under " lim "% of their memory limit"; exit bad }'

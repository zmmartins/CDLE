#!/usr/bin/env bash
# Copy the TPC-DS v4.0.0 query templates and apply the fixes needed to generate
# the queries with dsqgen and run all 99 of them in DuckDB.
#
# Usage: bash fix_templates.sh <kit folder (DSGen-software-code-4.0.0)> <destination folder>
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: bash fix_templates.sh <kit folder> <destination folder>" >&2
  exit 2
fi
KIT=$1
DEST=$2
if [ -e "$DEST" ]; then
  echo "$DEST already exists; remove it first" >&2
  exit 1
fi

cp -R "$KIT/query_templates" "$DEST"
cd "$DEST"

# 1. The v4.0.0 dialect files do not define _BEGIN/_END, so dsqgen stops with
#    "Substitution '_END' is used before being initialized".
cat >> netezza.tpl <<'EOF'
define _BEGIN = "-- start query " + [_QUERY] + " in stream " + [_STREAM] + " using template " + [_TEMPLATE];
define _END = "-- end query " + [_QUERY] + " in stream " + [_STREAM] + " using template " + [_TEMPLATE];
EOF

# 2. "cast(... as date) + 30 days" is not valid in DuckDB: use a standard SQL interval.
#    (15 templates: 5 12 16 20 21 32 37 40 77 80 82 92 94 95 98)
perl -pi -e 's/([+-]) *(\d+) +days\)/$1 interval \x27$2\x27 day)/g' query*.tpl

# 3. "at" is a reserved word in DuckDB; it is used as a table alias in query 90.
perl -pi -e 's/(wp_char_count between 5000 and 5200\)) at,/$1 at1,/' query90.tpl

# 4. Query 30 uses c_last_review_date_sk, but the kit's tpcds.sql names the column c_last_review_date.
perl -pi -e 's/c_last_review_date_sk/c_last_review_date/g' query30.tpl

# 5. Ambiguous ORDER BY columns in queries 58 and 72.
perl -pi -e 's/^(\s*order by\s+)item_id/$1ss_items.item_id/' query58.tpl
perl -pi -e 's/(order by total_cnt desc, i_item_desc, w_warehouse_name, )d_week_seq/$1d1.d_week_seq/' query72.tpl

# 6. "returns" is a reserved word in DuckDB; it is used as a column alias in query 77.
perl -pi -e 's/(?<!")\breturns\b(?!")/"returns"/g' query77.tpl

echo "Fixed templates written to $DEST"

# CS 451/551: Database Processing, Fall 2025

Acknowledgments and Thanks to Prof. Mohammad Sadoghi (UC Davis)

# M2 Testing information
Standard m2 tests:
python3 -u m2_tester_part1.py
python3 -u m2_tester_part2.py

- !Important! Delete the data created by m2_tester_part1 BEFORE running exam_tester_m2_part1!

Exam m2 tests:
python3 -u exam_tester_m2_part1.py
python3 -u exam_tester_m2_part2.py

### where things live

# Configuration & layout

    lstore/config.py

        - Meta columns: INDIRECTION, RID, TIMESTAMP, SCHEMA, and META_COLUMNS.

        - Persistence knobs: buffer-pool size, page capacity, filename suffixes.

        - DB metadata file name: metadata.json.

        - Merge flags: ENABLE_BACKGROUND_MERGE, MERGE_ON_CLOSE, MERGE_TAIL_THRESHOLD, FLUSH_ON_CLOSE.

        - Note: DATA_DIR is set dynamically by Database.open(path).



# Database lifecycle & catalog

    lstore/db.py

        - open(path): sets config.DATA_DIR = path, reads metadata.json, constructs tables/buffer pool, and calls Table.recover() per table.

        - close(): writes metadata.json as {name, num_columns, key_index}, merges only if MERGE_ON_CLOSE=True, then flushes dirty pages.



# Physical pages & buffer pool

    - lstore/page.py: fixed-size column pages with JSON toJSON()/fromJSON(). Page IDs are formatted to include table/column/page/base-or-tail.

    - lstore/pagebuffer.py: buffer pool with pin/unpin, dirty tracking, LRU-ish eviction, and flush_all().



# Table core (base/tail, updates, recovery, merge)

    lstore/table.py

        - page_directory: RID -> [(page_id, slot)] for all physical columns (meta + user).

        - Counters for next base/tail RID; deleted set; per-table Index; link to the buffer pool.

        - Insert: enforces PK uniqueness (via PK index if present; else scans base key cells), writes meta+user into base pages, updates indexes.

        - Update (cumulative tail): materializes the latest row, applies None as “no change,” builds a schema bitmask for changed columns, appends a tail RID with prev_ptr → previous RID, and updates base INDIRECTION to point to the new head.

        - Recovery: scans on-disk pages for base and tail records, rebuilds page_directory (meta + any written user cols), recovers counters, and re-creates the PK index.

        - Merge: implemented as a latest-values writeback into base pages; it clears base INDIRECTION/SCHEMA (history-collapsing). Because of that, we do not run merge on close by default.

        - Background-merge scaffolding (_schedule_merge, worker) exists; no automatic triggers are wired in this build.



# Query API & versioned reads

    lstore/query.py

        - API: insert, select, update, delete, sum (range), select_version, sum_version.

        - PK resolution prefers the PK index; fallback scans the base key column (never tails); respects deleted.

        - Version mapping: relative version (0, −1, −2, …) → non-negative index (0,1,2,…). The composer expects this already-normalized index (fixed a prior double-mapping bug).

        - Chain & compose: builds newest→older tail chain and overlays columns using the schema bitmask until all projected columns are satisfied (or we return the base).



# Indexes

    lstore/index.py

        - One dictionary per indexed user column; PK index is present by default.

        - create_index(col) populates from base records; drop_index(col) removes it.

        - update_entry() keeps secondary indexes consistent on updates (PK is immutable).

    lstore/bplustree.py

        - Optional B+-Tree reference; index.py currently uses hash maps but can be swapped.



# Storage model & lineage (what we implement)

    - Base pages store original inserts; tail pages store updates.

    - Indirection chain: base’s INDIRECTION points to the newest tail; each tail’s INDIRECTION points to the previous RID (tail or base).

    - Schema encoding on each tail is a bitmask of changed user columns.

    - Cumulative tails: tails carry the updated values for changed columns; reads reconstruct by overlaying tails newest→older until all needed columns are filled.



# Durability & buffer pool

    - All page I/O is rooted at config.DATA_DIR, which Database.open(path) sets (e.g., ./CS451 in the testers).

    - On page miss, the buffer pool loads from disk; on eviction, dirty pages are flushed.

    - On close(), dirty pages are flushed if FLUSH_ON_CLOSE=True.



# Recovery (what happens on open())

    1. Read metadata.json → build table list with {name, num_columns, key_index}.

    2. For each table, scan all base and tail pages and rebuild page_directory entries for:

        - Meta: RID, INDIRECTION, TIMESTAMP, SCHEMA

        - Any user columns present in that page

    3. Restore base/tail counters and (re)build the PK index.

    4. After recovery, tail chains are walkable (base head → tail → … → base), enabling versioned reads after a restart.



# Versioned reads (time-travel)

    Callers pass relative_version:

        0 = newest (latest tail or base if none)

        -1 = one version older

        -k = k versions older; beyond oldest tail → base

    We convert to an index (0,1,2,…) and:

        1. Start at that tail in the chain (or base if you asked too far back),

        2. Walk older tails, overlaying only columns where the schema bit is set,

        3. Stop when all projected columns are satisfied (or we reach base).



# Merge policy (current behavior)

    - Table.merge() exists and works, but it collapses history by writing latest values into base and clearing base INDIRECTION/SCHEMA.

    - Because we need time-travel across process boundaries (the “Part 1 → Part 2” testers), Database.close() does not call merge() unless you explicitly set MERGE_ON_CLOSE=True.

    - A background merge scaffold (queue + worker) is present; by default, no automatic threshold/“tail count” trigger is wired in this build. If you want to merge when a tail page seals or at a count threshold, add a call to _schedule_merge() in the tail-append/seal path.



# On-disk layout

<DATA_DIR>/               # set by Database.open(path)
  metadata.json           # [{name, num_columns, key_index}, ...]
  <table>/
    base/
      col_<i>_page_<n>.page.json
    tail/
      col_<i>_page_<n>.page.json

Each JSON page stores fixed-length integer slots and a small header. The buffer pool marshals these via Page.toJSON()/fromJSON().

# M2 Testing information
Standard m2 tests:
python3 -u m2_tester_part1.py
python3 -u m2_tester_part2.py

- !Important! Delete the data created by m2_tester_part1 BEFORE running exam_tester_m2_part1!

Exam m2 tests:
python3 -u exam_tester_m2_part1.py
python3 -u exam_tester_m2_part2.py

### where things live

# Configuration & layout

    lstore/config.py

        - Meta columns: INDIRECTION, RID, TIMESTAMP, SCHEMA, and META_COLUMNS.

        - Persistence knobs: buffer-pool size, page capacity, filename suffixes.

        - DB metadata file name: metadata.json.

        - Merge flags: ENABLE_BACKGROUND_MERGE, MERGE_ON_CLOSE, MERGE_TAIL_THRESHOLD, FLUSH_ON_CLOSE.

        - Note: DATA_DIR is set dynamically by Database.open(path).



# Database lifecycle & catalog

    lstore/db.py

        - open(path): sets config.DATA_DIR = path, reads metadata.json, constructs tables/buffer pool, and calls Table.recover() per table.

        - close(): writes metadata.json as {name, num_columns, key_index}, merges only if MERGE_ON_CLOSE=True, then flushes dirty pages.



# Physical pages & buffer pool

    - lstore/page.py: fixed-size column pages with JSON toJSON()/fromJSON(). Page IDs are formatted to include table/column/page/base-or-tail.

    - lstore/pagebuffer.py: buffer pool with pin/unpin, dirty tracking, LRU-ish eviction, and flush_all().



# Table core (base/tail, updates, recovery, merge)

    lstore/table.py

        - page_directory: RID -> [(page_id, slot)] for all physical columns (meta + user).

        - Counters for next base/tail RID; deleted set; per-table Index; link to the buffer pool.

        - Insert: enforces PK uniqueness (via PK index if present; else scans base key cells), writes meta+user into base pages, updates indexes.

        - Update (cumulative tail): materializes the latest row, applies None as “no change,” builds a schema bitmask for changed columns, appends a tail RID with prev_ptr → previous RID, and updates base INDIRECTION to point to the new head.

        - Recovery: scans on-disk pages for base and tail records, rebuilds page_directory (meta + any written user cols), recovers counters, and re-creates the PK index.

        - Merge: implemented as a latest-values writeback into base pages; it clears base INDIRECTION/SCHEMA (history-collapsing). Because of that, we do not run merge on close by default.

        - Background-merge scaffolding (_schedule_merge, worker) exists; no automatic triggers are wired in this build.



# Query API & versioned reads

    lstore/query.py

        - API: insert, select, update, delete, sum (range), select_version, sum_version.

        - PK resolution prefers the PK index; fallback scans the base key column (never tails); respects deleted.

        - Version mapping: relative version (0, −1, −2, …) → non-negative index (0,1,2,…). The composer expects this already-normalized index (fixed a prior double-mapping bug).

        - Chain & compose: builds newest→older tail chain and overlays columns using the schema bitmask until all projected columns are satisfied (or we return the base).



# Indexes

    lstore/index.py

        - One dictionary per indexed user column; PK index is present by default.

        - create_index(col) populates from base records; drop_index(col) removes it.

        - update_entry() keeps secondary indexes consistent on updates (PK is immutable).

    lstore/bplustree.py

        - Optional B+-Tree reference; index.py currently uses hash maps but can be swapped.



# Storage model & lineage (what we implement)

    - Base pages store original inserts; tail pages store updates.

    - Indirection chain: base’s INDIRECTION points to the newest tail; each tail’s INDIRECTION points to the previous RID (tail or base).

    - Schema encoding on each tail is a bitmask of changed user columns.

    - Cumulative tails: tails carry the updated values for changed columns; reads reconstruct by overlaying tails newest→older until all needed columns are filled.



# Durability & buffer pool

    - All page I/O is rooted at config.DATA_DIR, which Database.open(path) sets (e.g., ./CS451 in the testers).

    - On page miss, the buffer pool loads from disk; on eviction, dirty pages are flushed.

    - On close(), dirty pages are flushed if FLUSH_ON_CLOSE=True.



# Recovery (what happens on open())

    1. Read metadata.json → build table list with {name, num_columns, key_index}.

    2. For each table, scan all base and tail pages and rebuild page_directory entries for:

        - Meta: RID, INDIRECTION, TIMESTAMP, SCHEMA

        - Any user columns present in that page

    3. Restore base/tail counters and (re)build the PK index.

    4. After recovery, tail chains are walkable (base head → tail → … → base), enabling versioned reads after a restart.



# Versioned reads (time-travel)

    Callers pass relative_version:

        0 = newest (latest tail or base if none)

        -1 = one version older

        -k = k versions older; beyond oldest tail → base

    We convert to an index (0,1,2,…) and:

        1. Start at that tail in the chain (or base if you asked too far back),

        2. Walk older tails, overlaying only columns where the schema bit is set,

        3. Stop when all projected columns are satisfied (or we reach base).



# Merge policy (current behavior)

    - Table.merge() exists and works, but it collapses history by writing latest values into base and clearing base INDIRECTION/SCHEMA.

    - Because we need time-travel across process boundaries (the “Part 1 → Part 2” testers), Database.close() does not call merge() unless you explicitly set MERGE_ON_CLOSE=True.

    - A background merge scaffold (queue + worker) is present; by default, no automatic threshold/“tail count” trigger is wired in this build. If you want to merge when a tail page seals or at a count threshold, add a call to _schedule_merge() in the tail-append/seal path.



# On-disk layout

<DATA_DIR>/               # set by Database.open(path)
  metadata.json           # [{name, num_columns, key_index}, ...]
  <table>/
    base/
      col_<i>_page_<n>.page.json
    tail/
      col_<i>_page_<n>.page.json

Each JSON page stores fixed-length integer slots and a small header. The buffer pool marshals these via Page.toJSON()/fromJSON().
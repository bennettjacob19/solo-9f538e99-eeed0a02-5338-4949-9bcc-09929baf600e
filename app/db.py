"""SQLite storage for the simulated two-disk virtual volume.

Physical model
--------------
* Two disks (disk 0 / disk 1), each with 16 slots -> 32 data slots total.
* Every slot holds one 4 KiB block. The volume initially fills every slot.
* The volume owns ONE extra staging slot (table ``staging``) that can hold at
  most one block body; it is not part of the 32 regular disk slots.
"""

from __future__ import annotations

import os
import sqlite3

BLOCK_SIZE = 4096
DISK_COUNT = 2
SLOTS_PER_DISK = 16
BLOCK_COUNT = DISK_COUNT * SLOTS_PER_DISK  # 32

SCHEMA = """
CREATE TABLE IF NOT EXISTS slots (
    disk  INTEGER NOT NULL CHECK (disk IN (0, 1)),
    slot  INTEGER NOT NULL CHECK (slot BETWEEN 0 AND 15),
    lbn   INTEGER NOT NULL,
    body  BLOB    NOT NULL,
    PRIMARY KEY (disk, slot)
);

CREATE TABLE IF NOT EXISTS locations (
    lbn    INTEGER PRIMARY KEY,
    disk   INTEGER NOT NULL,
    slot   INTEGER NOT NULL,
    staged INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS staging (
    lbn  INTEGER PRIMARY KEY,
    body BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS migration (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    stripe_k     INTEGER NOT NULL CHECK (stripe_k IN (2, 4)),
    status       TEXT    NOT NULL CHECK (status IN ('running', 'done')),
    next_op      INTEGER NOT NULL,
    total_ops    INTEGER NOT NULL,
    last_writes  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ops (
    seq      INTEGER PRIMARY KEY,
    op       TEXT NOT NULL CHECK (op IN ('stage', 'move', 'unstage')),
    lbn      INTEGER NOT NULL,
    src_disk INTEGER,
    src_slot INTEGER,
    dst_disk INTEGER,
    dst_slot INTEGER
);

CREATE TABLE IF NOT EXISTS events (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     TEXT NOT NULL DEFAULT (datetime('now')),
    kind   TEXT NOT NULL,
    detail TEXT NOT NULL
);

-- At most one trial-write round exists at a time; the row is deleted when the
-- round is confirmed or revoked.  A row left behind by a process kill is the
-- "unfinished round" that a restart lets the operator continue.
CREATE TABLE IF NOT EXISTS trial_round (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    created_ts     TEXT NOT NULL DEFAULT (datetime('now')),
    undo_positions INTEGER NOT NULL DEFAULT 0,
    undo_bytes     INTEGER NOT NULL DEFAULT 0,
    last_action    TEXT
);

-- One row per distinct (lbn, byte offset) that currently has a trial value.
-- ``orig_byte`` is the committed byte captured at the first trial write and is
-- the only separately stored payload: undo bytes total <= 4096 by the round
-- quota, so stored trial/restore payload is hard-capped at 4 KiB.
CREATE TABLE IF NOT EXISTS trial_bytes (
    lbn       INTEGER NOT NULL,
    offset    INTEGER NOT NULL CHECK (offset BETWEEN 0 AND 4095),
    orig_byte BLOB NOT NULL CHECK (length(orig_byte) = 1),
    PRIMARY KEY (lbn, offset)
);

-- Result of the most recent confirm/revoke, kept after the round closes so
-- the console (and a restarted process) can still report actual body writes.
CREATE TABLE IF NOT EXISTS trial_last (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    action           TEXT NOT NULL,
    affected_blocks  INTEGER NOT NULL,
    body_writes      INTEGER NOT NULL,
    disk_writes      INTEGER NOT NULL,
    staging_writes   INTEGER NOT NULL,
    positions        INTEGER NOT NULL,
    ts               TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

TRIAL_POSITION_LIMIT = 4096
TRIAL_STORE_BUDGET = 4096  # bytes: separately stored trial/restore payload cap


def default_db_path() -> str:
    return os.environ.get("VIRTUAL_VOLUME_DB", os.path.join("data", "volume.db"))


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or default_db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # Rollback journal + FULL fsync: a process killed before COMMIT leaves the
    # journal in place and the next connection recovers the committed state.
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn


def seed_body(lbn: int) -> bytes:
    """Deterministic 4 KiB body for the initial fill."""
    header = f"INIT-BLOCK-LBN-{lbn:02d}\n".encode("ascii")
    return header + b"\x00" * (BLOCK_SIZE - len(header))


def initialize(conn: sqlite3.Connection) -> None:
    """(Re)create the initial volume: 32 blocks, single-block alternating."""
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM slots")
        conn.execute("DELETE FROM locations")
        conn.execute("DELETE FROM staging")
        conn.execute("DELETE FROM migration")
        conn.execute("DELETE FROM ops")
        conn.execute("DELETE FROM trial_bytes")
        conn.execute("DELETE FROM trial_round")
        conn.execute("DELETE FROM trial_last")
        for lbn in range(BLOCK_COUNT):
            # Initial striping: single-block alternating across the two disks.
            disk = lbn % DISK_COUNT
            slot = lbn // DISK_COUNT
            conn.execute(
                "INSERT INTO slots(disk, slot, lbn, body) VALUES (?, ?, ?, ?)",
                (disk, slot, lbn, seed_body(lbn)),
            )
            conn.execute(
                "INSERT INTO locations(lbn, disk, slot, staged) VALUES (?, ?, ?, 0)",
                (lbn, disk, slot),
            )
        conn.execute(
            "INSERT INTO events(kind, detail) VALUES ('init', ?)",
            ("volume initialized: 2 disks x 16 slots, single-block alternating",),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def ensure_seeded(conn: sqlite3.Connection) -> None:
    count = conn.execute("SELECT COUNT(*) AS c FROM slots").fetchone()["c"]
    if count == 0:
        initialize(conn)

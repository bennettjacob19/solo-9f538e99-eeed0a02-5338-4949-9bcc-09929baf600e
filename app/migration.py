"""Re-striping planner and crash-safe migration engine.

Permutation idea
----------------
Changing the stripe width only permutes the *positions* of the 32 logical
blocks: every old (disk, slot) has exactly one target (disk, slot).  A
permutation decomposes into cycles; each non-trivial cycle is carried out with
a single staging slot:

    stage block at cycle[0]            -> hole at cycle[0], block in staging
    move cycle[i] -> cycle[i+1] ...    -> hole walks backwards around the cycle
    unstage to the last hole

Only one block is ever held in staging, matching the "at most one extra
staged body" constraint.  Every operation (staging included) is one disk-slot
body write and is counted against the per-batch write budget; a budget of 1
still performs exactly one operation, so any positive budget makes progress.

Crash semantics
---------------
A batch runs inside one SQLite transaction.  ``simulate_crash`` kills the
process before COMMIT; the rollback journal then restores the last committed
disk layout on the next startup, while every already committed batch survives.
"""

from __future__ import annotations

import os
import sqlite3

from . import trial
from .db import BLOCK_COUNT, DISK_COUNT, SLOTS_PER_DISK


# --------------------------------------------------------------------------- #
# Layout math
# --------------------------------------------------------------------------- #

def old_position(lbn: int) -> tuple[int, int]:
    """Initial single-block alternating layout for logical block ``lbn``."""
    return lbn % DISK_COUNT, lbn // DISK_COUNT


def new_position(lbn: int, stripe_k: int) -> tuple[int, int]:
    """Target layout for run-length-``stripe_k`` alternating stripes."""
    group, within = divmod(lbn, stripe_k)
    disk = group % DISK_COUNT
    slot = (group // DISK_COUNT) * stripe_k + within
    return disk, slot


def _pos_index(disk: int, slot: int) -> int:
    return disk * SLOTS_PER_DISK + slot


def _pos_parts(index: int) -> tuple[int, int]:
    return divmod(index, SLOTS_PER_DISK)


# --------------------------------------------------------------------------- #
# Planner
# --------------------------------------------------------------------------- #

def plan_operations(stripe_k: int,
                    current: dict[int, tuple[int, int]] | None = None) -> list[dict]:
    """Return the ordered stage/move/unstage operations.

    ``current`` maps lbn -> (disk, slot) for the layout being migrated away
    from.  It defaults to the initial single-block alternating layout, so the
    planner also works when a volume is re-striped more than once.
    """
    if current is None:
        current = {lbn: old_position(lbn) for lbn in range(BLOCK_COUNT)}

    # occupant[p] = lbn currently at physical position p; target[p] = where
    # that block must move to.  Together they define the position permutation.
    occupant: dict[int, int] = {}
    target: dict[int, int] = {}
    for lbn, (disk, slot) in current.items():
        src = _pos_index(disk, slot)
        new_d, new_s = new_position(lbn, stripe_k)
        occupant[src] = lbn
        target[src] = _pos_index(new_d, new_s)

    visited: set[int] = set()
    ops: list[dict] = []
    for start in range(BLOCK_COUNT):
        if start in visited or target[start] == start:
            visited.add(start)
            continue

        # Walk the permutation cycle starting at ``start``.
        cycle: list[int] = []
        p = start
        while p not in visited:
            visited.add(p)
            cycle.append(p)
            p = target[p]

        first = cycle[0]
        first_d, first_s = _pos_parts(first)
        first_lbn = occupant[first]
        ops.append(
            dict(op="stage", lbn=first_lbn,
                 src_disk=first_d, src_slot=first_s,
                 dst_disk=None, dst_slot=None)
        )

        # Walk the hole backwards: cycle[i] -> target(cycle[i]) = cycle[i+1].
        for i in range(len(cycle) - 1, 0, -1):
            src = cycle[i]
            dst = target[src]
            src_d, src_s = _pos_parts(src)
            dst_d, dst_s = _pos_parts(dst)
            ops.append(
                dict(op="move", lbn=occupant[src],
                     src_disk=src_d, src_slot=src_s,
                     dst_disk=dst_d, dst_slot=dst_s)
            )

        last_hole = cycle[1]
        hole_d, hole_s = _pos_parts(last_hole)
        ops.append(
            dict(op="unstage", lbn=first_lbn,
                 src_disk=None, src_slot=None,
                 dst_disk=hole_d, dst_slot=hole_s)
        )
    return ops


def start_migration(conn: sqlite3.Connection, stripe_k: int) -> None:
    existing = conn.execute("SELECT status FROM migration WHERE id = 1").fetchone()
    if existing is not None and existing["status"] == "running":
        raise RuntimeError("a migration is already running")
    if stripe_k not in (2, 4):
        raise ValueError("stripe width must be 2 or 4")

    rows = conn.execute("SELECT lbn, disk, slot FROM locations").fetchall()
    if conn.execute("SELECT COUNT(*) AS c FROM staging").fetchone()["c"]:
        raise RuntimeError("cannot start a new migration while a block is staged")
    current = {r["lbn"]: (r["disk"], r["slot"]) for r in rows}
    if len(current) != BLOCK_COUNT:
        raise RuntimeError("volume is not fully populated; cannot plan migration")
    ops = plan_operations(stripe_k, current)
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM migration")
        conn.execute("DELETE FROM ops")
        conn.execute(
            "INSERT INTO migration(id, stripe_k, status, next_op, total_ops, last_writes)"
            " VALUES (1, ?, 'running', 0, ?, 0)",
            (stripe_k, len(ops)),
        )
        for seq, item in enumerate(ops):
            conn.execute(
                "INSERT INTO ops(seq, op, lbn, src_disk, src_slot, dst_disk, dst_slot)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (seq, item["op"], item["lbn"], item["src_disk"], item["src_slot"],
                 item["dst_disk"], item["dst_slot"]),
            )
        conn.execute(
            "INSERT INTO events(kind, detail) VALUES ('migration_start', ?)",
            (f"re-stripe to run-length {stripe_k}; {len(ops)} slot writes planned",),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #

def _apply_stage(conn: sqlite3.Connection, row: sqlite3.Row) -> None:
    if conn.execute("SELECT COUNT(*) AS c FROM staging").fetchone()["c"] != 0:
        raise RuntimeError("staging slot already occupied")
    slot = conn.execute(
        "SELECT body FROM slots WHERE disk = ? AND slot = ?",
        (row["src_disk"], row["src_slot"]),
    ).fetchone()
    if slot is None:
        raise RuntimeError(f"stage source empty at disk{row['src_disk']}.{row['src_slot']}")
    conn.execute("INSERT INTO staging(lbn, body) VALUES (?, ?)",
                 (row["lbn"], slot["body"]))
    conn.execute("DELETE FROM slots WHERE disk = ? AND slot = ?",
                 (row["src_disk"], row["src_slot"]))
    conn.execute("UPDATE locations SET staged = 1 WHERE lbn = ?", (row["lbn"],))


def _apply_move(conn: sqlite3.Connection, row: sqlite3.Row) -> None:
    src = conn.execute(
        "SELECT body FROM slots WHERE disk = ? AND slot = ?",
        (row["src_disk"], row["src_slot"]),
    ).fetchone()
    if src is None:
        raise RuntimeError(f"move source empty at disk{row['src_disk']}.{row['src_slot']}")
    dst = conn.execute(
        "SELECT 1 FROM slots WHERE disk = ? AND slot = ?",
        (row["dst_disk"], row["dst_slot"]),
    ).fetchone()
    if dst is not None:
        raise RuntimeError(f"move destination occupied at disk{row['dst_disk']}.{row['dst_slot']}")
    conn.execute(
        "INSERT INTO slots(disk, slot, lbn, body) VALUES (?, ?, ?, ?)",
        (row["dst_disk"], row["dst_slot"], row["lbn"], src["body"]),
    )
    conn.execute("DELETE FROM slots WHERE disk = ? AND slot = ?",
                 (row["src_disk"], row["src_slot"]))
    conn.execute(
        "UPDATE locations SET disk = ?, slot = ?, staged = 0 WHERE lbn = ?",
        (row["dst_disk"], row["dst_slot"], row["lbn"]),
    )


def _apply_unstage(conn: sqlite3.Connection, row: sqlite3.Row) -> None:
    staged = conn.execute("SELECT body FROM staging WHERE lbn = ?",
                          (row["lbn"],)).fetchone()
    if staged is None:
        raise RuntimeError(f"lbn {row['lbn']} is not in staging")
    dst = conn.execute(
        "SELECT 1 FROM slots WHERE disk = ? AND slot = ?",
        (row["dst_disk"], row["dst_slot"]),
    ).fetchone()
    if dst is not None:
        raise RuntimeError(f"unstage destination occupied at disk{row['dst_disk']}.{row['dst_slot']}")
    conn.execute(
        "INSERT INTO slots(disk, slot, lbn, body) VALUES (?, ?, ?, ?)",
        (row["dst_disk"], row["dst_slot"], row["lbn"], staged["body"]),
    )
    conn.execute("DELETE FROM staging WHERE lbn = ?", (row["lbn"],))
    conn.execute(
        "UPDATE locations SET disk = ?, slot = ?, staged = 0 WHERE lbn = ?",
        (row["dst_disk"], row["dst_slot"], row["lbn"]),
    )


def run_batch(conn: sqlite3.Connection, budget: int, simulate_crash: bool = False) -> dict:
    """Execute up to ``budget`` slot-body writes as one transaction."""
    if not isinstance(budget, int) or budget < 1:
        raise ValueError("write budget must be a positive integer")
    mig = conn.execute("SELECT * FROM migration WHERE id = 1").fetchone()
    if mig is None:
        raise RuntimeError("no migration in progress; start one first")
    if mig["status"] != "running":
        raise RuntimeError("migration already complete")

    conn.execute("BEGIN IMMEDIATE")
    try:
        executed = 0
        next_op = mig["next_op"]
        while executed < budget and next_op < mig["total_ops"]:
            row = conn.execute("SELECT * FROM ops WHERE seq = ?", (next_op,)).fetchone()
            if row["op"] == "stage":
                _apply_stage(conn, row)
            elif row["op"] == "move":
                _apply_move(conn, row)
            else:
                _apply_unstage(conn, row)
            conn.execute(
                "INSERT INTO events(kind, detail) VALUES ('slot_write', ?)",
                (f"op#{next_op} {row['op']} lbn={row['lbn']}",),
            )
            next_op += 1
            executed += 1

        done = next_op >= mig["total_ops"]
        conn.execute(
            "UPDATE migration SET next_op = ?, last_writes = ?,"
            " status = CASE WHEN ? THEN 'done' ELSE status END WHERE id = 1",
            (next_op, executed, 1 if done else 0),
        )
        if done:
            conn.execute(
                "INSERT INTO events(kind, detail) VALUES ('migration_done', ?)",
                (f"re-striping to run-length {mig['stripe_k']} complete after "
                 f"{next_op} slot writes",),
            )
        else:
            conn.execute(
                "INSERT INTO events(kind, detail) VALUES ('batch_commit', ?)",
                (f"batch wrote {executed} slot bodies (next op #{next_op})",),
            )

        if simulate_crash:
            # Deliberately exit BEFORE commit.  Nothing above called COMMIT, so
            # the rollback journal restores the pre-batch state on next open.
            # os._exit bypasses Flask exception handlers and finally blocks.
            os._exit(77)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {"executed": executed, "next_op": next_op, "done": done,
            "stripe_k": mig["stripe_k"]}


# --------------------------------------------------------------------------- #
# Logical I/O during migration
# --------------------------------------------------------------------------- #

def read_lbn(conn: sqlite3.Connection, lbn: int) -> dict:
    loc = conn.execute("SELECT * FROM locations WHERE lbn = ?", (lbn,)).fetchone()
    if loc is None:
        raise KeyError(f"unknown lbn {lbn}")
    if loc["staged"]:
        row = conn.execute("SELECT body FROM staging WHERE lbn = ?",
                           (lbn,)).fetchone()
        if row is None:
            raise RuntimeError(f"lbn {lbn} marked staged but staging row missing")
        return {"lbn": lbn, "disk": loc["disk"], "slot": loc["slot"],
                "staged": True, "body": row["body"]}
    row = conn.execute(
        "SELECT body FROM slots WHERE disk = ? AND slot = ?",
        (loc["disk"], loc["slot"]),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"lbn {lbn} location points at an empty slot")
    return {"lbn": lbn, "disk": loc["disk"], "slot": loc["slot"],
            "staged": False, "body": row["body"]}


def write_lbn(conn: sqlite3.Connection, lbn: int, body: bytes) -> dict:
    """Host-style overwrite; immediately committed, not counted in batch budget."""
    if len(body) != 4096:
        raise ValueError("block body must be exactly 4096 bytes")
    loc = conn.execute("SELECT * FROM locations WHERE lbn = ?", (lbn,)).fetchone()
    if loc is None:
        raise KeyError(f"unknown lbn {lbn}")
    conn.execute("BEGIN IMMEDIATE")
    try:
        if loc["staged"]:
            conn.execute("UPDATE staging SET body = ? WHERE lbn = ?", (body, lbn))
            where = "staging"
        else:
            conn.execute(
                "UPDATE slots SET body = ? WHERE disk = ? AND slot = ?",
                (body, loc["disk"], loc["slot"]),
            )
            where = f"disk{loc['disk']}.slot{loc['slot']}"
        # A successful full-block overwrite supersedes any prior trial bytes of
        # this block: its undo log is dropped and the quota is released.
        invalidated = trial.invalidate_lbn(conn, lbn)
        conn.execute(
            "INSERT INTO events(kind, detail) VALUES ('host_write', ?)",
            (f"lbn={lbn} overwrite committed at {where}"
             + (f"; {invalidated} trial position(s) superseded" if invalidated else ""),),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"lbn": lbn, "target": where, "committed": True,
            "trial_positions_invalidated": invalidated}


# --------------------------------------------------------------------------- #
# Verification / snapshot
# --------------------------------------------------------------------------- #

def verify(conn: sqlite3.Connection) -> dict:
    errors: list[str] = []
    mig = conn.execute("SELECT * FROM migration WHERE id = 1").fetchone()
    stripe_k = mig["stripe_k"] if mig else 1

    slot_rows = conn.execute("SELECT * FROM slots").fetchall()
    staged_count = conn.execute("SELECT COUNT(*) AS c FROM staging").fetchone()["c"]
    # Mid-migration a block may live in the single staging slot, so regular
    # slots + staging must always account for all 32 logical blocks exactly.
    if len(slot_rows) + staged_count != BLOCK_COUNT:
        errors.append(
            f"copies accounted: {len(slot_rows)} slots + {staged_count} staged "
            f"!= {BLOCK_COUNT}")
    lbns = [r["lbn"] for r in slot_rows]
    staged_lbns = [r["lbn"] for r in conn.execute("SELECT lbn FROM staging")]
    if sorted(lbns + staged_lbns) != list(range(BLOCK_COUNT)):
        errors.append("slots + staging do not hold exactly one copy of every lbn")
    for r in slot_rows:
        if len(r["body"]) != 4096:
            errors.append(f"lbn {r['lbn']} body is {len(r['body'])} bytes, not 4096")
    for r in conn.execute("SELECT lbn, length(body) AS n FROM staging"):
        if r["n"] != 4096:
            errors.append(f"staged lbn {r['lbn']} body is {r['n']} bytes, not 4096")

    if staged_count > 1:
        errors.append(f"staging holds {staged_count} blocks (limit is 1)")
    if mig is not None and mig["status"] == "done" and staged_count != 0:
        errors.append("migration done but staging slot not empty")
    # A staged block must be flagged in locations, and vice versa.
    flagged = {r["lbn"] for r in conn.execute(
        "SELECT lbn FROM locations WHERE staged = 1")}
    if flagged != set(staged_lbns):
        errors.append(f"staging flags mismatch: locations {sorted(flagged)} "
                      f"vs staging {sorted(staged_lbns)}")

    for lbn in range(BLOCK_COUNT):
        loc = conn.execute("SELECT * FROM locations WHERE lbn = ?",
                           (lbn,)).fetchone()
        if loc is None:
            errors.append(f"lbn {lbn} missing from locations")
            continue
        if mig is not None and mig["status"] == "done":
            exp_d, exp_s = new_position(lbn, stripe_k)
            if (loc["disk"], loc["slot"]) != (exp_d, exp_s) or loc["staged"]:
                errors.append(
                    f"lbn {lbn} at disk{loc['disk']}.slot{loc['slot']} "
                    f"staged={loc['staged']}, expected disk{exp_d}.slot{exp_s}"
                )

    # Trial-round invariants: counters match the undo log, distinct positions
    # stay within the 4096 quota, undo rows point at existing blocks/offsets,
    # and every undo byte is exactly one byte (stored payload <= 4 KiB).
    trow = conn.execute("SELECT * FROM trial_round WHERE id = 1").fetchone()
    distinct = conn.execute(
        "SELECT COUNT(*) AS c, COALESCE(SUM(length(orig_byte)), 0) AS b"
        " FROM trial_bytes"
    ).fetchone()
    if trow is None and distinct["c"]:
        errors.append("trial bytes exist without an open trial round")
    if trow is not None:
        if trow["undo_positions"] != distinct["c"]:
            errors.append(
                f"trial round counter {trow['undo_positions']} != "
                f"distinct undo rows {distinct['c']}")
        if distinct["c"] > 4096:
            errors.append(f"undoable positions {distinct['c']} exceed 4096 quota")
        if distinct["b"] > 4096:
            errors.append(f"stored undo payload {distinct['b']} B exceeds 4 KiB")
        bad = conn.execute(
            "SELECT COUNT(*) AS c FROM trial_bytes WHERE lbn NOT IN"
            " (SELECT lbn FROM locations) OR offset < 0 OR offset >= 4096"
        ).fetchone()["c"]
        if bad:
            errors.append(f"{bad} trial undo rows reference invalid lbn/offset")
    return {"ok": not errors, "errors": errors, "stripe_k": stripe_k}


def snapshot(conn: sqlite3.Connection) -> dict:
    mig = conn.execute("SELECT * FROM migration WHERE id = 1").fetchone()
    stripe_k = mig["stripe_k"] if mig else 1
    loc_rows = conn.execute("SELECT * FROM locations ORDER BY lbn").fetchall()
    trial_counts = {
        r["lbn"]: r["n"]
        for r in conn.execute(
            "SELECT lbn, COUNT(*) AS n FROM trial_bytes GROUP BY lbn")
    }
    blocks = []
    for loc in loc_rows:
        exp_d, exp_s = new_position(loc["lbn"], stripe_k)
        blocks.append({
            "lbn": loc["lbn"],
            "disk": loc["disk"],
            "slot": loc["slot"],
            "staged": bool(loc["staged"]),
            "expected_disk": exp_d,
            "expected_slot": exp_s,
            "at_target": (loc["disk"], loc["slot"], bool(loc["staged"]))
                          == (exp_d, exp_s, False),
            "trial_positions": trial_counts.get(loc["lbn"], 0),
        })
    staging = [dict(r) for r in conn.execute("SELECT lbn FROM staging")]
    events = [dict(r) for r in conn.execute(
        "SELECT id, ts, kind, detail FROM events ORDER BY id DESC LIMIT 25")]
    return {
        "migration": dict(mig) if mig else None,
        "stripe_k": stripe_k,
        "blocks": blocks,
        "staging": staging,
        "events": events,
        "verification": verify(conn),
        "trial": trial.round_snapshot(conn),
    }

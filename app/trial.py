"""Trial-write rounds for tentative metadata correction.

Design: in-place writes plus an undo byte log
------------------------------------------------
A trial patch is applied *in place* to the block's current body (a regular
disk slot, or the staging slot when the block is staged), so every normal read
path immediately returns the trial value.  Nothing extra is stored for the
trial value itself; ``trial_bytes`` only keeps the single original byte of
each distinct ``(lbn, offset)`` that currently carries a trial value.

Consequences that fall straight out of this design:

* Round quota of 4096 undoable byte positions: the primary key
  ``(lbn, offset)`` makes repeated trial writes to the same position count
  once.  Over-quota submissions are rejected *as a whole* (one transaction,
  rollback on breach, nothing applied).
* Separately stored trial/restore payload == undo bytes == number of distinct
  positions, hence hard-capped at 4 KiB.
* Confirm keeps the in-place bytes: zero body writes.  Revoke writes each
  affected block's body back at most once.  A successful full-block overwrite
  through the normal write path deletes that block's undo rows (the trial is
  superseded) and frees quota; a later trial on the block revokes back to the
  post-overwrite body because the undo byte was captured from that body.
* Every state change is one SQLite transaction (rollback journal +
  synchronous=FULL), so a failed confirm/revoke leaves the whole round
  untouched, and a round interrupted by a process restart can be continued,
  confirmed or revoked afterwards.
"""

from __future__ import annotations

import sqlite3

from .db import BLOCK_COUNT, BLOCK_SIZE, TRIAL_POSITION_LIMIT, TRIAL_STORE_BUDGET


class TrialError(ValueError):
    """Any rule violation of the trial-write protocol."""


# --------------------------------------------------------------------------- #
# Round lifecycle
# --------------------------------------------------------------------------- #

def get_round(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM trial_round WHERE id = 1").fetchone()


def open_round(conn: sqlite3.Connection) -> int:
    """Open the single round of this page session.

    A round left open by an earlier process is intentionally reusable: that is
    how an unfinished round is continued after restart.  It can only be closed
    by an explicit confirm/revoke.
    """
    existing = get_round(conn)
    if existing is not None:
        raise TrialError("已有一轮试写尚未结束，请先确认或撤销")
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "INSERT INTO trial_round(id, undo_positions, undo_bytes, last_action)"
            " VALUES (1, 0, 0, 'open')"
        )
        conn.execute(
            "INSERT INTO events(kind, detail) VALUES ('trial_open', ?)",
            ("trial-write round opened (4096 undoable positions / 4 KiB stored cap)",),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return 1


def _require_open_round(conn: sqlite3.Connection) -> sqlite3.Row:
    row = get_round(conn)
    if row is None:
        raise TrialError("当前没有进行中的试写轮次，请先开启一轮")
    return row


# --------------------------------------------------------------------------- #
# Patch handling
# --------------------------------------------------------------------------- #

def _parse_patches(raw) -> list[tuple[int, bytes]]:
    """Normalize the incoming patch list to (offset, bytes) pairs."""
    if not isinstance(raw, list) or not raw:
        raise TrialError("补丁必须是非空数组，每项含 offset 与 hex")
    parsed: list[tuple[int, bytes]] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise TrialError(f"补丁 #{i} 必须是对象")
        if "offset" not in item or "hex" not in item:
            raise TrialError(f"补丁 #{i} 必须同时包含 offset 和 hex")
        try:
            offset = int(item["offset"])
        except (TypeError, ValueError):
            raise TrialError(f"补丁 #{i} 的 offset 不是整数") from None
        if not 0 <= offset <= BLOCK_SIZE - 1:
            raise TrialError(f"补丁 #{i} 的偏移 {offset} 超出 0..{BLOCK_SIZE - 1}")
        try:
            data = bytes.fromhex(str(item["hex"]))
        except ValueError:
            raise TrialError(f"补丁 #{i} 的 hex 不是合法十六进制") from None
        if not data:
            raise TrialError(f"补丁 #{i} 的 hex 为空，至少 1 字节")
        if offset + len(data) > BLOCK_SIZE:
            raise TrialError(
                f"补丁 #{i} 越过块尾：偏移 {offset} + {len(data)} 字节 > {BLOCK_SIZE}"
            )
        parsed.append((offset, data))
    return parsed


def _current_body(conn: sqlite3.Connection, lbn: int, loc: sqlite3.Row) -> bytes:
    if loc["staged"]:
        row = conn.execute("SELECT body FROM staging WHERE lbn = ?",
                           (lbn,)).fetchone()
        if row is None:
            raise TrialError(f"lbn {lbn} 标记为暂存但临时槽无正文")
        return row["body"]
    row = conn.execute(
        "SELECT body FROM slots WHERE disk = ? AND slot = ?",
        (loc["disk"], loc["slot"]),
    ).fetchone()
    if row is None:
        raise TrialError(f"lbn {lbn} 的盘位指向空槽")
    return row["body"]


def _write_body(conn: sqlite3.Connection, lbn: int, loc: sqlite3.Row,
                body: bytes) -> str:
    if loc["staged"]:
        conn.execute("UPDATE staging SET body = ? WHERE lbn = ?", (body, lbn))
        return "staging"
    conn.execute(
        "UPDATE slots SET body = ? WHERE disk = ? AND slot = ?",
        (body, loc["disk"], loc["slot"]),
    )
    return f"disk{loc['disk']}.slot{loc['slot']}"


def submit_patch(conn: sqlite3.Connection, lbn: int, raw_patches) -> dict:
    """Apply one patch submission to the open round, atomically.

    Repeated trial writes to an existing position consume no extra quota.  If
    the submission would push distinct undoable positions past 4096 (or stored
    undo payload past 4 KiB), the whole submission is rejected and nothing is
    changed.
    """
    if not isinstance(lbn, int) or not 0 <= lbn < BLOCK_COUNT:
        raise TrialError(f"lbn 必须在 0..{BLOCK_COUNT - 1} 之间")
    patches = _parse_patches(raw_patches)
    round_row = _require_open_round(conn)

    conn.execute("BEGIN IMMEDIATE")
    try:
        loc = conn.execute("SELECT * FROM locations WHERE lbn = ?",
                           (lbn,)).fetchone()
        if loc is None:
            raise TrialError(f"未知 lbn {lbn}")
        body = bytearray(_current_body(conn, lbn, loc))

        # Expand the patch over concrete byte positions; later entries win,
        # matching a stream of byte writes inside one submission.
        incoming: dict[int, int] = {}
        for offset, data in patches:
            for j, value in enumerate(data):
                incoming[offset + j] = value
        if not incoming:
            raise TrialError("补丁没有任何有效字节")

        existing = {
            r["offset"]: r["orig_byte"]
            for r in conn.execute(
                "SELECT offset, orig_byte FROM trial_bytes WHERE lbn = ?",
                (lbn,),
            )
        }
        new_positions = [off for off in incoming if off not in existing]
        projected = round_row["undo_positions"] + len(new_positions)
        if projected > TRIAL_POSITION_LIMIT:
            raise TrialError(
                f"整笔拒绝：本轮可撤销位置将达 {projected}，"
                f"超过 {TRIAL_POSITION_LIMIT} 个的额度（已用 "
                f"{round_row['undo_positions']}，本笔新位置 {len(new_positions)}）"
            )
        if projected > TRIAL_STORE_BUDGET:
            raise TrialError(
                f"整笔拒绝：另存试写/还原数据将达 {projected} 字节，"
                f"超过 {TRIAL_STORE_BUDGET} 字节（4 KiB）上限"
            )

        # Capture undo bytes from the current committed/trial body before the
        # first write to each new position.
        for off in new_positions:
            conn.execute(
                "INSERT INTO trial_bytes(lbn, offset, orig_byte) VALUES (?, ?, ?)",
                (lbn, off, bytes((body[off],))),
            )

        changed_bytes = 0
        for off, value in incoming.items():
            if body[off] != value:
                body[off] = value
                changed_bytes += 1
        target = _write_body(conn, lbn, loc, bytes(body))

        conn.execute(
            "UPDATE trial_round SET undo_positions = ?, undo_bytes = ?,"
            " last_action = 'patch' WHERE id = 1",
            (projected, projected),
        )
        conn.execute(
            "INSERT INTO events(kind, detail) VALUES ('trial_patch', ?)",
            (f"lbn={lbn} patch: {len(incoming)} byte position(s) "
             f"({len(new_positions)} new), {changed_bytes} byte value(s) "
             f"changed at {target}; round positions {projected}/"
             f"{TRIAL_POSITION_LIMIT}",),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "lbn": lbn,
        "target": target,
        "byte_positions": len(incoming),
        "new_positions": len(new_positions),
        "changed_bytes": changed_bytes,
        "round_positions": projected,
        "position_limit": TRIAL_POSITION_LIMIT,
        "stored_bytes": projected,
        "stored_budget": TRIAL_STORE_BUDGET,
    }


# --------------------------------------------------------------------------- #
# Interaction with full-block overwrites
# --------------------------------------------------------------------------- #

def invalidate_lbn(conn: sqlite3.Connection, lbn: int) -> int:
    """Drop trial state of a block replaced by a successful full-block write.

    Must be called inside the caller's open transaction.  Frees the quota
    occupied by the block; a subsequent trial on the same block captures undo
    bytes from the new committed body, so revocation restores the post-
    overwrite text.
    """
    cur = conn.execute("DELETE FROM trial_bytes WHERE lbn = ?", (lbn,))
    removed = cur.rowcount
    if removed:
        conn.execute(
            "UPDATE trial_round SET undo_positions = "
            "(SELECT COUNT(*) FROM trial_bytes),"
            " undo_bytes = (SELECT COUNT(*) FROM trial_bytes),"
            " last_action = 'invalidate' WHERE id = 1"
        )
        conn.execute(
            "INSERT INTO events(kind, detail) VALUES ('trial_invalidate', ?)",
            (f"lbn={lbn} full-block overwrite committed; {removed} prior "
             f"trial position(s) superseded and quota released",),
        )
    return removed


# --------------------------------------------------------------------------- #
# Confirm / revoke
# --------------------------------------------------------------------------- #

def finalize(conn: sqlite3.Connection, confirm: bool) -> dict:
    """Close the round.

    Only blocks that still carry trial bytes are touched, and each block body
    is written at most once.  Confirm writes nothing (trial bytes already live
    in place); revoke restores the captured original bytes in one write per
    affected block.  Any failure rolls back the entire transaction and leaves
    the round open and unchanged.
    """
    round_row = _require_open_round(conn)
    action = "confirm" if confirm else "revoke"
    conn.execute("BEGIN IMMEDIATE")
    try:
        affected = [
            r["lbn"] for r in conn.execute(
                "SELECT DISTINCT lbn FROM trial_bytes ORDER BY lbn"
            )
        ]
        disk_writes = 0
        staging_writes = 0
        block_writes = 0
        for lbn in affected:
            loc = conn.execute("SELECT * FROM locations WHERE lbn = ?",
                               (lbn,)).fetchone()
            if loc is None:
                raise TrialError(f"lbn {lbn} 已不在卷中，无法{action}")
            if confirm:
                # Nothing to write: the in-place trial bytes are the result.
                continue
            body = bytearray(_current_body(conn, lbn, loc))
            rows = conn.execute(
                "SELECT offset, orig_byte FROM trial_bytes WHERE lbn = ?",
                (lbn,),
            ).fetchall()
            for r in rows:
                body[r["offset"]] = r["orig_byte"][0]
            target = _write_body(conn, lbn, loc, bytes(body))
            block_writes += 1
            if target == "staging":
                staging_writes += 1
            else:
                disk_writes += 1

        restored = 0 if confirm else round_row["undo_positions"]
        conn.execute("DELETE FROM trial_bytes")
        conn.execute("DELETE FROM trial_round WHERE id = 1")
        conn.execute("DELETE FROM trial_last WHERE id = 1")
        conn.execute(
            "INSERT INTO trial_last(id, action, affected_blocks, body_writes,"
            " disk_writes, staging_writes, positions)"
            " VALUES (1, ?, ?, ?, ?, ?, ?)",
            (action, len(affected), block_writes, disk_writes,
             staging_writes, round_row["undo_positions"]),
        )
        conn.execute(
            "INSERT INTO events(kind, detail) VALUES (?, ?)",
            (f"trial_{action}",
             (f"round {action}ed: {len(affected)} block(s) with trial bytes, "
              f"{block_writes} body write(s) (disk slots {disk_writes}, "
              f"staging {staging_writes}), {restored} byte position(s) "
              f"{'kept' if confirm else 'restored'}"),),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "action": action,
        "affected_blocks": len(affected),
        "body_writes": block_writes,
        "disk_slot_writes": disk_writes,
        "staging_writes": staging_writes,
        "positions": round_row["undo_positions"],
    }


# --------------------------------------------------------------------------- #
# Snapshot
# --------------------------------------------------------------------------- #

def last_finalize(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute("SELECT * FROM trial_last WHERE id = 1").fetchone()
    if row is None:
        return None
    return dict(row)


def round_snapshot(conn: sqlite3.Connection) -> dict:
    """Trial state for the console: open round (if any) plus last close-out."""
    row = get_round(conn)
    open_round = None
    if row is not None:
        per_block = [
            {"lbn": r["lbn"], "positions": r["n"]}
            for r in conn.execute(
                "SELECT lbn, COUNT(*) AS n FROM trial_bytes "
                "GROUP BY lbn ORDER BY lbn"
            )
        ]
        open_round = {
            "open": True,
            "created_ts": row["created_ts"],
            "undo_positions": row["undo_positions"],
            "position_limit": TRIAL_POSITION_LIMIT,
            "stored_bytes": row["undo_bytes"],
            "stored_budget_bytes": TRIAL_STORE_BUDGET,
            "last_action": row["last_action"],
            "blocks": per_block,
        }
    return {"round": open_round, "last_finalize": last_finalize(conn)}

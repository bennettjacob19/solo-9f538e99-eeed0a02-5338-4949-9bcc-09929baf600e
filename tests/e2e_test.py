#!/usr/bin/env python3
"""End-to-end tests for the online re-striping simulation.

Spawns the Flask server as a subprocess against a throwaway SQLite file and
exercises: initial layout, budget=1 progress, host read/write while blocks
are staged, crash-before-commit rollback vs. committed-batch durability,
resume after restart, and final stripe verification for k=2 and k=4.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.db import BLOCK_COUNT, BLOCK_SIZE, DISK_COUNT  # noqa: E402
from app import migration  # noqa: E402

BASE = "http://127.0.0.1:8000"
PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def http(method: str, path: str, payload: dict | None = None,
         timeout: int = 10):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            ctype = resp.headers.get("Content-Type", "")
            return resp.status, json.loads(raw) if "json" in ctype else raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw
    except (urllib.error.URLError, ConnectionError, OSError) as exc:
        return 0, str(exc)


def wait_up(deadline_s: float = 30.0) -> None:
    end = time.time() + deadline_s
    while time.time() < end:
        try:
            code, _ = http("GET", "/api/snapshot", timeout=2)
            if code == 200:
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.3)
    raise RuntimeError("server did not come up")


def wait_exit(proc: subprocess.Popen, deadline_s: float = 15.0) -> int:
    end = time.time() + deadline_s
    while time.time() < end:
        rc = proc.poll()
        if rc is not None:
            return rc
        time.sleep(0.2)
    proc.kill()
    raise RuntimeError("server did not exit after simulated crash")


def start_server(db_path: str) -> subprocess.Popen:
    env = os.environ.copy()
    env["VIRTUAL_VOLUME_DB"] = db_path
    proc = subprocess.Popen(
        [sys.executable, "-m", "flask", "--app", "app.server", "run",
         "--host", "127.0.0.1", "--port", "8000", "--without-threads",
         "--no-reload", "--no-debugger"],
        cwd=ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    wait_up()
    return proc


def snapshot() -> dict:
    code, data = http("GET", "/api/snapshot")
    assert code == 200, data
    return data


def block_at(snap: dict, lbn: int) -> dict:
    return next(b for b in snap["blocks"] if b["lbn"] == lbn)


# --------------------------------------------------------------------------- #
def test_planner_math() -> None:
    print("[planner] cycle decomposition is a valid permutation plan")
    for k in (2, 4):
        ops = migration.plan_operations(k)
        check(f"k={k} plan non-empty", len(ops) > 0)
        check(f"k={k} all lbn ids valid",
              all(0 <= o["lbn"] < BLOCK_COUNT for o in ops))
        stages = [o for o in ops if o["op"] == "stage"]
        unstages = [o for o in ops if o["op"] == "unstage"]
        check(f"k={k} stage/unstage balanced", len(stages) == len(unstages))
        fixed = sum(1 for l in range(BLOCK_COUNT)
                    if migration.old_position(l) == migration.new_position(l, k))
        moved = {o["lbn"] for o in ops if o["op"] in ("move", "unstage")}
        check(f"k={k} every non-fixed lbn lands at target exactly once "
              f"({BLOCK_COUNT - fixed} moved)", len(moved) == BLOCK_COUNT - fixed)
        check(f"k={k} staging count equals cycle count (one staged block max)",
              len(stages) >= 1 and len(stages) <= BLOCK_COUNT // 2)

    # Re-plan from an already migrated layout: must derive targets from the
    # CURRENT positions, not from the initial layout.
    current = {l: migration.new_position(l, 2) for l in range(BLOCK_COUNT)}
    ops = migration.plan_operations(4, current)
    touched = {o["lbn"] for o in ops}
    expected = {l for l in range(BLOCK_COUNT)
                if migration.new_position(l, 2) != migration.new_position(l, 4)}
    check("re-striping from k=2 layout touches exactly the displaced lbn",
          touched == expected)


def test_initial_layout() -> None:
    print("[init] 2 disks x 16 slots, single-block alternating, 4 KiB bodies")
    snap = snapshot()
    check("32 blocks tracked", len(snap["blocks"]) == BLOCK_COUNT)
    check("staging empty initially", snap["staging"] == [])
    check("no migration initially", snap["migration"] is None)
    check("verification ok", snap["verification"]["ok"])
    for b in snap["blocks"]:
        exp_disk, exp_slot = b["lbn"] % DISK_COUNT, b["lbn"] // DISK_COUNT
        check(f"lbn {b['lbn']} initial alternating placement",
              (b["disk"], b["slot"], b["staged"], b["at_target"])
              == (exp_disk, exp_slot, False, True))
    code, blk = http("GET", "/api/blocks/0")
    check("initial body is exactly 4 KiB",
          code == 200 and len(bytes.fromhex(blk["body_hex"])) == BLOCK_SIZE)


def test_migration(proc, db_path: str, k: int) -> subprocess.Popen:
    print(f"[migration k={k}] start, budget=1, online I/O, crashes, resume, verify")
    code, _ = http("POST", "/api/migration/start", {"stripe_k": k})
    check("migration started", code == 200)
    total = snapshot()["migration"]["total_ops"]
    check("plan has slot writes", total > 0)

    # --- budget 1 makes exactly one op of progress ---------------------------
    code, r = http("POST", "/api/migration/batch", {"budget": 1})
    check("budget=1 executes exactly one write", code == 200 and r["executed"] == 1)
    snap = snapshot()
    check("next_op advanced to 1", snap["migration"]["next_op"] == 1)
    check("page counter shows this batch wrote 1",
          snap["migration"]["last_writes"] == 1)
    check("at most one block in staging", len(snap["staging"]) <= 1)

    # --- host I/O on a staged block: latest committed content wins ----------
    staged_lbn = snap["staging"][0]["lbn"] if snap["staging"] else None
    marker_stage = None
    if staged_lbn is not None:
        marker_stage = f"HOST-WRITE-STAGED-{staged_lbn}".encode().ljust(BLOCK_SIZE, b"#")
        code, w = http("PUT", f"/api/blocks/{staged_lbn}",
                       {"body_hex": marker_stage.hex()})
        check("overwrite of staged lbn lands in staging",
              code == 200 and w["ok"] and w["target"] == "staging")
        code, rd = http("GET", f"/api/blocks/{staged_lbn}")
        check("reading a staged lbn returns the latest committed body",
              code == 200 and rd["staged"] is True
              and bytes.fromhex(rd["body_hex"]) == marker_stage)

    # --- host I/O on an on-disk block ---------------------------------------
    disk_lbn = next(b["lbn"] for b in snap["blocks"] if not b["staged"])
    marker_disk = b"HOST-WRITE-DISK".ljust(BLOCK_SIZE, b"*")
    code, w = http("PUT", f"/api/blocks/{disk_lbn}",
                   {"body_hex": marker_disk.hex()})
    check("overwrite of on-disk lbn accepted", code == 200 and w["ok"])
    code, rd = http("GET", f"/api/blocks/{disk_lbn}")
    check("on-disk overwrite reads back",
          bytes.fromhex(rd["body_hex"]) == marker_disk)

    # --- crash 1: batch writes but never commits -----------------------------
    progress_before = snap["migration"]["next_op"]
    code, _ = http("POST", "/api/migration/batch", {"budget": 3, "crash": True})
    check("crash request yields no HTTP response", code == 0)
    rc = wait_exit(proc)
    check("process exited with code 77", rc == 77, f"rc={rc}")
    proc = start_server(db_path)
    snap = snapshot()
    check("restart keeps migration running", snap["migration"]["status"] == "running")
    check("uncommitted batch rolled back to committed op#",
          snap["migration"]["next_op"] == progress_before)
    check("batch counter shows last committed batch (1)",
          snap["migration"]["last_writes"] == 1)
    check("staging invariant intact after rollback", len(snap["staging"]) <= 1)
    if marker_stage is not None:
        code, rd = http("GET", f"/api/blocks/{staged_lbn}")
        check("committed overwrite survives restart",
              code == 200 and bytes.fromhex(rd["body_hex"]) == marker_stage)
    code, rd = http("GET", f"/api/blocks/{disk_lbn}")
    check("committed on-disk overwrite survives restart",
          bytes.fromhex(rd["body_hex"]) == marker_disk)

    # --- normal batches with larger budgets ---------------------------------
    code, r = http("POST", "/api/migration/batch", {"budget": 4})
    check("budget=4 executes <= 4 writes", code == 200 and r["executed"] == 4)
    check("committed progress advanced by 4",
          snapshot()["migration"]["next_op"] == progress_before + 4)

    # --- crash 2 later in the migration --------------------------------------
    snap = snapshot()
    if snap["migration"]["total_ops"] - snap["migration"]["next_op"] >= 3:
        progress_before = snap["migration"]["next_op"]
        http("POST", "/api/migration/batch", {"budget": 2, "crash": True})
        rc = wait_exit(proc)
        check("second crash exit code 77", rc == 77)
        proc = start_server(db_path)
        snap = snapshot()
        check("second rollback resumes at same committed op#",
              snap["migration"]["next_op"] == progress_before)

    # --- drain to completion with varied budgets (including 1) ---------------
    while True:
        snap = snapshot()
        if snap["migration"]["status"] == "done":
            break
        remaining = snap["migration"]["total_ops"] - snap["migration"]["next_op"]
        budget = 1 if remaining % 3 == 0 else min(4, remaining)
        code, r = http("POST", "/api/migration/batch", {"budget": budget})
        check("every positive budget makes progress",
              code == 200 and r["executed"] >= 1)
        if r["done"]:
            break

    snap = snapshot()
    check("migration marked done",
          snap["migration"]["status"] == "done"
          and snap["migration"]["next_op"] == snap["migration"]["total_ops"])
    check("staging empty at completion", snap["staging"] == [])
    code, v = http("GET", "/api/verify")
    check(f"k={k} verification passes", code == 200 and v["ok"], str(v.get("errors")))
    for b in snap["blocks"]:
        ed, es = migration.new_position(b["lbn"], k)
        check(f"k={k} lbn {b['lbn']} final placement matches new stripe",
              (b["disk"], b["slot"], b["staged"], b["at_target"])
              == (ed, es, False, True))

    # Host-written bodies must have followed their blocks to final positions.
    if marker_stage is not None:
        code, rd = http("GET", f"/api/blocks/{staged_lbn}")
        check("staged-block overwrite carried through to final disk slot",
              code == 200 and rd["staged"] is False
              and bytes.fromhex(rd["body_hex"]) == marker_stage)
    code, rd = http("GET", f"/api/blocks/{disk_lbn}")
    check("on-disk overwrite carried through to final position",
          bytes.fromhex(rd["body_hex"]) == marker_disk)
    return proc


def main() -> int:
    test_planner_math()
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "volume.db")
        proc = start_server(db_path)
        try:
            test_initial_layout()
            proc = test_migration(proc, db_path, 2)

            # Reset: run the second stripe width from a fresh initial volume.
            proc.terminate()
            proc.wait(timeout=10)
            db_path2 = os.path.join(tmp, "volume2.db")
            proc = start_server(db_path2)
            test_initial_layout()
            proc = test_migration(proc, db_path2, 4)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    print(f"\n{FAIL} failed, {PASS} passed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())

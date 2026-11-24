#!/usr/bin/env python3
"""End-to-end tests for trial-write rounds.

Covers every rule of the trial protocol over HTTP against a throwaway DB,
plus direct-engine checks for finalize failure atomicity and the 4 KiB stored
payload boundary:

* one open round at a time; unfinished round survives restart and can be
  continued / confirmed / revoked;
* patch by (lbn, byte offset) with hex payload; original read entry returns
  the trial value immediately, including for a block held in the staging slot;
* 4096 distinct undoable positions per round; repeated writes to the same
  position count once; over-quota (and malformed / out-of-range) submissions
  are rejected as a whole;
* separately stored trial/restore payload <= 4 KiB;
* successful full-block overwrite invalidates prior trial bytes of that
  block and frees quota; a later trial revokes back to the post-overwrite
  body;
* confirm touches no block body; revoke writes each affected block at most
  once and reports disk-slot vs staging writes;
* migration moves trial in-place bytes with their blocks;
* a failure inside confirm/revoke rolls the whole round back;
* verification invariants hold throughout.
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

from app.db import BLOCK_COUNT, BLOCK_SIZE  # noqa: E402
from app import db, migration, trial  # noqa: E402

BASE = "http://127.0.0.1:8001"
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


def http(method: str, path: str, payload: dict | None = None, timeout: int = 10):
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
        code, _ = http("GET", "/api/snapshot", timeout=2)
        if code == 200:
            return
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
         "--host", "127.0.0.1", "--port", "8001", "--without-threads",
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


def read_body(lbn: int) -> bytes:
    code, rd = http("GET", f"/api/blocks/{lbn}")
    assert code == 200, rd
    return bytes.fromhex(rd["body_hex"])


def block_info(lbn: int) -> dict:
    return next(b for b in snapshot()["blocks"] if b["lbn"] == lbn)


def patch(lbn: int, patches: list[dict]):
    return http("POST", "/api/trial/patch", {"lbn": lbn, "patches": patches})


def body_marker(tag: str) -> bytes:
    return tag.encode().ljust(BLOCK_SIZE, b"#")


# --------------------------------------------------------------------------- #
def test_round_lifecycle_and_reads() -> None:
    print("[trial] open / patch / immediate read / single open round")
    code, st = http("GET", "/api/trial")
    check("no round initially", code == 200 and st["round"] is None)

    code, err = patch(0, [{"offset": 0, "hex": "ab"}])
    check("patch without open round rejected", code == 400)

    code, r = http("POST", "/api/trial/open", {})
    check("round opened", code == 200 and r["ok"] and r["round"]["undo_positions"] == 0)
    code, _ = http("POST", "/api/trial/open", {})
    check("second open rejected while round active", code == 400)

    original = read_body(0)
    # Multi-byte patch and a second patch on overlapping/new positions.
    code, r = patch(0, [{"offset": 0, "hex": "deadbeef"},
                        {"offset": 100, "hex": "01ff"}])
    check("first patch accepted", code == 200 and r["ok"], str(r))
    check("6 distinct positions counted", r["round_positions"] == 6)
    check("stored payload equals distinct positions", r["stored_bytes"] == 6)

    body = read_body(0)
    check("original entry immediately reads trial value",
          body[:4] == bytes.fromhex("deadbeef") and body[100:102] == bytes.fromhex("01ff"))
    check("untouched bytes unchanged", body[4:100] == original[4:100])

    # Repeated trial write to the same position counts once; value updates.
    code, r = patch(0, [{"offset": 0, "hex": "cc"},
                        {"offset": 1, "hex": "ee"}])
    check("overlapping patch accepted", code == 200)
    check("same positions do not grow quota",
          r["round_positions"] == 6 and r["new_positions"] == 0)
    body = read_body(0)
    check("latest trial value wins at same position", body[0] == 0xCC and body[1] == 0xEE)

    # Bad inputs are whole-submission rejections.
    code, r = patch(0, [{"offset": 0, "hex": "zz"}])
    check("invalid hex rejected whole", code == 400)
    code, r = patch(0, [{"offset": BLOCK_SIZE - 1, "hex": "0102"}])
    check("patch crossing block end rejected", code == 400)
    code, r = patch(0, [{"offset": 99999, "hex": "01"}])
    check("out-of-range offset rejected", code == 400)
    code, r = patch(0, [])
    check("empty patch list rejected", code == 400)
    code, r = patch(99, [{"offset": 0, "hex": "01"}])
    check("bad lbn rejected", code == 400)
    body = read_body(0)
    check("rejected submissions changed nothing",
          body[0] == 0xCC and body[1] == 0xEE and body[2] == 0xBE)
    check("quota unchanged after rejections",
          snapshot()["trial"]["round"]["undo_positions"] == 6)


def test_quota_and_revoke_restore() -> None:
    print("[trial] 4096-position quota, whole rejection, revoke restores")
    # Fresh round: confirm the current one first (confirm = zero body writes).
    code, r = http("POST", "/api/trial/finalize", {"confirm": True})
    check("confirm closes round", code == 200 and r["ok"])
    check("confirm performs zero body writes",
          r["body_writes"] == 0 and r["disk_slot_writes"] == 0
          and r["staging_writes"] == 0)
    check("confirm affects only blocks with trial bytes",
          r["affected_blocks"] == 1)
    code, _ = http("POST", "/api/trial/finalize", {"confirm": True})
    check("finalize without open round rejected", code == 400)
    # Trial values stay in place after confirm.
    body = read_body(0)
    check("confirmed trial bytes persist", body[0] == 0xCC and body[100] == 0x01)

    http("POST", "/api/trial/open", {})
    lbn = 5
    original = read_body(lbn)
    # Fill exactly the 4096-position quota on one block.
    code, r = patch(lbn, [{"offset": 0, "hex": "ab" * BLOCK_SIZE}])
    check("full-block trial patch reaches quota exactly",
          code == 200 and r["round_positions"] == 4096
          and r["stored_bytes"] == 4096)
    check("stored payload hard-capped at 4 KiB", r["stored_bytes"] <= 4096)

    # One more distinct position must be rejected as a whole.
    probe = bytearray(read_body(3))
    code, r = patch(3, [{"offset": 7, "hex": "5a"}])
    check("over-quota submission rejected whole", code == 400, "4096 reached")
    check("rejection message states the limit", "4096" in r["error"])
    check("over-quota patch left no bytes behind", read_body(3) == bytes(probe))
    check("round quota still exactly 4096 after rejection",
          snapshot()["trial"]["round"]["undo_positions"] == 4096)

    # Writing an already-trial position still succeeds (no new quota).
    code, r = patch(lbn, [{"offset": 10, "hex": "77"}])
    check("repeat position accepted at full quota", code == 200
          and r["round_positions"] == 4096)

    # Revoke restores the original bytes with one body write for this block.
    code, r = http("POST", "/api/trial/finalize", {"confirm": False})
    check("revoke closes round", code == 200 and r["ok"])
    check("revoke writes affected block exactly once",
          r["body_writes"] == 1 and r["disk_slot_writes"] == 1
          and r["staging_writes"] == 0 and r["affected_blocks"] == 1)
    check("revoke restores every byte", read_body(lbn) == original)
    check("round gone after revoke", snapshot()["trial"]["round"] is None)
    lf = snapshot()["trial"]["last_finalize"]
    check("last finalize stats retained for console",
          lf["action"] == "revoke" and lf["body_writes"] == 1
          and lf["disk_writes"] == 1 and lf["positions"] == 4096)


def test_full_overwrite_invalidation() -> None:
    print("[trial] full-block overwrite invalidates prior trial, frees quota")
    http("POST", "/api/trial/open", {})
    lbn = 7
    original = read_body(lbn)
    code, r = patch(lbn, [{"offset": 0, "hex": "1122"}, {"offset": 50, "hex": "33"}])
    check("patch on lbn 7 accepted", code == 200 and r["round_positions"] == 3)

    marker = body_marker("FULL-OVERWRITE-LBN7")
    code, w = http("PUT", f"/api/blocks/{lbn}", {"body_hex": marker.hex()})
    check("full-block overwrite committed", code == 200 and w["ok"])
    check("overwrite reports superseded trial positions",
          w["trial_positions_invalidated"] == 3)
    check("overwrite frees quota",
          snapshot()["trial"]["round"]["undo_positions"] == 0)
    check("read returns the new committed body", read_body(lbn) == marker)

    # A new trial on the same block revokes back to the post-overwrite body,
    # not the pre-trial original.
    code, r = patch(lbn, [{"offset": 0, "hex": "99"}])
    check("re-trial after overwrite accepted", code == 200 and r["round_positions"] == 1)
    code, r = http("POST", "/api/trial/finalize", {"confirm": False})
    check("revoke after overwrite restores post-overwrite text",
          code == 200 and read_body(lbn) == marker)
    check("pre-trial original is not resurrected", read_body(lbn) != original)
    check("revoke touched only the re-trialed block", r["affected_blocks"] == 1)


def test_multi_block_revoke_counts() -> None:
    print("[trial] multi-block revoke writes each block body at most once")
    http("POST", "/api/trial/open", {})
    originals = {lbn: read_body(lbn) for lbn in (2, 4, 6)}
    for lbn in (2, 4, 6):
        code, _ = patch(lbn, [{"offset": lbn, "hex": "ab"},
                              {"offset": lbn + 10, "hex": "cd"}])
        check(f"patch lbn {lbn}", code == 200)
    code, r = http("POST", "/api/trial/finalize", {"confirm": False})
    check("three affected blocks, three single body writes",
          r["affected_blocks"] == 3 and r["body_writes"] == 3
          and r["disk_slot_writes"] == 3)
    for lbn, original in originals.items():
        check(f"lbn {lbn} restored", read_body(lbn) == original)


def test_staged_trial_and_migration(proc, db_path: str) -> subprocess.Popen:
    print("[trial] trial on a staged block and coexistence with re-striping")
    http("POST", "/api/trial/open", {})
    code, _ = http("POST", "/api/migration/start", {"stripe_k": 2})
    check("migration started with open trial round", code == 200)

    # Advance until some block is in the staging slot.
    staged_lbn = None
    for _ in range(6):
        http("POST", "/api/migration/batch", {"budget": 1})
        snap = snapshot()
        if snap["staging"]:
            staged_lbn = snap["staging"][0]["lbn"]
            break
    check("a block is now staged", staged_lbn is not None)
    original_staged = read_body(staged_lbn)

    code, r = patch(staged_lbn, [{"offset": 0, "hex": "5aa5"},
                                 {"offset": 20, "hex": "ff"}])
    check("trial patch accepted on staged block", code == 200, str(r))
    body = read_body(staged_lbn)
    check("staged block read via staging entry returns trial bytes",
          block_info(staged_lbn)["staged"]
          and body[:2] == bytes.fromhex("5aa5") and body[20] == 0xFF)

    # Full-block overwrite of the staged block also invalidates its trial.
    marker = body_marker(f"STAGED-OVERWRITE-{staged_lbn}")
    code, w = http("PUT", f"/api/blocks/{staged_lbn}", {"body_hex": marker.hex()})
    check("overwrite of staged block lands in staging",
          code == 200 and w["target"] == "staging"
          and w["trial_positions_invalidated"] == 3)
    check("staged read returns overwrite", read_body(staged_lbn) == marker)

    # A second staged trial, then let the migration carry the block to a disk
    # slot: in-place bytes must travel with the block.
    code, _ = patch(staged_lbn, [{"offset": 1, "hex": "7e"}])
    check("second staged trial accepted", code == 200)
    while True:
        snap = snapshot()
        blk = next(b for b in snap["blocks"] if b["lbn"] == staged_lbn)
        if not blk["staged"]:
            break
        code, r = http("POST", "/api/migration/batch", {"budget": 1})
        if code != 200:
            break
    body = read_body(staged_lbn)
    check("trial/overwrite bytes followed block onto disk slot",
          body == marker[:1] + bytes([0x7E]) + marker[2:])

    # Drain the migration; trial round stays open and coherent throughout.
    while snapshot()["migration"]["status"] != "done":
        code, r = http("POST", "/api/migration/batch", {"budget": 8})
        if code != 200 or r["done"]:
            break
    check("migration completed while trial round open",
          snapshot()["migration"]["status"] == "done")
    code, v = http("GET", "/api/verify")
    check("verification passes with open trial round", code == 200 and v["ok"],
          str(v.get("errors")))

    # Revoke now restores the staging-originated trial against the disk body.
    code, r = http("POST", "/api/trial/finalize", {"confirm": False})
    check("post-migration revoke writes the disk slot once",
          code == 200 and r["staging_writes"] == 0
          and r["disk_slot_writes"] == r["affected_blocks"])
    check("revoke restores marker (post-overwrite body) on disk",
          read_body(staged_lbn) == marker)
    check("original pre-trial bytes not resurrected",
          read_body(staged_lbn) != original_staged)
    code, v = http("GET", "/api/verify")
    check("verification passes after revoke", v["ok"], str(v.get("errors")))
    return proc


def test_restart_resumes_round(proc, db_path: str) -> subprocess.Popen:
    print("[trial] unfinished round survives crash/restart and can resume")
    http("POST", "/api/trial/open", {})
    lbn = 9
    original = read_body(lbn)
    code, _ = patch(lbn, [{"offset": 3, "hex": "4243"}, {"offset": 9, "hex": "44"}])
    check("patch before restart", code == 200)
    snap = snapshot()
    positions = snap["trial"]["round"]["undo_positions"]

    # Kill the process abruptly (no HTTP crash hook needed for trial state;
    # every trial statement already commits, so committed trial bytes persist).
    proc.terminate()
    proc.wait(timeout=10)
    proc = start_server(db_path)
    snap = snapshot()
    check("round still open after restart", snap["trial"]["round"] is not None)
    check("quota survives restart",
          snap["trial"]["round"]["undo_positions"] == positions)
    body = read_body(lbn)
    check("trial bytes survive restart", body[3] == 0x42 and body[4] == 0x43
          and body[9] == 0x44)

    # Continue the round with another patch, then revoke.
    code, r = patch(lbn, [{"offset": 3, "hex": "99"}])
    check("round can be continued after restart", code == 200
          and r["new_positions"] == 0)
    code, r = http("POST", "/api/trial/finalize", {"confirm": False})
    check("round revoked after restart", code == 200)
    check("full original restored after restart-revoke", read_body(lbn) == original)

    # Also verify an open round survives the migration crash-before-commit.
    http("POST", "/api/trial/open", {})
    code, _ = patch(11, [{"offset": 0, "hex": "babe"}])
    check("patch in round that will cross a crash", code == 200)
    http("POST", "/api/migration/start", {"stripe_k": 4})
    progress_before = snapshot()["migration"]["next_op"]
    http("POST", "/api/migration/batch", {"budget": 2, "crash": True})
    rc = wait_exit(proc)
    check("simulated crash exit code 77", rc == 77)
    proc = start_server(db_path)
    snap = snapshot()
    check("trial round open across crash rollback",
          snap["trial"]["round"] is not None
          and snap["trial"]["round"]["undo_positions"] == 2)
    check("migration rolled back to last commit",
          snap["migration"]["next_op"] == progress_before)
    body = read_body(11)
    check("trial bytes intact across crash", body[:2] == bytes.fromhex("babe"))
    code, r = http("POST", "/api/trial/finalize", {"confirm": True})
    check("round confirmable after crash recovery", code == 200
          and r["body_writes"] == 0)
    return proc


def test_finalize_failure_atomicity() -> None:
    print("[trial] engine-level: finalize failure leaves whole round intact")
    with tempfile.TemporaryDirectory() as tmp:
        conn = db.connect(os.path.join(tmp, "engine.db"))
        db.ensure_seeded(conn)
        trial.open_round(conn)
        trial.submit_patch(conn, 1, [{"offset": 0, "hex": "abcd"},
                                    {"offset": 5, "hex": "ef"}])
        before = migration.read_lbn(conn, 1)["body"]
        counters = dict(trial.round_snapshot(conn)["round"])

        # Sabotage the revoke by dropping the locations row mid-transaction via
        # a trigger that raises when staging/slots would be updated: easier and
        # equally valid is to monkeypatch the body writer to fail on first use.
        original_write = trial._write_body
        calls = {"n": 0}

        def failing_write(c, lbn, loc, body):
            calls["n"] += 1
            raise RuntimeError("simulated slot write failure")

        trial._write_body = failing_write
        try:
            raised = False
            try:
                trial.finalize(conn, confirm=False)
            except RuntimeError as exc:
                raised = "simulated slot write failure" in str(exc)
            check("revoke propagates the write failure", raised)
        finally:
            trial._write_body = original_write

        check("round still open after failed revoke",
              trial.get_round(conn) is not None)
        snap = trial.round_snapshot(conn)["round"]
        check("quota unchanged after failed revoke",
              snap["undo_positions"] == counters["undo_positions"])
        check("undo log intact after failed revoke",
              conn.execute("SELECT COUNT(*) c FROM trial_bytes").fetchone()["c"]
              == counters["undo_positions"])
        check("block body untouched after failed revoke",
              migration.read_lbn(conn, 1)["body"] == before)

        # A subsequent real revoke then succeeds (round still usable).
        r = trial.finalize(conn, confirm=False)
        check("retry revoke succeeds and restores bytes",
              r["body_writes"] == 1
              and migration.read_lbn(conn, 1)["body"][:2]
              == db.seed_body(1)[:2])
        conn.close()


def test_verify_invariants_and_reset() -> None:
    print("[trial] verification invariants and reset cleanup")
    http("POST", "/api/trial/open", {})
    code, _ = patch(13, [{"offset": 0, "hex": "12"}, {"offset": 1, "hex": "34"}])
    check("patch for invariant check", code == 200)
    code, v = http("GET", "/api/verify")
    check("verify ok while round open", code == 200 and v["ok"], str(v.get("errors")))
    req = urllib.request.Request(BASE + "/volume/reset", method="POST")
    with urllib.request.urlopen(req) as resp:
        check("reset accepted", resp.status in (200, 302))
    snap = snapshot()
    check("reset removes trial round", snap["trial"]["round"] is None)
    check("reset restores initial block body",
          read_body(13) == db.seed_body(13))
    code, v = http("GET", "/api/verify")
    check("verify ok after reset", v["ok"], str(v.get("errors")))


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "trial.db")
        proc = start_server(db_path)
        try:
            test_round_lifecycle_and_reads()
            test_quota_and_revoke_restore()
            test_full_overwrite_invalidation()
            test_multi_block_revoke_counts()
            proc = test_staged_trial_and_migration(proc, db_path)
            proc = test_restart_resumes_round(proc, db_path)
            test_verify_invariants_and_reset()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
    test_finalize_failure_atomicity()

    print(f"\n{FAIL} failed, {PASS} passed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())

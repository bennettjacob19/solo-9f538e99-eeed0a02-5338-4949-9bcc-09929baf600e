"""Flask operation console for the re-striping simulation."""

from __future__ import annotations

import sqlite3

from flask import (Flask, abort, flash, jsonify, redirect, render_template,
                   request, url_for)

from . import db, migration, trial

BLOCK_SIZE = db.BLOCK_SIZE


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = "virtual-volume-striping-console"

    def get_conn() -> sqlite3.Connection:
        conn = db.connect(db_path)
        db.ensure_seeded(conn)
        return conn

    def _parse_int(value, name: str, lo: int, hi: int) -> int:
        try:
            n = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{name} 必须是整数")
        if not lo <= n <= hi:
            raise ValueError(f"{name} 必须在 {lo}..{hi} 之间")
        return n

    def _normalize_body(text: str) -> bytes:
        body = text.encode("utf-8", errors="replace")
        if len(body) > BLOCK_SIZE:
            body = body[:BLOCK_SIZE]
        return body + b"\x00" * (BLOCK_SIZE - len(body))

    # ------------------------------------------------------------------ pages
    @app.get("/")
    def index():
        conn = get_conn()
        try:
            state = migration.snapshot(conn)
            # Which LBN *should* occupy each physical slot under the target
            # stripe (or the initial stripe when no migration is configured).
            target_at: dict[tuple[int, int], int] = {}
            for lbn in range(db.BLOCK_COUNT):
                td, ts = migration.new_position(lbn, state["stripe_k"])
                target_at[(td, ts)] = lbn
            return render_template("index.html", state=state, target_at=target_at)
        finally:
            conn.close()

    @app.post("/migration/start")
    def start():
        try:
            stripe_k = _parse_int(request.form.get("stripe_k"), "条带宽度", 2, 4)
            if stripe_k not in (2, 4):
                raise ValueError("条带宽度只能是 2 或 4")
            conn = get_conn()
            try:
                migration.start_migration(conn, stripe_k)
            finally:
                conn.close()
            flash(f"已启动换条带：新条带宽度 {stripe_k} 块", "ok")
        except Exception as exc:  # noqa: BLE001 - console feedback
            flash(str(exc), "err")
        return redirect(url_for("index"))

    @app.post("/migration/batch")
    def batch():
        try:
            budget = _parse_int(request.form.get("budget"), "本批最多写入次数", 1, 10000)
            crash = request.form.get("crash") == "1"
            conn = get_conn()
            try:
                result = migration.run_batch(conn, budget, simulate_crash=crash)
            finally:
                conn.close()
            flash(
                f"本批提交 {result['executed']} 次盘槽写入"
                + ("，搬运已全部完成" if result["done"] else f"，下一批从 op#{result['next_op']} 继续"),
                "ok",
            )
        except Exception as exc:  # noqa: BLE001
            flash(str(exc), "err")
        return redirect(url_for("index"))

    @app.post("/volume/read")
    def read_page():
        try:
            lbn = _parse_int(request.form.get("lbn"), "逻辑块号", 0, db.BLOCK_COUNT - 1)
            conn = get_conn()
            try:
                r = migration.read_lbn(conn, lbn)
            finally:
                conn.close()
            head = r["body"].rstrip(b"\x00")[:48]
            where = "临时槽" if r["staged"] else f"盘{r['disk']} 槽{r['slot']}"
            flash(
                f"读到 LBN {lbn}（位置：{where}），最近已提交正文前 48 字节：{head!r}",
                "ok",
            )
        except Exception as exc:  # noqa: BLE001
            flash(str(exc), "err")
        return redirect(url_for("index"))

    @app.post("/volume/write")
    def write_page():
        try:
            lbn = _parse_int(request.form.get("lbn"), "逻辑块号", 0, db.BLOCK_COUNT - 1)
            conn = get_conn()
            try:
                r = migration.write_lbn(conn, lbn, _normalize_body(request.form.get("body", "")))
            finally:
                conn.close()
            flash(f"LBN {lbn} 改写已提交（写入 {r['target']}）", "ok")
        except Exception as exc:  # noqa: BLE001
            flash(str(exc), "err")
        return redirect(url_for("index"))

    @app.post("/volume/reset")
    def reset():
        conn = get_conn()
        try:
            db.initialize(conn)
        finally:
            conn.close()
        flash("卷已重置为初始单块交替布局", "ok")
        return redirect(url_for("index"))

    # ------------------------------------------------------------- trial pages
    def _parse_patch_text(text: str) -> list[dict]:
        """Parse one ``offset:hex`` pair per line (blank/``#`` lines ignored)."""
        patches: list[dict] = []
        for lineno, raw_line in enumerate((text or "").splitlines(), 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                raise ValueError(f"第 {lineno} 行格式应为 偏移:十六进制")
            off_text, hex_text = line.split(":", 1)
            patches.append({"offset": off_text.strip(), "hex": hex_text.strip()})
        if not patches:
            raise ValueError("至少填写一行 偏移:十六进制 补丁")
        return patches

    @app.post("/trial/open")
    def trial_open_page():
        conn = get_conn()
        try:
            trial.open_round(conn)
        except Exception as exc:  # noqa: BLE001
            flash(str(exc), "err")
        else:
            flash("已开启一轮试写：同一时间只此一轮，确认或撤销后才能再开", "ok")
        finally:
            conn.close()
        return redirect(url_for("index"))

    @app.post("/trial/patch")
    def trial_patch_page():
        try:
            lbn = _parse_int(request.form.get("lbn"), "逻辑块号", 0, db.BLOCK_COUNT - 1)
            patches = _parse_patch_text(request.form.get("patches", ""))
            conn = get_conn()
            try:
                r = trial.submit_patch(conn, lbn, patches)
            finally:
                conn.close()
            flash(
                f"试写已落到 LBN {lbn}（{r['target']}），原入口即刻读到新值；"
                f"本笔 {r['byte_positions']} 个位置（新增 {r['new_positions']}），"
                f"本轮累计可撤销 {r['round_positions']}/{r['position_limit']} 个位置，"
                f"另存 {r['stored_bytes']}/{r['stored_budget']} 字节",
                "ok",
            )
        except Exception as exc:  # noqa: BLE001
            flash(f"试写整笔拒绝：{exc}", "err")
        return redirect(url_for("index"))

    @app.post("/trial/finalize")
    def trial_finalize_page():
        try:
            confirm = request.form.get("action") == "confirm"
            conn = get_conn()
            try:
                r = trial.finalize(conn, confirm=confirm)
            finally:
                conn.close()
            if confirm:
                flash(
                    f"已确认整轮：涉及 {r['affected_blocks']} 个块，"
                    f"盘槽正文写 0 次、临时槽正文写 0 次（试写值本就在正文中）",
                    "ok",
                )
            else:
                flash(
                    f"已撤销整轮：涉及 {r['affected_blocks']} 个块，"
                    f"正文实际写 {r['body_writes']} 次"
                    f"（盘槽 {r['disk_slot_writes']} 次、临时槽 {r['staging_writes']} 次），"
                    f"恢复 {r['positions']} 个字节位置",
                    "ok",
                )
        except Exception as exc:  # noqa: BLE001
            flash(f"收尾失败，整轮保持原状：{exc}", "err")
        return redirect(url_for("index"))

    # ------------------------------------------------------------- JSON / API
    @app.get("/api/snapshot")
    def api_snapshot():
        conn = get_conn()
        try:
            return jsonify(migration.snapshot(conn))
        finally:
            conn.close()

    @app.post("/api/migration/start")
    def api_start():
        data = request.get_json(silent=True) or {}
        try:
            stripe_k = int(data.get("stripe_k"))
            if stripe_k not in (2, 4):
                raise ValueError("stripe_k must be 2 or 4")
            conn = get_conn()
            try:
                migration.start_migration(conn, stripe_k)
            finally:
                conn.close()
            return jsonify(ok=True)
        except Exception as exc:  # noqa: BLE001
            return jsonify(ok=False, error=str(exc)), 400

    @app.post("/api/migration/batch")
    def api_batch():
        data = request.get_json(silent=True) or {}
        try:
            budget = int(data.get("budget", 1))
            if budget < 1:
                raise ValueError("budget must be >= 1")
            conn = get_conn()
            try:
                # On crash the process exits; no response will be produced.
                result = migration.run_batch(
                    conn, budget, simulate_crash=bool(data.get("crash"))
                )
            finally:
                conn.close()
            return jsonify(ok=True, **result)
        except Exception as exc:  # noqa: BLE001
            return jsonify(ok=False, error=str(exc)), 400

    @app.get("/api/blocks/<int:lbn>")
    def api_read(lbn: int):
        if not 0 <= lbn < db.BLOCK_COUNT:
            abort(404)
        conn = get_conn()
        try:
            r = migration.read_lbn(conn, lbn)
            return jsonify({
                "lbn": r["lbn"], "disk": r["disk"], "slot": r["slot"],
                "staged": r["staged"],
                "body_hex": r["body"].hex(),
                "body_preview": r["body"].rstrip(b"\x00")[:64].decode(
                    "utf-8", errors="replace"),
            })
        finally:
            conn.close()

    @app.put("/api/blocks/<int:lbn>")
    def api_write(lbn: int):
        if not 0 <= lbn < db.BLOCK_COUNT:
            abort(404)
        data = request.get_json(silent=True) or {}
        try:
            if "body_hex" in data:
                body = bytes.fromhex(data["body_hex"])
                if len(body) != BLOCK_SIZE:
                    raise ValueError("body_hex must decode to exactly 4096 bytes")
            else:
                body = _normalize_body(str(data.get("body", "")))
            conn = get_conn()
            try:
                r = migration.write_lbn(conn, lbn, body)
            finally:
                conn.close()
            return jsonify(ok=True, **r)
        except Exception as exc:  # noqa: BLE001
            return jsonify(ok=False, error=str(exc)), 400

    @app.get("/api/trial")
    def api_trial_status():
        conn = get_conn()
        try:
            return jsonify(trial.round_snapshot(conn))
        finally:
            conn.close()

    @app.post("/api/trial/open")
    def api_trial_open():
        conn = get_conn()
        try:
            trial.open_round(conn)
            return jsonify(ok=True, **trial.round_snapshot(conn))
        except Exception as exc:  # noqa: BLE001
            return jsonify(ok=False, error=str(exc)), 400
        finally:
            conn.close()

    @app.post("/api/trial/patch")
    def api_trial_patch():
        data = request.get_json(silent=True) or {}
        try:
            lbn = int(data.get("lbn"))
            if not 0 <= lbn < db.BLOCK_COUNT:
                raise ValueError("lbn out of range")
            patches = data.get("patches")
            conn = get_conn()
            try:
                r = trial.submit_patch(conn, lbn, patches)
            finally:
                conn.close()
            return jsonify(ok=True, **r)
        except Exception as exc:  # noqa: BLE001
            return jsonify(ok=False, error=str(exc)), 400

    @app.post("/api/trial/finalize")
    def api_trial_finalize():
        data = request.get_json(silent=True) or {}
        try:
            confirm = bool(data.get("confirm", True))
            conn = get_conn()
            try:
                r = trial.finalize(conn, confirm=confirm)
            finally:
                conn.close()
            return jsonify(ok=True, **r)
        except Exception as exc:  # noqa: BLE001
            return jsonify(ok=False, error=str(exc)), 400

    @app.get("/api/verify")
    def api_verify():
        conn = get_conn()
        try:
            return jsonify(migration.verify(conn))
        finally:
            conn.close()

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=False)

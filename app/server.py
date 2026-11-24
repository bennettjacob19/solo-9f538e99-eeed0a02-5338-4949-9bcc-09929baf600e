"""Flask operation console for the re-striping simulation."""

from __future__ import annotations

import sqlite3

from flask import (Flask, abort, flash, jsonify, redirect, render_template,
                   request, url_for)

from . import db, migration

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

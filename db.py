import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def utcnow() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


class DB:
    def __init__(self, path: str):
        self.path = path
        Path(os.path.dirname(path) or ".").mkdir(parents=True, exist_ok=True)
        self._init()

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON;")
        return con

    def _col_exists(self, con: sqlite3.Connection, table: str, col: str) -> bool:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r["name"] == col for r in rows)

    def _init(self) -> None:
        with self._conn() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    last_submission_id INTEGER,
                    last_folder_path TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS submissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,

                    org TEXT NOT NULL,
                    role TEXT NOT NULL,
                    name TEXT NOT NULL,

                    event_date TEXT NOT NULL,
                    event_title TEXT NOT NULL DEFAULT '',

                    scene TEXT NOT NULL,

                    night_mount TEXT NOT NULL,
                    mount_who TEXT NOT NULL,
                    techs_count TEXT NOT NULL,

                    extra_equipment TEXT NOT NULL,
                    plugs TEXT NOT NULL,

                    power_type TEXT NOT NULL,
                    power_count TEXT NOT NULL,
                    power_where_json TEXT NOT NULL,

                    dimmer_needed TEXT NOT NULL,
                    dimmer_text TEXT NOT NULL,

                    operator TEXT NOT NULL,
                    console_help TEXT NOT NULL,
                    console_model TEXT NOT NULL,

                    ydisk_folder TEXT NOT NULL,

                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_submissions_user_id ON submissions(user_id);
                CREATE INDEX IF NOT EXISTS idx_submissions_event_date ON submissions(event_date);

                CREATE TABLE IF NOT EXISTS docs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    submission_id INTEGER,
                    file_name TEXT NOT NULL,
                    ydisk_path TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(submission_id) REFERENCES submissions(id) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_docs_user_id ON docs(user_id);
                CREATE INDEX IF NOT EXISTS idx_docs_submission_id ON docs(submission_id);

                CREATE TABLE IF NOT EXISTS drafts (
                    user_id INTEGER PRIMARY KEY,
                    fsm_state TEXT NOT NULL,
                    draft_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

            # миграция старой БД: если нет event_title — добавим
            if not self._col_exists(con, "submissions", "event_title"):
                con.execute("ALTER TABLE submissions ADD COLUMN event_title TEXT NOT NULL DEFAULT ''")

    # -------- users last --------
    def upsert_user_last(self, user_id: int, submission_id: Optional[int], folder_path: Optional[str]) -> None:
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO users(user_id, last_submission_id, last_folder_path, updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                    last_submission_id=excluded.last_submission_id,
                    last_folder_path=excluded.last_folder_path,
                    updated_at=excluded.updated_at
                """,
                (user_id, submission_id, folder_path, utcnow()),
            )

    def get_user_last(self, user_id: int) -> Tuple[Optional[int], Optional[str]]:
        with self._conn() as con:
            r = con.execute(
                "SELECT last_submission_id, last_folder_path FROM users WHERE user_id=?",
                (user_id,),
            ).fetchone()
            if not r:
                return None, None
            return (r["last_submission_id"], r["last_folder_path"])

    # -------- drafts --------
    def upsert_draft(self, user_id: int, fsm_state: str, draft_json: str) -> None:
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO drafts(user_id, fsm_state, draft_json, updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                    fsm_state=excluded.fsm_state,
                    draft_json=excluded.draft_json,
                    updated_at=excluded.updated_at
                """,
                (user_id, fsm_state, draft_json, utcnow()),
            )

    def get_draft(self, user_id: int) -> Optional[sqlite3.Row]:
        with self._conn() as con:
            return con.execute(
                "SELECT * FROM drafts WHERE user_id=?",
                (user_id,),
            ).fetchone()

    def delete_draft(self, user_id: int) -> None:
        with self._conn() as con:
            con.execute("DELETE FROM drafts WHERE user_id=?", (user_id,))

    # -------- submissions --------
    def insert_submission(self, user_id: int, a: Dict[str, Any]) -> int:
        cols = [
            "org", "role", "name",
            "event_date", "event_title",
            "scene",
            "night_mount", "mount_who", "techs_count",
            "extra_equipment", "plugs",
            "power_type", "power_count", "power_where_json",
            "dimmer_needed", "dimmer_text",
            "operator", "console_help", "console_model",
            "ydisk_folder",
        ]
        now = utcnow()
        values = [a.get(c, "") for c in cols]
        with self._conn() as con:
            cur = con.execute(
                f"""
                INSERT INTO submissions (
                    user_id, {",".join(cols)}, created_at, updated_at
                ) VALUES (
                    ?, {",".join(["?"]*len(cols))}, ?, ?
                )
                """,
                [user_id, *values, now, now],
            )
            return int(cur.lastrowid)

    def update_submission(self, sub_id: int, patch: Dict[str, Any]) -> bool:
        if not patch:
            return False

        allowed = {
            "org","role","name",
            "event_date","event_title",
            "scene",
            "night_mount","mount_who","techs_count",
            "extra_equipment","plugs",
            "power_type","power_count","power_where_json",
            "dimmer_needed","dimmer_text",
            "operator","console_help","console_model",
            "ydisk_folder",
        }
        items = [(k, v) for k, v in patch.items() if k in allowed]
        if not items:
            return False

        sets = ", ".join([f"{k}=?" for k, _ in items] + ["updated_at=?"])
        values = [v for _, v in items] + [utcnow(), sub_id]

        with self._conn() as con:
            cur = con.execute(f"UPDATE submissions SET {sets} WHERE id=?", values)
            return cur.rowcount > 0

    def get_submission(self, sub_id: int) -> Optional[sqlite3.Row]:
        with self._conn() as con:
            return con.execute("SELECT * FROM submissions WHERE id=?", (sub_id,)).fetchone()

    def get_last_submission_by_user(self, user_id: int) -> Optional[sqlite3.Row]:
        with self._conn() as con:
            return con.execute(
                "SELECT * FROM submissions WHERE user_id=? ORDER BY id DESC LIMIT 1",
                (user_id,),
            ).fetchone()

    def list_submissions(self, limit: int = 10, offset: int = 0) -> List[sqlite3.Row]:
        with self._conn() as con:
            return con.execute(
                "SELECT * FROM submissions ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()

    def list_submissions_by_month(self, year: int, month: int, limit: int = 200) -> List[sqlite3.Row]:
        mm = f"{month:02d}"
        yyyy = str(year)
        # event_date хранится как DD.MM.YYYY -> берем подстроку 4..5 месяц, 7..10 год
        with self._conn() as con:
            return con.execute(
                """
                SELECT * FROM submissions
                WHERE substr(event_date, 4, 2)=? AND substr(event_date, 7, 4)=?
                ORDER BY event_date ASC, id ASC
                LIMIT ?
                """,
                (mm, yyyy, limit),
            ).fetchall()

    def delete_submission(self, sub_id: int) -> bool:
        with self._conn() as con:
            cur = con.execute("DELETE FROM submissions WHERE id=?", (sub_id,))
            return cur.rowcount > 0

    def count_submissions(self) -> int:
        with self._conn() as con:
            r = con.execute("SELECT COUNT(*) AS c FROM submissions").fetchone()
            return int(r["c"])

    # -------- docs --------
    def save_doc(self, user_id: int, submission_id: Optional[int], file_name: str, ydisk_path: str) -> int:
        with self._conn() as con:
            cur = con.execute(
                "INSERT INTO docs(user_id, submission_id, file_name, ydisk_path, created_at) VALUES(?,?,?,?,?)",
                (user_id, submission_id, file_name, ydisk_path, utcnow()),
            )
            return int(cur.lastrowid)

    def list_docs(self, user_id: int, limit: int = 30) -> List[sqlite3.Row]:
        with self._conn() as con:
            return con.execute(
                "SELECT * FROM docs WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
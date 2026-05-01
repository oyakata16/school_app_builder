# app_builder_streamlit.py
# 学校業務アプリ作成システム Streamlit版 V1
# 目的:
# - コードを書かずに、学校内の小さな業務アプリを作成する
# - 項目定義から入力フォーム・一覧・CSV・印刷・承認を自動生成する
#
# 実行:
#   streamlit run app_builder_streamlit.py
#
# Streamlit Cloud:
# - GitHubに app_builder_streamlit.py と requirements.txt を置く
# - Secrets に ADMIN_PASSWORD を設定すると管理職画面のパスワードになる

from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


APP_TITLE = "学校業務アプリ作成システム"
APP_VERSION = "Streamlit版 V1"
DB_PATH = Path(os.environ.get("APP_BUILDER_DB", "app_builder.db"))

FIELD_TYPES = {
    "text": "文字",
    "textarea": "長文",
    "number": "数値",
    "date": "日付",
    "select": "選択",
    "checkbox": "チェック",
}

STATUS_DRAFT = "下書き"
STATUS_SUBMITTED = "提出"
STATUS_APPROVED = "承認"
STATUS_RETURNED = "差戻"
STATUS_SAVED = "保存"

STATUS_ORDER = [STATUS_SUBMITTED, STATUS_RETURNED, STATUS_APPROVED, STATUS_SAVED, STATUS_DRAFT]


# -----------------------------
# 基本ユーティリティ
# -----------------------------

def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_text() -> str:
    return date.today().isoformat()


def safe_key(text: str, fallback: str = "field") -> str:
    """日本語ラベルから、内部用の英数字キーを作る。"""
    base = text.strip().lower()
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = fallback
    if base[0].isdigit():
        base = f"f_{base}"
    return base[:40]


def normalize_app_key(app_key: str) -> str:
    app_key = app_key.strip().upper()
    app_key = re.sub(r"[^A-Z0-9_\-]", "", app_key)
    return app_key[:30]


def parse_choices(raw: str) -> List[str]:
    if not raw:
        return []
    parts: List[str] = []
    for line in raw.replace("、", ",").split(","):
        item = line.strip()
        if item:
            parts.append(item)
    return parts


def json_dumps(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def json_loads(text: str) -> Dict[str, Any]:
    try:
        value = json.loads(text or "{}")
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        return {}


def get_secret_password() -> str:
    try:
        value = st.secrets.get("ADMIN_PASSWORD", "1234")
        return str(value) if value else "1234"
    except Exception:
        return "1234"


# -----------------------------
# DB
# -----------------------------

def connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS apps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                app_key TEXT NOT NULL UNIQUE,
                app_name TEXT NOT NULL,
                description TEXT DEFAULT '',
                requires_approval INTEGER DEFAULT 1,
                active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fields (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                app_key TEXT NOT NULL,
                field_key TEXT NOT NULL,
                label TEXT NOT NULL,
                field_type TEXT NOT NULL,
                required INTEGER DEFAULT 0,
                choices_csv TEXT DEFAULT '',
                display_order INTEGER DEFAULT 1,
                active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(app_key, field_key)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                app_key TEXT NOT NULL,
                data_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_by TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                submitted_at TEXT DEFAULT '',
                approved_at TEXT DEFAULT '',
                approved_by TEXT DEFAULT '',
                admin_comment TEXT DEFAULT ''
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        con.commit()


def seed_sample_data() -> None:
    """初回だけサンプルアプリを入れる。"""
    with connect() as con:
        count = con.execute("SELECT COUNT(*) AS c FROM apps").fetchone()["c"]
        if count > 0:
            return

        t = now_text()
        apps = [
            ("APP001", "備品貸出", "校内備品の貸出申請", 1),
            ("APP002", "出張申請", "出張・外出の申請", 1),
            ("APP003", "ヒヤリハット報告", "事故防止のための記録", 0),
        ]
        con.executemany(
            """
            INSERT INTO apps(app_key, app_name, description, requires_approval, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            [(a, b, c, d, t, t) for a, b, c, d in apps],
        )

        sample_fields = [
            ("APP001", "request_date", "申請日", "date", 1, "", 1),
            ("APP001", "teacher_name", "教員名", "text", 1, "", 2),
            ("APP001", "item_name", "備品名", "select", 1, "プロジェクター,書画カメラ,体育用具,タブレット,その他", 3),
            ("APP001", "use_date", "使用日", "date", 1, "", 4),
            ("APP001", "return_date", "返却予定日", "date", 0, "", 5),
            ("APP001", "note", "備考", "textarea", 0, "", 6),
            ("APP002", "trip_date", "出張日", "date", 1, "", 1),
            ("APP002", "teacher_name", "教員名", "text", 1, "", 2),
            ("APP002", "destination", "行き先", "text", 1, "", 3),
            ("APP002", "purpose", "用務", "textarea", 1, "", 4),
            ("APP002", "transport", "交通手段", "select", 1, "電車,バス,徒歩,自家用車,その他", 5),
            ("APP002", "note", "備考", "textarea", 0, "", 6),
            ("APP003", "occurred_date", "発生日", "date", 1, "", 1),
            ("APP003", "place", "場所", "text", 1, "", 2),
            ("APP003", "grade", "学年", "select", 0, "1年,2年,3年,4年,5年,6年,全校,その他", 3),
            ("APP003", "content", "内容", "textarea", 1, "", 4),
            ("APP003", "response", "対応", "textarea", 1, "", 5),
            ("APP003", "prevention", "今後の防止策", "textarea", 0, "", 6),
        ]
        con.executemany(
            """
            INSERT INTO fields(app_key, field_key, label, field_type, required, choices_csv,
                               display_order, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            [(a, b, c, d, e, f, g, t, t) for a, b, c, d, e, f, g in sample_fields],
        )
        con.commit()


def get_apps(active_only: bool = True) -> List[sqlite3.Row]:
    sql = "SELECT * FROM apps"
    params: Tuple[Any, ...] = ()
    if active_only:
        sql += " WHERE active = 1"
    sql += " ORDER BY app_key"
    with connect() as con:
        return list(con.execute(sql, params).fetchall())


def get_app(app_key: str) -> Optional[sqlite3.Row]:
    with connect() as con:
        return con.execute("SELECT * FROM apps WHERE app_key = ?", (app_key,)).fetchone()


def get_fields(app_key: str, active_only: bool = True) -> List[sqlite3.Row]:
    sql = "SELECT * FROM fields WHERE app_key = ?"
    params: List[Any] = [app_key]
    if active_only:
        sql += " AND active = 1"
    sql += " ORDER BY display_order, id"
    with connect() as con:
        return list(con.execute(sql, params).fetchall())


def next_app_key() -> str:
    with connect() as con:
        rows = con.execute("SELECT app_key FROM apps WHERE app_key LIKE 'APP%' ORDER BY app_key").fetchall()
    nums = []
    for r in rows:
        m = re.match(r"APP(\d+)$", r["app_key"])
        if m:
            nums.append(int(m.group(1)))
    n = (max(nums) + 1) if nums else 1
    return f"APP{n:03d}"


def create_app(app_key: str, app_name: str, description: str, requires_approval: bool) -> Tuple[bool, str]:
    app_key = normalize_app_key(app_key)
    if not app_key:
        return False, "アプリIDが空です。"
    if not app_name.strip():
        return False, "アプリ名が空です。"
    try:
        t = now_text()
        with connect() as con:
            con.execute(
                """
                INSERT INTO apps(app_key, app_name, description, requires_approval, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                """,
                (app_key, app_name.strip(), description.strip(), 1 if requires_approval else 0, t, t),
            )
            con.commit()
        return True, f"{app_key} を作成しました。"
    except sqlite3.IntegrityError:
        return False, "同じアプリIDがすでにあります。"


def update_app(app_key: str, app_name: str, description: str, requires_approval: bool, active: bool) -> None:
    with connect() as con:
        con.execute(
            """
            UPDATE apps
               SET app_name = ?, description = ?, requires_approval = ?, active = ?, updated_at = ?
             WHERE app_key = ?
            """,
            (app_name.strip(), description.strip(), 1 if requires_approval else 0, 1 if active else 0, now_text(), app_key),
        )
        con.commit()


def create_field(
    app_key: str,
    label: str,
    field_type: str,
    required: bool,
    choices_csv: str,
    display_order: int,
    field_key: str = "",
) -> Tuple[bool, str]:
    label = label.strip()
    if not label:
        return False, "項目名が空です。"
    if field_type not in FIELD_TYPES:
        return False, "入力形式が不正です。"
    if field_type == "select" and not parse_choices(choices_csv):
        return False, "選択形式では選択肢が必要です。"

    base_key = safe_key(field_key or label, fallback="field")
    with connect() as con:
        existing = {r["field_key"] for r in con.execute("SELECT field_key FROM fields WHERE app_key = ?", (app_key,)).fetchall()}
        new_key = base_key
        i = 2
        while new_key in existing:
            new_key = f"{base_key}_{i}"
            i += 1
        t = now_text()
        con.execute(
            """
            INSERT INTO fields(app_key, field_key, label, field_type, required, choices_csv,
                               display_order, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (app_key, new_key, label, field_type, 1 if required else 0, choices_csv.strip(), display_order, t, t),
        )
        con.commit()
    return True, f"項目「{label}」を追加しました。"


def update_field(field_id: int, label: str, field_type: str, required: bool, choices_csv: str, display_order: int, active: bool) -> Tuple[bool, str]:
    label = label.strip()
    if not label:
        return False, "項目名が空です。"
    if field_type == "select" and not parse_choices(choices_csv):
        return False, "選択形式では選択肢が必要です。"
    with connect() as con:
        con.execute(
            """
            UPDATE fields
               SET label = ?, field_type = ?, required = ?, choices_csv = ?,
                   display_order = ?, active = ?, updated_at = ?
             WHERE id = ?
            """,
            (label, field_type, 1 if required else 0, choices_csv.strip(), display_order, 1 if active else 0, now_text(), field_id),
        )
        con.commit()
    return True, "項目を更新しました。"


def create_record(app_key: str, data: Dict[str, Any], status: str, created_by: str) -> int:
    t = now_text()
    submitted_at = t if status == STATUS_SUBMITTED else ""
    with connect() as con:
        cur = con.execute(
            """
            INSERT INTO records(app_key, data_json, status, created_by, created_at, updated_at, submitted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (app_key, json_dumps(data), status, created_by.strip(), t, t, submitted_at),
        )
        con.commit()
        return int(cur.lastrowid)


def update_record(record_id: int, data: Dict[str, Any], status: str) -> None:
    t = now_text()
    submitted_at = t if status == STATUS_SUBMITTED else ""
    with connect() as con:
        con.execute(
            """
            UPDATE records
               SET data_json = ?, status = ?, updated_at = ?, submitted_at = ?
             WHERE id = ?
            """,
            (json_dumps(data), status, t, submitted_at, record_id),
        )
        con.commit()


def get_record(record_id: int) -> Optional[sqlite3.Row]:
    with connect() as con:
        return con.execute("SELECT * FROM records WHERE id = ?", (record_id,)).fetchone()


def list_records(app_key: Optional[str] = None, status: Optional[str] = None, limit: int = 500) -> List[sqlite3.Row]:
    sql = "SELECT * FROM records WHERE 1 = 1"
    params: List[Any] = []
    if app_key:
        sql += " AND app_key = ?"
        params.append(app_key)
    if status and status != "すべて":
        sql += " AND status = ?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with connect() as con:
        return list(con.execute(sql, params).fetchall())


def set_record_status(record_id: int, status: str, approved_by: str, admin_comment: str) -> None:
    t = now_text()
    approved_at = t if status == STATUS_APPROVED else ""
    with connect() as con:
        con.execute(
            """
            UPDATE records
               SET status = ?, updated_at = ?, approved_at = ?, approved_by = ?, admin_comment = ?
             WHERE id = ?
            """,
            (status, t, approved_at, approved_by.strip(), admin_comment.strip(), record_id),
        )
        con.commit()


def records_to_dataframe(records: List[sqlite3.Row], apps_by_key: Dict[str, sqlite3.Row]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    fields_cache: Dict[str, List[sqlite3.Row]] = {}
    for r in records:
        app_key = r["app_key"]
        if app_key not in fields_cache:
            fields_cache[app_key] = get_fields(app_key)
        data = json_loads(r["data_json"])
        row: Dict[str, Any] = {
            "ID": r["id"],
            "アプリID": app_key,
            "アプリ名": apps_by_key.get(app_key, {}).get("app_name", app_key) if isinstance(apps_by_key.get(app_key), dict) else (apps_by_key.get(app_key)["app_name"] if apps_by_key.get(app_key) else app_key),
            "状態": r["status"],
            "作成者": r["created_by"],
            "作成日時": r["created_at"],
            "更新日時": r["updated_at"],
        }
        for f in fields_cache[app_key]:
            row[f["label"]] = data.get(f["field_key"], "")
        row["承認者"] = r["approved_by"]
        row["承認日時"] = r["approved_at"]
        row["管理職コメント"] = r["admin_comment"]
        rows.append(row)
    return pd.DataFrame(rows)


# -----------------------------
# 画面部品
# -----------------------------

def render_header() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
        .status-badge { display:inline-block; padding:0.18rem 0.55rem; border-radius:999px; font-weight:700; border:1px solid #ddd; }
        .small-note { color:#666; font-size:0.9rem; }
        .print-card { border:1px solid #222; padding:18px; margin:8px 0; background:#fff; }
        @media print {
            .stSidebar, header, footer, [data-testid="stToolbar"], [data-testid="stDecoration"] { display:none !important; }
            .block-container { max-width: 100% !important; padding: 0.5rem !important; }
            button { display:none !important; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title(APP_TITLE)
    st.caption(APP_VERSION)


def app_select(label: str = "アプリを選択", key: str = "app_select") -> Optional[str]:
    apps = get_apps(active_only=True)
    if not apps:
        st.warning("アプリが未登録です。まず管理画面でアプリを作成してください。")
        return None
    options = [a["app_key"] for a in apps]
    names = {a["app_key"]: f'{a["app_key"]}：{a["app_name"]}' for a in apps}
    return st.selectbox(label, options=options, format_func=lambda x: names.get(x, x), key=key)


def sidebar_state() -> Tuple[str, str, bool]:
    st.sidebar.header("利用者設定")
    user_name = st.sidebar.text_input("氏名", value=st.session_state.get("user_name", ""), key="sidebar_user_name")
    role = st.sidebar.radio("画面", ["教員", "管理職"], horizontal=True, key="sidebar_role")
    is_admin = False
    if role == "管理職":
        password = st.sidebar.text_input("管理職パスワード", type="password", key="sidebar_admin_password")
        is_admin = password == get_secret_password()
        if not is_admin:
            st.sidebar.warning("管理職機能はパスワード入力後に利用できます。")
    st.sidebar.divider()
    st.sidebar.caption("初期パスワードは 1234 です。Streamlit Cloudでは Secrets の ADMIN_PASSWORD を設定してください。")
    return user_name, role, is_admin


def render_field_widget(field: sqlite3.Row, default: Any = None, prefix: str = "form") -> Any:
    fkey = field["field_key"]
    label = field["label"]
    required = bool(field["required"])
    field_type = field["field_type"]
    label_text = f"{label}{' *' if required else ''}"
    widget_key = f"{prefix}_{field['id']}_{fkey}"

    if field_type == "text":
        return st.text_input(label_text, value=str(default or ""), key=widget_key)

    if field_type == "textarea":
        return st.text_area(label_text, value=str(default or ""), key=widget_key, height=120)

    if field_type == "number":
        if default in (None, ""):
            return st.number_input(label_text, value=0.0, key=widget_key)
        try:
            value = float(default)
        except (TypeError, ValueError):
            value = 0.0
        return st.number_input(label_text, value=value, key=widget_key)

    if field_type == "date":
        if isinstance(default, str) and default:
            try:
                d = date.fromisoformat(default[:10])
            except ValueError:
                d = date.today()
        elif isinstance(default, date):
            d = default
        else:
            d = date.today()
        return st.date_input(label_text, value=d, key=widget_key).isoformat()

    if field_type == "select":
        choices = parse_choices(field["choices_csv"])
        if not choices:
            choices = [""]
        index = choices.index(default) if default in choices else 0
        return st.selectbox(label_text, options=choices, index=index, key=widget_key)

    if field_type == "checkbox":
        default_bool = bool(default) if isinstance(default, bool) else str(default).lower() in ["true", "1", "yes", "はい"]
        return st.checkbox(label_text, value=default_bool, key=widget_key)

    return st.text_input(label_text, value=str(default or ""), key=widget_key)


def validate_data(fields: List[sqlite3.Row], data: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    for f in fields:
        label = f["label"]
        value = data.get(f["field_key"])
        if f["required"]:
            if value is None or value == "" or value == []:
                errors.append(f"「{label}」が未入力です。")
        if f["field_type"] == "select":
            choices = parse_choices(f["choices_csv"])
            if value and choices and value not in choices:
                errors.append(f"「{label}」の選択肢が不正です。")
    return errors


def status_markdown(status: str) -> str:
    return f'<span class="status-badge">{status}</span>'


# -----------------------------
# 各ページ
# -----------------------------

def page_home() -> None:
    st.subheader("概要")
    st.write("項目を設定すると、入力フォーム・一覧・CSV出力・印刷・承認画面を自動生成します。")
    apps = get_apps(active_only=True)
    records = list_records(limit=10000)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("登録アプリ", len(apps))
    c2.metric("総データ件数", len(records))
    c3.metric("提出", sum(1 for r in records if r["status"] == STATUS_SUBMITTED))
    c4.metric("差戻", sum(1 for r in records if r["status"] == STATUS_RETURNED))

    st.divider()
    st.subheader("登録アプリ")
    if apps:
        df = pd.DataFrame(
            [
                {
                    "アプリID": a["app_key"],
                    "アプリ名": a["app_name"],
                    "説明": a["description"],
                    "承認": "あり" if a["requires_approval"] else "なし",
                    "更新日時": a["updated_at"],
                }
                for a in apps
            ]
        )
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("まだアプリがありません。")


def page_app_admin(is_admin: bool) -> None:
    st.subheader("アプリ管理")
    if not is_admin:
        st.warning("管理職パスワードを入力してください。")
        return

    tab1, tab2, tab3 = st.tabs(["新規アプリ", "項目設定", "アプリ編集"])

    with tab1:
        st.markdown("### 新しいアプリを作成")
        with st.form("create_app_form", clear_on_submit=False):
            default_key = next_app_key()
            app_key = st.text_input("アプリID", value=default_key)
            app_name = st.text_input("アプリ名", placeholder="例：面談記録")
            description = st.text_area("説明", placeholder="このアプリの用途")
            requires_approval = st.checkbox("管理職承認を使う", value=True)
            submitted = st.form_submit_button("アプリを作成")
        if submitted:
            ok, msg = create_app(app_key, app_name, description, requires_approval)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)

    with tab2:
        st.markdown("### 入力項目を設定")
        app_key = app_select("項目を設定するアプリ", key="field_admin_app")
        if app_key:
            fields = get_fields(app_key, active_only=False)
            with st.expander("項目を追加", expanded=True):
                with st.form("add_field_form", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        label = st.text_input("項目名", placeholder="例：教員名")
                        field_type_label = st.selectbox("入力形式", list(FIELD_TYPES.values()))
                        field_type = [k for k, v in FIELD_TYPES.items() if v == field_type_label][0]
                    with col2:
                        required = st.checkbox("必須", value=False)
                        display_order = st.number_input("表示順", min_value=1, max_value=999, value=len(fields) + 1, step=1)
                    choices_csv = st.text_area("選択肢（選択形式の場合。カンマ区切り）", placeholder="例：1年,2年,3年")
                    field_key = st.text_input("内部キー（空欄可。英数字推奨）", placeholder="例：teacher_name")
                    add_clicked = st.form_submit_button("項目を追加")
                if add_clicked:
                    ok, msg = create_field(app_key, label, field_type, required, choices_csv, int(display_order), field_key)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

            st.markdown("### 現在の項目")
            fields = get_fields(app_key, active_only=False)
            if not fields:
                st.info("項目が未登録です。")
            for f in fields:
                with st.expander(f'{f["display_order"]}. {f["label"]} / {FIELD_TYPES.get(f["field_type"], f["field_type"])}'):
                    with st.form(f"edit_field_{f['id']}"):
                        col1, col2 = st.columns(2)
                        with col1:
                            new_label = st.text_input("項目名", value=f["label"])
                            ft_label = st.selectbox(
                                "入力形式",
                                list(FIELD_TYPES.values()),
                                index=list(FIELD_TYPES.keys()).index(f["field_type"]),
                            )
                            new_field_type = [k for k, v in FIELD_TYPES.items() if v == ft_label][0]
                        with col2:
                            new_required = st.checkbox("必須", value=bool(f["required"]))
                            new_order = st.number_input("表示順", min_value=1, max_value=999, value=int(f["display_order"]), step=1)
                            new_active = st.checkbox("使用する", value=bool(f["active"]))
                        new_choices = st.text_area("選択肢", value=f["choices_csv"] or "")
                        if st.form_submit_button("この項目を更新"):
                            ok, msg = update_field(int(f["id"]), new_label, new_field_type, new_required, new_choices, int(new_order), new_active)
                            if ok:
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)

    with tab3:
        st.markdown("### アプリ情報を編集")
        app_key = app_select("編集するアプリ", key="edit_app_select")
        if app_key:
            app = get_app(app_key)
            if app:
                with st.form("edit_app_form"):
                    app_name = st.text_input("アプリ名", value=app["app_name"])
                    description = st.text_area("説明", value=app["description"] or "")
                    requires_approval = st.checkbox("管理職承認を使う", value=bool(app["requires_approval"]))
                    active = st.checkbox("使用する", value=bool(app["active"]))
                    if st.form_submit_button("アプリ情報を更新"):
                        update_app(app_key, app_name, description, requires_approval, active)
                        st.success("更新しました。")
                        st.rerun()


def page_entry(user_name: str) -> None:
    st.subheader("入力フォーム")
    app_key = app_select("入力するアプリ", key="entry_app_select")
    if not app_key:
        return
    app = get_app(app_key)
    fields = get_fields(app_key)
    if not app or not fields:
        st.warning("アプリまたは項目が未設定です。")
        return

    st.markdown(f"### {app['app_name']}")
    if app["description"]:
        st.caption(app["description"])

    with st.form("dynamic_entry_form", clear_on_submit=False):
        data: Dict[str, Any] = {}
        for f in fields:
            data[f["field_key"]] = render_field_widget(f, prefix="entry")
        note = "承認ありのアプリは「提出」として保存されます。承認なしのアプリは「保存」として登録されます。"
        st.caption(note)
        submitted = st.form_submit_button("保存する", type="primary")

    if submitted:
        errors = validate_data(fields, data)
        if not user_name.strip():
            errors.append("左側の氏名を入力してください。")
        if errors:
            st.error("入力内容を確認してください。")
            for e in errors:
                st.write(f"- {e}")
        else:
            status = STATUS_SUBMITTED if bool(app["requires_approval"]) else STATUS_SAVED
            rid = create_record(app_key, data, status, user_name)
            st.success(f"保存しました。受付ID：{rid}")


def page_records() -> None:
    st.subheader("データ一覧・CSV出力")
    apps = get_apps(active_only=False)
    apps_by_key = {a["app_key"]: a for a in apps}
    app_options = ["すべて"] + [a["app_key"] for a in apps]
    app_names = {a["app_key"]: f'{a["app_key"]}：{a["app_name"]}' for a in apps}

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        selected_app = st.selectbox("アプリ", app_options, format_func=lambda x: "すべて" if x == "すべて" else app_names.get(x, x))
    with col2:
        selected_status = st.selectbox("状態", ["すべて"] + STATUS_ORDER)
    with col3:
        limit = st.number_input("最大表示件数", min_value=50, max_value=5000, value=500, step=50)

    app_key = None if selected_app == "すべて" else selected_app
    records = list_records(app_key=app_key, status=selected_status, limit=int(limit))
    df = records_to_dataframe(records, apps_by_key)

    search_word = st.text_input("簡易検索", placeholder="一覧内の文字を検索")
    if search_word and not df.empty:
        mask = df.astype(str).apply(lambda col: col.str.contains(search_word, case=False, na=False)).any(axis=1)
        df = df[mask]

    if df.empty:
        st.info("該当データがありません。")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)

    csv_data = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "CSVダウンロード",
        data=csv_data,
        file_name=f"app_builder_records_{today_text()}.csv",
        mime="text/csv",
    )

    st.divider()
    st.markdown("### 詳細表示")
    record_ids = df["ID"].astype(int).tolist()
    rid = st.selectbox("詳細を見るID", record_ids, key="detail_record_id")
    render_record_detail(rid)


def render_record_detail(record_id: int) -> None:
    r = get_record(record_id)
    if not r:
        st.warning("データが見つかりません。")
        return
    app = get_app(r["app_key"])
    fields = get_fields(r["app_key"])
    data = json_loads(r["data_json"])

    st.markdown(f"**{app['app_name'] if app else r['app_key']}**　ID: {r['id']}", unsafe_allow_html=True)
    st.markdown(status_markdown(r["status"]), unsafe_allow_html=True)

    rows = []
    for f in fields:
        value = data.get(f["field_key"], "")
        rows.append({"項目": f["label"], "内容": value})
    rows.extend(
        [
            {"項目": "作成者", "内容": r["created_by"]},
            {"項目": "作成日時", "内容": r["created_at"]},
            {"項目": "更新日時", "内容": r["updated_at"]},
            {"項目": "承認者", "内容": r["approved_by"]},
            {"項目": "承認日時", "内容": r["approved_at"]},
            {"項目": "管理職コメント", "内容": r["admin_comment"]},
        ]
    )
    st.table(pd.DataFrame(rows))


def page_approval(is_admin: bool, user_name: str) -> None:
    st.subheader("管理職承認・差戻")
    if not is_admin:
        st.warning("管理職パスワードを入力してください。")
        return

    apps = get_apps(active_only=False)
    apps_by_key = {a["app_key"]: a for a in apps}
    app_options = ["すべて"] + [a["app_key"] for a in apps]
    app_names = {a["app_key"]: f'{a["app_key"]}：{a["app_name"]}' for a in apps}
    col1, col2 = st.columns([2, 1])
    with col1:
        selected_app = st.selectbox("アプリ", app_options, format_func=lambda x: "すべて" if x == "すべて" else app_names.get(x, x), key="approval_app")
    with col2:
        selected_status = st.selectbox("状態", [STATUS_SUBMITTED, STATUS_RETURNED, STATUS_APPROVED, STATUS_SAVED, "すべて"], key="approval_status")
    app_key = None if selected_app == "すべて" else selected_app
    records = list_records(app_key=app_key, status=selected_status, limit=1000)
    df = records_to_dataframe(records, apps_by_key)

    if df.empty:
        st.info("該当データがありません。")
        return

    st.dataframe(df, use_container_width=True, hide_index=True)
    rid = st.selectbox("処理するID", df["ID"].astype(int).tolist(), key="approval_record_id")
    render_record_detail(rid)

    with st.form("approval_form"):
        comment = st.text_area("管理職コメント", placeholder="承認・差戻理由など")
        col_a, col_b = st.columns(2)
        approve_clicked = col_a.form_submit_button("承認する", type="primary")
        return_clicked = col_b.form_submit_button("差し戻す")
    if approve_clicked or return_clicked:
        if not user_name.strip():
            st.error("左側の氏名を入力してください。")
            return
        new_status = STATUS_APPROVED if approve_clicked else STATUS_RETURNED
        set_record_status(int(rid), new_status, user_name, comment)
        st.success(f"ID {rid} を {new_status} にしました。")
        st.rerun()


def page_print() -> None:
    st.subheader("印刷用表示")
    apps = get_apps(active_only=False)
    apps_by_key = {a["app_key"]: a for a in apps}
    app_key = app_select("印刷するアプリ", key="print_app_select")
    if not app_key:
        return
    records = list_records(app_key=app_key, limit=1000)
    if not records:
        st.info("印刷できるデータがありません。")
        return
    df = records_to_dataframe(records, apps_by_key)
    st.dataframe(df, use_container_width=True, hide_index=True)
    rid = st.selectbox("印刷するID", df["ID"].astype(int).tolist(), key="print_record_id")
    r = get_record(int(rid))
    if not r:
        return
    app = get_app(r["app_key"])
    fields = get_fields(r["app_key"])
    data = json_loads(r["data_json"])

    st.info("ブラウザの印刷機能でPDF保存できます。Windowsなら Ctrl + P です。")
    st.markdown("---")
    st.markdown('<div class="print-card">', unsafe_allow_html=True)
    st.markdown(f"## {app['app_name'] if app else r['app_key']}")
    st.markdown(f"**受付ID:** {r['id']}　　**状態:** {r['status']}")
    st.markdown(f"**作成者:** {r['created_by']}　　**作成日時:** {r['created_at']}")
    printable_rows = []
    for f in fields:
        printable_rows.append({"項目": f["label"], "内容": data.get(f["field_key"], "")})
    st.table(pd.DataFrame(printable_rows))
    if r["admin_comment"]:
        st.markdown("### 管理職コメント")
        st.write(r["admin_comment"])
    st.markdown(f"**承認者:** {r['approved_by']}　　**承認日時:** {r['approved_at']}")
    st.markdown("</div>", unsafe_allow_html=True)


def page_backup(is_admin: bool) -> None:
    st.subheader("バックアップ")
    if not is_admin:
        st.warning("管理職パスワードを入力してください。")
        return

    st.write("現在のSQLiteデータベースをダウンロードできます。")
    if DB_PATH.exists():
        data = DB_PATH.read_bytes()
        st.download_button(
            "データベースをダウンロード",
            data=data,
            file_name=f"app_builder_backup_{today_text()}.db",
            mime="application/octet-stream",
        )
    else:
        st.warning("データベースファイルが見つかりません。")

    st.divider()
    st.write("全データをCSVでまとめて出力します。")
    apps = get_apps(active_only=False)
    apps_by_key = {a["app_key"]: a for a in apps}
    records = list_records(limit=10000)
    df = records_to_dataframe(records, apps_by_key)
    if not df.empty:
        st.download_button(
            "全データCSVダウンロード",
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"app_builder_all_records_{today_text()}.csv",
            mime="text/csv",
        )
    else:
        st.info("データがありません。")


def main() -> None:
    init_db()
    seed_sample_data()
    render_header()
    user_name, role, is_admin = sidebar_state()

    if role == "教員":
        menu = st.sidebar.radio(
            "メニュー",
            ["ホーム", "入力フォーム", "データ一覧", "印刷用表示"],
        )
    else:
        menu = st.sidebar.radio(
            "メニュー",
            ["ホーム", "入力フォーム", "データ一覧", "管理職承認", "アプリ管理", "印刷用表示", "バックアップ"],
        )

    if menu == "ホーム":
        page_home()
    elif menu == "入力フォーム":
        page_entry(user_name)
    elif menu == "データ一覧":
        page_records()
    elif menu == "管理職承認":
        page_approval(is_admin, user_name)
    elif menu == "アプリ管理":
        page_app_admin(is_admin)
    elif menu == "印刷用表示":
        page_print()
    elif menu == "バックアップ":
        page_backup(is_admin)


if __name__ == "__main__":
    main()

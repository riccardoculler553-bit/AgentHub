"""MySQL 结构同步（一次性工具）：把旧库缺的列按当前 ORM 补齐，再 alembic stamp head。

背景：该库历史由 Base.metadata.create_all 建（只建缺表、不改旧表），直接跑
alembic upgrade 会撞"表已存在"与 MySQL 8.x 的 TEXT/JSON DEFAULT 限制。
本脚本按 Base.metadata 对比 information_schema，逐列 ADD；然后由调用方 stamp。

用法：
  cd d:\\Websocket\\server
  python ../scripts/sync_mysql_schema.py   # dry-run 默认
  python ../scripts/sync_mysql_schema.py --apply
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "server"))
sys.path.insert(0, str(ROOT / "client"))

import pymysql  # noqa: E402
from sqlalchemy.dialects import mysql  # noqa: E402
from sqlalchemy.schema import CreateColumn  # noqa: E402

from app.core.config import settings  # noqa: E402


def connect():
    from sqlalchemy.engine import make_url

    url = make_url(settings.database_url)
    return pymysql.connect(
        host=url.host or "127.0.0.1",
        port=url.port or 3306,
        user=url.username,
        password=url.password,
        database=url.database,
        charset="utf8mb4",
        autocommit=False,
    )


def existing_columns(cur, table: str) -> dict[str, dict]:
    cur.execute(
        "SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE, COLUMN_DEFAULT "
        "FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table,),
    )
    return {r[0]: {"nullable": r[1] == "YES", "type": r[2], "default": r[3]} for r in cur.fetchall()}


def default_literal(col) -> str | None:
    """Backfill literal for a NOT NULL column, derived from the ORM python default."""
    d = col.default
    value = d.arg(None) if (d is not None and getattr(d, "is_callable", False)) else (
        d.arg if d is not None and getattr(d, "is_scalar", False) else None
    )
    if value is None and col.default is not None and getattr(col.default, "is_callable", False):
        value = col.default.arg(None)
    if value is None:
        return None
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return f"'{str(value).replace(chr(39), chr(39) * 2)}'"


def add_column_sql(table: str, col) -> list[str]:
    from app.db.database import Base  # noqa: F401 - ensure metadata loaded

    ddl_type = col.type.compile(dialect=mysql.dialect())
    not_null = "" if col.nullable else " NOT NULL"
    stmts = []
    needs_backfill = not col.nullable and col.type.__class__.__name__ == "JSON"
    base = f"ALTER TABLE `{table}` ADD COLUMN {CreateColumn(col).compile(dialect=mysql.dialect()).string.split(' ', 1)[1] if False else ''}"
    # CreateColumn 编译含列名；为避免依赖其格式差异，手工拼：
    col_sql = f"`{col.name}` {ddl_type}{not_null}"
    if not needs_backfill and not col.nullable:
        lit = default_literal(col)
        if lit is None and col.type.__class__.__name__ in ("String", "Text"):
            lit = "''"
        if lit is None and col.type.__class__.__name__ in ("Integer", "BigInteger", "Boolean"):
            lit = "0"
        if lit is not None:
            col_sql += f" DEFAULT {lit}"
    stmts.append(f"ALTER TABLE `{table}` ADD COLUMN {col_sql}")
    if needs_backfill:
        stmts.append(f"UPDATE `{table}` SET `{col.name}` = '[]' WHERE `{col.name}` IS NULL")
        stmts.append(
            f"ALTER TABLE `{table}` MODIFY COLUMN `{col.name}` {ddl_type} NOT NULL"
        )
    return stmts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="实际执行（默认只打印）")
    args = parser.parse_args()

    from app.db import models  # noqa: F401
    from app.agent import db_models as agent_models  # noqa: F401
    from app.command import db_models as command_models  # noqa: F401
    from app.capability import db_models as cap_models  # noqa: F401
    from app.capability_runtime import db_models as cr_models  # noqa: F401
    from app.task import db_models as task_models  # noqa: F401
    from app.workflow import db_models as wf_models  # noqa: F401
    from app.artifact import db_models as art_models  # noqa: F401
    from app.db.database import Base

    conn = connect()
    cur = conn.cursor()
    statements: list[str] = []

    for table in sorted(Base.metadata.tables):
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %s",
            (table,),
        )
        if cur.fetchone()[0] == 0:
            print(f"[missing table] {table} -> 交给 create_all/迁移，本脚本跳过")
            continue
        existing = existing_columns(cur, table)
        for col in Base.metadata.tables[table].columns:
            if col.name.lower() in {c.lower() for c in existing}:
                continue
            for stmt in add_column_sql(table, col):
                statements.append(stmt)
            print(f"[add column] {table}.{col.name} ({col.type})")

    if not statements:
        print("schema 已同步，无需变更")
        return 0
    for stmt in statements:
        print(("RUN " if args.apply else "PLAN ") + stmt[:160])
        if args.apply:
            cur.execute(stmt)
    if args.apply:
        conn.commit()
        print("applied.")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

import io
import csv
import argparse
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

import yaml
import pyodbc
import psycopg2
from psycopg2 import sql
from tqdm import tqdm


# ----------------------------
# Load YAML config
# ----------------------------
def load_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Config file not found: {p.resolve()}")

    with p.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # Minimal validation / defaults
    cfg.setdefault("mssql", {})
    cfg.setdefault("postgres", {})
    cfg.setdefault("load", {})

    cfg["mssql"].setdefault("port", 1433)
    cfg["mssql"].setdefault("schema", "dbo")
    cfg["mssql"].setdefault("odbc_driver", "ODBC Driver 17 for SQL Server")

    cfg["postgres"].setdefault("host", "localhost")
    cfg["postgres"].setdefault("port", 5432)
    cfg["postgres"].setdefault("schema", "public")

    cfg["load"].setdefault("fetch_many", 5000)
    cfg["load"].setdefault("truncate_before_load", True)

    required_paths = [
        ("mssql", "host"),
        ("mssql", "database"),
        ("mssql", "user"),
        ("mssql", "password"),
        ("postgres", "database"),
        ("postgres", "user"),
        ("postgres", "password"),
    ]
    missing = []
    for section, key in required_paths:
        v = cfg.get(section, {}).get(key)
        if v is None or str(v).strip() == "":
            missing.append(f"{section}.{key}")

    if missing:
        raise SystemExit(f"Missing required config fields: {', '.join(missing)}")

    return cfg


# ----------------------------
# Type mapping SQL Server -> Postgres
# ----------------------------
def map_type(mssql_type: str, char_len, num_precision, num_scale) -> str:
    t = mssql_type.lower()

    if t == "int":
        return "integer"
    if t == "bigint":
        return "bigint"
    if t == "smallint":
        return "smallint"
    if t == "tinyint":
        return "smallint"
    if t == "bit":
        return "boolean"

    if t == "float":
        return "double precision"
    if t == "real":
        return "real"
    if t in ("decimal", "numeric", "money", "smallmoney"):
        if num_precision is not None and num_scale is not None:
            return f"numeric({int(num_precision)},{int(num_scale)})"
        return "numeric"

    if t == "date":
        return "date"
    if t in ("datetime", "smalldatetime", "datetime2"):
        return "timestamp without time zone"
    if t == "time":
        return "time without time zone"

    if t == "uniqueidentifier":
        return "uuid"

    if t in ("varchar", "nvarchar", "char", "nchar"):
        if char_len is None or int(char_len) < 0:  # MAX
            return "text"
        return f"varchar({int(char_len)})"

    if t in ("text", "ntext"):
        return "text"

    if t in ("varbinary", "binary", "image"):
        return "bytea"

    return "text"


# ----------------------------
# Connectors
# ----------------------------
def connect_mssql(cfg: Dict[str, Any]) -> pyodbc.Connection:
    m = cfg["mssql"]
    server = f"{m['host']},{m['port']}"
    conn_str = (
        f"DRIVER={{{m['odbc_driver']}}};"
        f"SERVER={server};"
        f"DATABASE={m['database']};"
        f"UID={m['user']};"
        f"PWD={m['password']};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def connect_pg(cfg: Dict[str, Any]) -> psycopg2.extensions.connection:
    p = cfg["postgres"]
    return psycopg2.connect(
        host=p.get("host", "localhost"),
        port=p.get("port", 5432),
        dbname=p["database"],
        user=p["user"],
        password=p["password"],
    )


# ----------------------------
# SQL Server metadata
# ----------------------------
def list_tables(mssql: pyodbc.Connection, mssql_schema: str) -> List[Tuple[str, str]]:
    q = """
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = ?
    ORDER BY TABLE_NAME;
    """
    cur = mssql.cursor()
    cur.execute(q, mssql_schema)
    return [(r[0], r[1]) for r in cur.fetchall()]

def get_columns(mssql: pyodbc.Connection, schema: str, table: str):
    q = """
    SELECT
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
    ORDER BY ORDINAL_POSITION;
    """
    cur = mssql.cursor()
    cur.execute(q, schema, table)
    return cur.fetchall()

def estimate_rowcount(mssql: pyodbc.Connection, schema: str, table: str) -> Optional[int]:
    q = """
    SELECT SUM(ps.row_count) AS row_count
    FROM sys.dm_db_partition_stats ps
    JOIN sys.objects o ON ps.object_id = o.object_id
    JOIN sys.schemas s ON o.schema_id = s.schema_id
    WHERE o.type = 'U'
      AND s.name = ?
      AND o.name = ?
      AND ps.index_id IN (0,1);
    """
    cur = mssql.cursor()
    try:
        cur.execute(q, schema, table)
        val = cur.fetchone()[0]
        return int(val) if val is not None else None
    except Exception:
        return None


# ----------------------------
# Postgres DDL
# ----------------------------
def ensure_schema(pg, pg_schema: str):
    with pg.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(pg_schema)))
    pg.commit()

def create_table_if_missing(pg, pg_schema: str, table: str, cols) -> List[str]:
    col_names = []
    col_defs = []

    for (name, dtype, char_len, prec, scale, is_nullable) in cols:
        col_names.append(name)
        pg_type = map_type(dtype, char_len, prec, scale)
        nullable = (str(is_nullable).upper() == "YES")
        col_defs.append(
            sql.SQL("{} {} {}").format(
                sql.Identifier(name),
                sql.SQL(pg_type),
                sql.SQL("" if nullable else "NOT NULL")
            )
        )

    ddl = (
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} (").format(
            sql.Identifier(pg_schema),
            sql.Identifier(table),
        )
        + sql.SQL(", ").join(col_defs)
        + sql.SQL(");")
    )

    with pg.cursor() as cur:
        cur.execute(ddl)
    pg.commit()
    return col_names

def truncate_table(pg, pg_schema: str, table: str):
    with pg.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE TABLE {}.{};").format(
            sql.Identifier(pg_schema), sql.Identifier(table)
        ))
    pg.commit()


# ----------------------------
# Data copy: stream batches + COPY
# ----------------------------
def copy_table_data(
    mssql,
    pg,
    mssql_schema: str,
    pg_schema: str,
    table: str,
    col_names: List[str],
    fetch_many: int,
    truncate_before_load: bool,
):
    select_cols = ", ".join([f"[{c}]" for c in col_names])
    q = f"SELECT {select_cols} FROM [{mssql_schema}].[{table}];"

    mcur = mssql.cursor()
    mcur.execute(q)

    if truncate_before_load:
        truncate_table(pg, pg_schema, table)

    copy_sql = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    ).format(
        sql.Identifier(pg_schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, col_names)),
    )

    total = estimate_rowcount(mssql, mssql_schema, table)
    row_pbar = tqdm(total=total, desc=f"Rows {mssql_schema}.{table}", unit="rows", leave=False)

    with pg.cursor() as pcur:
        while True:
            rows = mcur.fetchmany(fetch_many)
            if not rows:
                break

            buf = io.StringIO()
            w = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

            for r in rows:
                out = []
                for v in r:
                    if v is None:
                        out.append(r"\N")
                    elif isinstance(v, bytes):
                        out.append("\\x" + v.hex())
                    else:
                        out.append(v)
                w.writerow(out)

            buf.seek(0)
            pcur.copy_expert(copy_sql.as_string(pg), buf)
            row_pbar.update(len(rows))

    pg.commit()
    row_pbar.close()


def main():
    parser = argparse.ArgumentParser(description="Copy MSSQL schema + data to Postgres using YAML config.")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML (default: config.yaml)")
    args = parser.parse_args()

    cfg = load_config(args.config)

    mssql_schema = cfg["mssql"]["schema"]
    pg_schema = cfg["postgres"]["schema"]
    fetch_many = int(cfg["load"]["fetch_many"])
    truncate_before_load = bool(cfg["load"]["truncate_before_load"])

    mssql = connect_mssql(cfg)
    pg = connect_pg(cfg)

    try:
        ensure_schema(pg, pg_schema)

        tables = list_tables(mssql, mssql_schema)
        if not tables:
            print(f"No tables found in SQL Server schema '{mssql_schema}'.")
            return

        # Pass 1: create tables
        table_cols: Dict[str, List[str]] = {}
        for schema, table in tqdm(tables, desc="Tables (DDL)", unit="table"):
            cols = get_columns(mssql, schema, table)
            col_names = create_table_if_missing(pg, pg_schema, table, cols)
            table_cols[table] = col_names

        # Pass 2: load data
        for schema, table in tqdm(tables, desc="Tables (COPY)", unit="table"):
            copy_table_data(
                mssql=mssql,
                pg=pg,
                mssql_schema=schema,
                pg_schema=pg_schema,
                table=table,
                col_names=table_cols[table],
                fetch_many=fetch_many,
                truncate_before_load=truncate_before_load,
            )

        print("DONE: schema + data copied (tables + rows).")

    finally:
        try:
            mssql.close()
        except Exception:
            pass
        try:
            pg.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
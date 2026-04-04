#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Callable

import os
import pyodbc
import yaml
from tqdm import tqdm
import json

CONFIG_PATH = Path("config.yaml")

GRAPH_SCHEMA = "net"
SOURCE_SCHEMA = "wts"
GRAPH_START_DATE = "2018-01-01"
STATEMENT_TIMEOUT_SECONDS = 0

TOP_N_WASTELINE_NUMBERS = 25
TOP_N_FACILITIES = 100
TOP_N_GENERATORS = 50
TOP_N_TRANSPORTERS = 500


def make_year_quarter(year: int, quarter: int) -> int:
    return year * 100 + quarter


def quarter_date_range(year: int, quarter: int) -> tuple[str, str]:
    start_month = 1 + (quarter - 1) * 3
    start = date(year, start_month, 1)

    if quarter == 4:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, start_month + 3, 1)

    return start.isoformat(), end.isoformat()


@dataclass
class QuarterInfo:
    year: int
    quarter: int
    year_quarter: int
    manifest_count: int
    waste_line_count: int

    @property
    def label(self) -> str:
        return f"{self.year}-Q{self.quarter}"

    def _asdict(self) -> dict:
        return {
            "year": self.year,
            "quarter": self.quarter,
            "year_quarter": self.year_quarter,
            "manifest_count": self.manifest_count,
            "waste_line_count": self.waste_line_count,
        }

def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def format_seconds(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_pg_connection(cfg: dict) -> pyodbc.Connection:
    pg = cfg["postgres"]
    driver = pg.get("odbc_driver")
    if not driver:
        raise RuntimeError(
            "Missing postgres.odbc_driver in config.yaml. "
            "Run `import pyodbc; print(pyodbc.drivers())` and use the exact PostgreSQL driver name."
        )

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={pg['host']};"
        f"PORT={pg['port']};"
        f"DATABASE={pg['database']};"
        f"UID={pg['user']};"
        f"PWD={pg['password']};"
    )

    try:
        return pyodbc.connect(conn_str, autocommit=True)
    except pyodbc.InterfaceError as e:
        installed = pyodbc.drivers()
        raise RuntimeError(
            f"Could not connect to Postgres with ODBC driver '{driver}'.\n"
            f"Installed ODBC drivers on this machine: {installed}\n"
            f"Original error: {e}"
        ) from e


def fetchall_dict(cursor: pyodbc.Cursor):
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def exec_sql(
    conn: pyodbc.Connection,
    sql: str,
    params: Optional[tuple] = None,
    label: Optional[str] = None,
    fetch: bool = False,
):
    started = time.time()
    cur = conn.cursor()
    try:
        if STATEMENT_TIMEOUT_SECONDS > 0:
            try:
                cur.timeout = STATEMENT_TIMEOUT_SECONDS
            except Exception:
                pass

        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)

        rows = fetchall_dict(cur) if fetch else None
        elapsed = time.time() - started
        if label:
            log(f"{label} completed in {format_seconds(elapsed)}")
        return rows
    except pyodbc.Error as e:
        if label:
            log(f"{label} FAILED: {e}")
        raise
    finally:
        cur.close()


def exec_sql_retry(
    conn: pyodbc.Connection,
    sql: str,
    params: Optional[tuple] = None,
    label: Optional[str] = None,
    fetch: bool = False,
    retries: int = 2,
    sleep_seconds: int = 5,
):
    attempt = 0
    while True:
        attempt += 1
        try:
            return exec_sql(conn, sql, params=params, label=label, fetch=fetch)
        except pyodbc.Error as e:
            msg = str(e).lower()
            log(f"{label or 'SQL step'} failed on attempt {attempt}: {e}")

            if "out of memory" in msg:
                log("Postgres reported out-of-memory.")
            elif "no space left on device" in msg or "disk full" in msg:
                log("Postgres reported disk full.")
            elif "statement timeout" in msg or "canceling statement due to statement timeout" in msg:
                log("Statement timeout occurred.")
            elif "deadlock detected" in msg:
                log("Deadlock detected.")
            elif "could not resize shared memory segment" in msg:
                log("Shared memory exhausted.")

            if attempt > retries:
                raise
            log(f"Retrying in {sleep_seconds}s...")
            time.sleep(sleep_seconds)


def run_steps_with_tqdm(desc: str, steps: List[tuple[str, Callable[[], None]]]) -> None:
    with tqdm(total=len(steps), desc=desc, unit="step") as bar:
        for _, fn in steps:
            fn()
            bar.update(1)


def create_metadata_tables(conn: pyodbc.Connection) -> None:
    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {GRAPH_SCHEMA};

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.graph_build_run (
        run_id           bigserial PRIMARY KEY,
        started_at       timestamp NOT NULL DEFAULT now(),
        finished_at      timestamp,
        status           varchar(20) NOT NULL DEFAULT 'RUNNING',
        start_date       date NOT NULL,
        notes            text
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.graph_build_step (
        run_id           bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_build_run(run_id),
        year_quarter     integer NOT NULL,
        step_name        varchar(100) NOT NULL,
        started_at       timestamp NOT NULL DEFAULT now(),
        finished_at      timestamp,
        status           varchar(20) NOT NULL DEFAULT 'RUNNING',
        detail           text,
        PRIMARY KEY (run_id, year_quarter, step_name)
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.graph_stage_status (
        year_quarter int PRIMARY KEY,
        waste_line_fact_ready boolean NOT NULL DEFAULT false,
        manifest_transporter_ready boolean NOT NULL DEFAULT false,
        staged_at timestamp,
        notes text
    );
    """
    exec_sql_retry(conn, sql, label="Create metadata tables")


def create_target_tables(conn: pyodbc.Connection) -> None:
    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {GRAPH_SCHEMA};

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.graph_waste_stream (
        waste_stream_id                    bigserial PRIMARY KEY,
        waste_stream_key                   text NOT NULL,
        usdot_hazardous_indicator          varchar(1),
        primary_description                varchar(500),
        usdot_description                  varchar(500),
        non_hazardous_waste_description    varchar(500),
        management_method_code             varchar(4),
        management_method_description      varchar(100),
        form_code                          varchar(4),
        form_code_description              varchar(125),
        source_code                        varchar(3),
        source_code_description            varchar(125),
        container_type_code                varchar(2),
        container_type_description         varchar(50),
        display_name                       text,
        created_at                         timestamp NOT NULL DEFAULT now()
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_graph_waste_stream_key
        ON {GRAPH_SCHEMA}.graph_waste_stream (waste_stream_key);

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.graph_edge_generator_facility_stream_qtr (
        edge_id                    bigserial PRIMARY KEY,
        year                       int NOT NULL,
        quarter                    int NOT NULL,
        year_quarter               int NOT NULL,
        generator_node_id          bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_node(graph_node_id),
        facility_node_id           bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_node(graph_node_id),
        waste_stream_id            bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_waste_stream(waste_stream_id),
        manifest_count             bigint,
        waste_line_count           bigint,
        total_waste_tons           double precision,
        total_waste_kg             double precision,
        unique_transporters        bigint,
        first_shipped_date         timestamp,
        last_shipped_date          timestamp,
        created_at                 timestamp NOT NULL DEFAULT now(),
        CONSTRAINT uq_gef_sq UNIQUE (year_quarter, generator_node_id, facility_node_id, waste_stream_id)
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.graph_edge_generator_transporter_stream_qtr (
        edge_id                    bigserial PRIMARY KEY,
        year                       int NOT NULL,
        quarter                    int NOT NULL,
        year_quarter               int NOT NULL,
        generator_node_id          bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_node(graph_node_id),
        transporter_node_id        bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_node(graph_node_id),
        waste_stream_id            bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_waste_stream(waste_stream_id),
        manifest_count             bigint,
        waste_line_count           bigint,
        total_waste_tons           double precision,
        total_waste_kg             double precision,
        unique_facilities          bigint,
        first_shipped_date         timestamp,
        last_shipped_date          timestamp,
        created_at                 timestamp NOT NULL DEFAULT now(),
        CONSTRAINT uq_get_sq UNIQUE (year_quarter, generator_node_id, transporter_node_id, waste_stream_id)
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.graph_edge_transporter_facility_stream_qtr (
        edge_id                    bigserial PRIMARY KEY,
        year                       int NOT NULL,
        quarter                    int NOT NULL,
        year_quarter               int NOT NULL,
        transporter_node_id        bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_node(graph_node_id),
        facility_node_id           bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_node(graph_node_id),
        waste_stream_id            bigint NOT NULL REFERENCES {GRAPH_SCHEMA}.graph_waste_stream(waste_stream_id),
        manifest_count             bigint,
        waste_line_count           bigint,
        total_waste_tons           double precision,
        total_waste_kg             double precision,
        unique_generators          bigint,
        first_shipped_date         timestamp,
        last_shipped_date          timestamp,
        created_at                 timestamp NOT NULL DEFAULT now(),
        CONSTRAINT uq_gtf_sq UNIQUE (year_quarter, transporter_node_id, facility_node_id, waste_stream_id)
    );
    """
    exec_sql_retry(conn, sql, label="Create target tables")


def create_staging_tables(conn: pyodbc.Connection) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.stg_top_wasteline_numbers (
        "WasteLineNumber" integer PRIMARY KEY,
        row_count bigint NOT NULL,
        created_at timestamp NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.stg_top_facilities (
        "DesignatedFacilityEPAID" varchar(12) PRIMARY KEY,
        row_count bigint NOT NULL,
        created_at timestamp NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.stg_top_generators (
        "GeneratorEPAID" varchar(15) PRIMARY KEY,
        row_count bigint NOT NULL,
        created_at timestamp NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.stg_top_transporters (
        "TransporterEPAID" varchar(15) PRIMARY KEY,
        row_count bigint NOT NULL,
        created_at timestamp NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.stg_waste_line_fact_qtr (
        "ManifestTrackingNumber"         varchar(12),
        "WasteLineNumber"                integer,
        "ShippedDate"                    timestamp,
        year                             int,
        quarter                          int,
        year_quarter                     int,
        "GeneratorEPAID"                 varchar(15),
        "DesignatedFacilityEPAID"        varchar(12),
        usdot_hazardous_indicator        varchar(1),
        usdot_description                varchar(500),
        non_hazardous_waste_description  varchar(500),
        management_method_code           varchar(4),
        management_method_description    varchar(100),
        form_code                        varchar(4),
        form_code_description            varchar(125),
        source_code                      varchar(3),
        source_code_description          varchar(125),
        container_type_code              varchar(2),
        container_type_description       varchar(50),
        primary_description              varchar(500),
        waste_quantity_tons              double precision,
        waste_quantity_kg                double precision,
        unique_transporters              bigint,
        waste_stream_key                 text
    );

    CREATE TABLE IF NOT EXISTS {GRAPH_SCHEMA}.stg_manifest_transporter_qtr (
        year_quarter              int,
        "ManifestTrackingNumber"  varchar(12),
        "TransporterEPAID"        varchar(15)
    );
    """
    exec_sql_retry(conn, sql, label="Create staging tables")


def create_core_indexes(conn: pyodbc.Connection) -> None:
    sql = f"""
    CREATE INDEX IF NOT EXISTS ix_em_manifest_ship_track
        ON {SOURCE_SCHEMA}."EM_MANIFEST" ("ShippedDate", "ManifestTrackingNumber");

    CREATE INDEX IF NOT EXISTS ix_em_manifest_facility
        ON {SOURCE_SCHEMA}."EM_MANIFEST" ("DesignatedFacilityEPAID");

    CREATE INDEX IF NOT EXISTS ix_em_manifest_generator
        ON {SOURCE_SCHEMA}."EM_MANIFEST" ("GeneratorEPAID");

    CREATE INDEX IF NOT EXISTS ix_em_waste_line_track
        ON {SOURCE_SCHEMA}."EM_WASTE_LINE" ("ManifestTrackingNumber");

    CREATE INDEX IF NOT EXISTS ix_em_waste_line_number
        ON {SOURCE_SCHEMA}."EM_WASTE_LINE" ("WasteLineNumber");

    CREATE INDEX IF NOT EXISTS ix_em_transporter_track
        ON {SOURCE_SCHEMA}."EM_TRANSPORTER" ("ManifestTrackingNumber");

    CREATE INDEX IF NOT EXISTS ix_em_transporter_epa
        ON {SOURCE_SCHEMA}."EM_TRANSPORTER" ("TransporterEPAID");

    CREATE UNIQUE INDEX IF NOT EXISTS ux_graph_node_type_business_id
        ON {GRAPH_SCHEMA}.graph_node (node_type, business_id);

    CREATE INDEX IF NOT EXISTS ix_stg_wlfq_yearq
        ON {GRAPH_SCHEMA}.stg_waste_line_fact_qtr (year_quarter);

    CREATE INDEX IF NOT EXISTS ix_stg_wlfq_track
        ON {GRAPH_SCHEMA}.stg_waste_line_fact_qtr ("ManifestTrackingNumber");

    CREATE INDEX IF NOT EXISTS ix_stg_wlfq_generator
        ON {GRAPH_SCHEMA}.stg_waste_line_fact_qtr ("GeneratorEPAID");

    CREATE INDEX IF NOT EXISTS ix_stg_wlfq_facility
        ON {GRAPH_SCHEMA}.stg_waste_line_fact_qtr ("DesignatedFacilityEPAID");

    CREATE INDEX IF NOT EXISTS ix_stg_wlfq_stream
        ON {GRAPH_SCHEMA}.stg_waste_line_fact_qtr (waste_stream_key);

    CREATE INDEX IF NOT EXISTS ix_stg_wlfq_yearq_gen_fac_stream
        ON {GRAPH_SCHEMA}.stg_waste_line_fact_qtr (year_quarter, "GeneratorEPAID", "DesignatedFacilityEPAID", waste_stream_key);

    CREATE INDEX IF NOT EXISTS ix_stg_wlfq_yearq_facility
        ON {GRAPH_SCHEMA}.stg_waste_line_fact_qtr (year_quarter, "DesignatedFacilityEPAID");

    CREATE INDEX IF NOT EXISTS ix_stg_wlfq_yearq_wasteline
        ON {GRAPH_SCHEMA}.stg_waste_line_fact_qtr (year_quarter, "WasteLineNumber");

    CREATE INDEX IF NOT EXISTS ix_stg_mtq_yearq
        ON {GRAPH_SCHEMA}.stg_manifest_transporter_qtr (year_quarter);

    CREATE INDEX IF NOT EXISTS ix_stg_mtq_track
        ON {GRAPH_SCHEMA}.stg_manifest_transporter_qtr ("ManifestTrackingNumber");

    CREATE INDEX IF NOT EXISTS ix_stg_mtq_transporter
        ON {GRAPH_SCHEMA}.stg_manifest_transporter_qtr ("TransporterEPAID");

    CREATE INDEX IF NOT EXISTS ix_stg_mtq_yearq_track
        ON {GRAPH_SCHEMA}.stg_manifest_transporter_qtr (year_quarter, "ManifestTrackingNumber");
    """
    exec_sql_retry(conn, sql, label="Create core and staging indexes")


def refresh_nodes(conn: pyodbc.Connection) -> None:
    sql = f"""
    INSERT INTO {GRAPH_SCHEMA}.graph_node (node_type, business_id, business_name, state_code, source_table)
    SELECT
        'GENERATOR',
        m."GeneratorEPAID",
        MAX(m."GeneratorName"),
        MAX(m."GeneratorLocationState"),
        'EM_MANIFEST'
    FROM {SOURCE_SCHEMA}."EM_MANIFEST" m
    WHERE m."GeneratorEPAID" IS NOT NULL
      AND m."ShippedDate" >= CAST(? AS date)
    GROUP BY m."GeneratorEPAID"
    ON CONFLICT (node_type, business_id) DO UPDATE
    SET
        business_name = EXCLUDED.business_name,
        state_code = EXCLUDED.state_code,
        source_table = EXCLUDED.source_table,
        is_active = true;

    INSERT INTO {GRAPH_SCHEMA}.graph_node (node_type, business_id, business_name, state_code, source_table)
    SELECT
        'FACILITY',
        m."DesignatedFacilityEPAID",
        MAX(m."DesignatedFacilityName"),
        MAX(m."DesignatedFacilityLocationState"),
        'EM_MANIFEST'
    FROM {SOURCE_SCHEMA}."EM_MANIFEST" m
    WHERE m."DesignatedFacilityEPAID" IS NOT NULL
      AND m."ShippedDate" >= CAST(? AS date)
    GROUP BY m."DesignatedFacilityEPAID"
    ON CONFLICT (node_type, business_id) DO UPDATE
    SET
        business_name = EXCLUDED.business_name,
        state_code = EXCLUDED.state_code,
        source_table = EXCLUDED.source_table,
        is_active = true;

    INSERT INTO {GRAPH_SCHEMA}.graph_node (node_type, business_id, business_name, state_code, source_table)
    SELECT
        'TRANSPORTER',
        t."TransporterEPAID",
        MAX(t."TransporterName"),
        NULL,
        'EM_TRANSPORTER'
    FROM {SOURCE_SCHEMA}."EM_TRANSPORTER" t
    JOIN {SOURCE_SCHEMA}."EM_MANIFEST" m
      ON m."ManifestTrackingNumber" = t."ManifestTrackingNumber"
    WHERE t."TransporterEPAID" IS NOT NULL
      AND m."ShippedDate" >= CAST(? AS date)
    GROUP BY t."TransporterEPAID"
    ON CONFLICT (node_type, business_id) DO UPDATE
    SET
        business_name = EXCLUDED.business_name,
        source_table = EXCLUDED.source_table,
        is_active = true;
    """
    exec_sql_retry(conn, sql, params=(GRAPH_START_DATE, GRAPH_START_DATE, GRAPH_START_DATE), label="Refresh graph_node")


def rebuild_top_filter_tables(conn: pyodbc.Connection) -> None:
    steps = [
        (
            "top waste lines",
            f"""
            TRUNCATE TABLE {GRAPH_SCHEMA}.stg_top_wasteline_numbers;

            INSERT INTO {GRAPH_SCHEMA}.stg_top_wasteline_numbers ("WasteLineNumber", row_count)
            SELECT
                x."WasteLineNumber",
                x.row_count
            FROM (
                SELECT
                    w."WasteLineNumber",
                    COUNT(*) AS row_count
                FROM {SOURCE_SCHEMA}."EM_WASTE_LINE" w
                WHERE w."WasteLineNumber" IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM {SOURCE_SCHEMA}."EM_MANIFEST" m
                    WHERE m."ManifestTrackingNumber" = w."ManifestTrackingNumber"
                        AND m."ShippedDate" >= CAST(? AS date)
                )
                GROUP BY w."WasteLineNumber"
                ORDER BY COUNT(*) DESC
                LIMIT {TOP_N_WASTELINE_NUMBERS}
            ) x;
            """,
        ),
        (
            "top facilities",
            f"""
            TRUNCATE TABLE {GRAPH_SCHEMA}.stg_top_facilities;
            INSERT INTO {GRAPH_SCHEMA}.stg_top_facilities ("DesignatedFacilityEPAID", row_count)
            SELECT
                m."DesignatedFacilityEPAID",
                COUNT(*) AS row_count
            FROM {SOURCE_SCHEMA}."EM_MANIFEST" m
            WHERE m."ShippedDate" >= CAST(? AS date)
              AND m."DesignatedFacilityEPAID" IS NOT NULL
            GROUP BY m."DesignatedFacilityEPAID"
            ORDER BY COUNT(*) DESC
            LIMIT {TOP_N_FACILITIES};
            """,
        ),
        (
            "top generators",
            f"""
            TRUNCATE TABLE {GRAPH_SCHEMA}.stg_top_generators;
            INSERT INTO {GRAPH_SCHEMA}.stg_top_generators ("GeneratorEPAID", row_count)
            SELECT
                m."GeneratorEPAID",
                COUNT(*) AS row_count
            FROM {SOURCE_SCHEMA}."EM_MANIFEST" m
            WHERE m."ShippedDate" >= CAST(? AS date)
              AND m."GeneratorEPAID" IS NOT NULL
            GROUP BY m."GeneratorEPAID"
            ORDER BY COUNT(*) DESC
            LIMIT {TOP_N_GENERATORS};
            """,
        ),
        (
            "top transporters",
            f"""
            TRUNCATE TABLE {GRAPH_SCHEMA}.stg_top_transporters;
            INSERT INTO {GRAPH_SCHEMA}.stg_top_transporters ("TransporterEPAID", row_count)
            SELECT
                t."TransporterEPAID",
                COUNT(*) AS row_count
            FROM {SOURCE_SCHEMA}."EM_TRANSPORTER" t
            JOIN {SOURCE_SCHEMA}."EM_MANIFEST" m
              ON m."ManifestTrackingNumber" = t."ManifestTrackingNumber"
            WHERE m."ShippedDate" >= CAST(? AS date)
              AND t."TransporterEPAID" IS NOT NULL
            GROUP BY t."TransporterEPAID"
            ORDER BY COUNT(*) DESC
            LIMIT {TOP_N_TRANSPORTERS};
            """,
        ),
    ]

    with tqdm(total=len(steps), desc="Rebuild top filter tables", unit="table") as bar:
        for label, sql in steps:
            exec_sql_retry(conn, sql, params=(GRAPH_START_DATE,), label=f"Build {label}")
            bar.update(1)


def get_stageable_quarters(conn: pyodbc.Connection) -> List[QuarterInfo]:
    sql = f"""
    SELECT
        EXTRACT(YEAR FROM m."ShippedDate")::int AS year,
        EXTRACT(QUARTER FROM m."ShippedDate")::int AS quarter,
        (EXTRACT(YEAR FROM m."ShippedDate")::int * 100
         + EXTRACT(QUARTER FROM m."ShippedDate")::int) AS year_quarter,
        COUNT(DISTINCT m."ManifestTrackingNumber") AS manifest_count,
        COUNT(*) AS waste_line_count
    FROM {SOURCE_SCHEMA}."EM_MANIFEST" m
    JOIN {SOURCE_SCHEMA}."EM_WASTE_LINE" w
      ON w."ManifestTrackingNumber" = m."ManifestTrackingNumber"
    JOIN {GRAPH_SCHEMA}.stg_top_wasteline_numbers t25
      ON t25."WasteLineNumber" = w."WasteLineNumber"
    JOIN {GRAPH_SCHEMA}.stg_top_facilities tf
      ON tf."DesignatedFacilityEPAID" = m."DesignatedFacilityEPAID"
    JOIN {GRAPH_SCHEMA}.stg_top_generators tg
      ON tg."GeneratorEPAID" = m."GeneratorEPAID"
    WHERE m."ShippedDate" >= CAST(? AS date)
      AND m."GeneratorEPAID" IS NOT NULL
      AND m."DesignatedFacilityEPAID" IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY 1, 2;
    """
    rows = exec_sql_retry(conn, sql, params=(GRAPH_START_DATE,), label="Load stageable quarter list", fetch=True)
    return [QuarterInfo(**row) for row in rows]


def start_run(conn: pyodbc.Connection, mode: str) -> int:
    cur = conn.cursor()
    cur.execute(
        f"""
        INSERT INTO {GRAPH_SCHEMA}.graph_build_run (start_date, notes)
        VALUES (CAST(? AS date), ?)
        RETURNING run_id;
        """,
        GRAPH_START_DATE,
        f"Mode={mode}; top waste lines={TOP_N_WASTELINE_NUMBERS}; top facilities={TOP_N_FACILITIES}; "
        f"top generators={TOP_N_GENERATORS}; top transporters={TOP_N_TRANSPORTERS}",
    )
    run_id = cur.fetchone()[0]
    cur.close()
    return run_id


def finish_run(conn: pyodbc.Connection, run_id: int, status: str) -> None:
    exec_sql_retry(
        conn,
        f"""
        UPDATE {GRAPH_SCHEMA}.graph_build_run
        SET finished_at = now(),
            status = ?
        WHERE run_id = ?;
        """,
        params=(status, run_id),
        label="Finish build run",
    )


def mark_step(conn: pyodbc.Connection, run_id: int, year_quarter: int, step_name: str, status: str, detail: Optional[str] = None) -> None:
    if status == "RUNNING":
        exec_sql(
            conn,
            f"""
            INSERT INTO {GRAPH_SCHEMA}.graph_build_step (
                run_id, year_quarter, step_name, status, detail
            )
            VALUES (?, ?, ?, 'RUNNING', ?)
            ON CONFLICT (run_id, year_quarter, step_name)
            DO UPDATE SET
                started_at = now(),
                finished_at = NULL,
                status = 'RUNNING',
                detail = EXCLUDED.detail;
            """,
            params=(run_id, year_quarter, step_name, detail),
        )
    else:
        exec_sql(
            conn,
            f"""
            UPDATE {GRAPH_SCHEMA}.graph_build_step
            SET finished_at = now(),
                status = ?,
                detail = ?
            WHERE run_id = ?
              AND year_quarter = ?
              AND step_name = ?;
            """,
            params=(status, detail, run_id, year_quarter, step_name),
        )


def is_quarter_staged(conn: pyodbc.Connection, yq: int) -> bool:
    rows = exec_sql_retry(
        conn,
        f"""
        SELECT CASE WHEN EXISTS (
            SELECT 1
            FROM {GRAPH_SCHEMA}.graph_stage_status
            WHERE year_quarter = ?
              AND waste_line_fact_ready = true
              AND manifest_transporter_ready = true
            LIMIT 1
        ) THEN 1 ELSE 0 END AS is_ready;
        """,
        params=(yq,),
        fetch=True,
    )
    return bool(rows[0]["is_ready"])


def is_quarter_loaded(conn: pyodbc.Connection, yq: int) -> bool:
    rows = exec_sql_retry(
        conn,
        f"""
        SELECT CASE WHEN EXISTS (
            SELECT 1
            FROM {GRAPH_SCHEMA}.graph_edge_generator_facility_stream_qtr
            WHERE year_quarter = ?
            LIMIT 1
        ) THEN 1 ELSE 0 END AS exists_flag;
        """,
        params=(yq,),
        fetch=True,
    )
    return bool(rows[0]["exists_flag"])


def stage_quarter(conn: pyodbc.Connection, run_id: int, q: QuarterInfo) -> None:
    mark_step(conn, run_id, q.year_quarter, "stage_quarter", "RUNNING", f"Staging {q.label}")

    q_start_date, q_end_date = quarter_date_range(q.year, q.quarter)

    steps = [
        "delete old staged rows",
        "insert waste-line fact",
        "insert manifest-transporter map",
        "mark stage status",
    ]

    with tqdm(total=len(steps), desc=f"{q.label} stage", unit="step", leave=False) as step_bar:
        exec_sql_retry(
            conn,
            f"""
            DELETE FROM {GRAPH_SCHEMA}.stg_waste_line_fact_qtr WHERE year_quarter = ?;
            DELETE FROM {GRAPH_SCHEMA}.stg_manifest_transporter_qtr WHERE year_quarter = ?;
            DELETE FROM {GRAPH_SCHEMA}.graph_stage_status WHERE year_quarter = ?;
            """,
            params=(q.year_quarter, q.year_quarter, q.year_quarter),
            label=f"{q.label}: clear old stage rows",
        )
        step_bar.update(1)

        exec_sql_retry(
            conn,
            f"""
            INSERT INTO {GRAPH_SCHEMA}.stg_waste_line_fact_qtr (
                "ManifestTrackingNumber", "WasteLineNumber", "ShippedDate",
                year, quarter, year_quarter,
                "GeneratorEPAID", "DesignatedFacilityEPAID",
                usdot_hazardous_indicator, usdot_description, non_hazardous_waste_description,
                management_method_code, management_method_description,
                form_code, form_code_description,
                source_code, source_code_description,
                container_type_code, container_type_description,
                primary_description,
                waste_quantity_tons, waste_quantity_kg,
                unique_transporters, waste_stream_key
            )
            WITH manifest_base AS (
                SELECT
                    m."ManifestTrackingNumber",
                    m."ShippedDate",
                    EXTRACT(YEAR FROM m."ShippedDate")::int AS year,
                    EXTRACT(QUARTER FROM m."ShippedDate")::int AS quarter,
                    (EXTRACT(YEAR FROM m."ShippedDate")::int * 100
                     + EXTRACT(QUARTER FROM m."ShippedDate")::int) AS year_quarter,
                    m."GeneratorEPAID",
                    m."DesignatedFacilityEPAID"
                FROM {SOURCE_SCHEMA}."EM_MANIFEST" m
                JOIN {GRAPH_SCHEMA}.stg_top_facilities tf
                  ON tf."DesignatedFacilityEPAID" = m."DesignatedFacilityEPAID"
                JOIN {GRAPH_SCHEMA}.stg_top_generators tg
                  ON tg."GeneratorEPAID" = m."GeneratorEPAID"
                WHERE m."ShippedDate" >= CAST(? AS date)
                  AND m."ShippedDate" < CAST(? AS date)
                  AND m."GeneratorEPAID" IS NOT NULL
                  AND m."DesignatedFacilityEPAID" IS NOT NULL
            ),
            line_transporters AS (
                SELECT
                    t."ManifestTrackingNumber",
                    COUNT(DISTINCT t."TransporterEPAID") AS unique_transporters
                FROM {SOURCE_SCHEMA}."EM_TRANSPORTER" t
                JOIN manifest_base mb
                  ON mb."ManifestTrackingNumber" = t."ManifestTrackingNumber"
                JOIN {GRAPH_SCHEMA}.stg_top_transporters tt
                  ON tt."TransporterEPAID" = t."TransporterEPAID"
                WHERE t."TransporterEPAID" IS NOT NULL
                GROUP BY t."ManifestTrackingNumber"
            )
            SELECT
                mb."ManifestTrackingNumber",
                w."WasteLineNumber",
                mb."ShippedDate",
                mb.year,
                mb.quarter,
                mb.year_quarter,
                mb."GeneratorEPAID",
                mb."DesignatedFacilityEPAID",
                NULLIF(BTRIM(COALESCE(w."USDOTHazardousIndicator", '')), ''),
                NULLIF(BTRIM(COALESCE(w."USDOTDescription", '')), ''),
                NULLIF(BTRIM(COALESCE(w."NonHazardousWasteDescription", '')), ''),
                NULLIF(BTRIM(COALESCE(w."ManagementMethodCode", '')), ''),
                NULLIF(BTRIM(COALESCE(w."ManagementMethodDescription", '')), ''),
                NULLIF(BTRIM(COALESCE(w."FormCode", '')), ''),
                NULLIF(BTRIM(COALESCE(w."FormCodeDescription", '')), ''),
                NULLIF(BTRIM(COALESCE(w."SourceCode", '')), ''),
                NULLIF(BTRIM(COALESCE(w."SourceCodeDescription", '')), ''),
                NULLIF(BTRIM(COALESCE(w."ContainerTypeCode", '')), ''),
                NULLIF(BTRIM(COALESCE(w."ContainerTypeDescription", '')), ''),
                COALESCE(
                    NULLIF(BTRIM(COALESCE(w."USDOTDescription", '')), ''),
                    NULLIF(BTRIM(COALESCE(w."NonHazardousWasteDescription", '')), ''),
                    'Unknown Waste'
                ),
                COALESCE(w."WasteQuantityTons", 0.0),
                COALESCE(w."WasteQuantityKilograms", 0.0),
                COALESCE(lt.unique_transporters, 0),
                md5(
                    COALESCE(NULLIF(BTRIM(COALESCE(w."USDOTHazardousIndicator", '')), ''), '') || '|' ||
                    COALESCE(NULLIF(BTRIM(COALESCE(w."USDOTDescription", '')), ''), '') || '|' ||
                    COALESCE(NULLIF(BTRIM(COALESCE(w."NonHazardousWasteDescription", '')), ''), '') || '|' ||
                    COALESCE(NULLIF(BTRIM(COALESCE(w."ManagementMethodCode", '')), ''), '') || '|' ||
                    COALESCE(NULLIF(BTRIM(COALESCE(w."FormCode", '')), ''), '') || '|' ||
                    COALESCE(NULLIF(BTRIM(COALESCE(w."SourceCode", '')), ''), '') || '|' ||
                    COALESCE(NULLIF(BTRIM(COALESCE(w."ContainerTypeCode", '')), ''), '')
                )
            FROM manifest_base mb
            JOIN {SOURCE_SCHEMA}."EM_WASTE_LINE" w
              ON w."ManifestTrackingNumber" = mb."ManifestTrackingNumber"
            JOIN {GRAPH_SCHEMA}.stg_top_wasteline_numbers t25
              ON t25."WasteLineNumber" = w."WasteLineNumber"
            LEFT JOIN line_transporters lt
              ON lt."ManifestTrackingNumber" = mb."ManifestTrackingNumber";
            """,
            params=(q_start_date, q_end_date),
            label=f"{q.label}: stage waste-line fact",
        )
        step_bar.update(1)

        exec_sql_retry(
            conn,
            f"""
            INSERT INTO {GRAPH_SCHEMA}.stg_manifest_transporter_qtr (
                year_quarter,
                "ManifestTrackingNumber",
                "TransporterEPAID"
            )
            SELECT DISTINCT
                CAST(? AS integer),
                t."ManifestTrackingNumber",
                t."TransporterEPAID"
            FROM {SOURCE_SCHEMA}."EM_TRANSPORTER" t
            JOIN (
                SELECT DISTINCT "ManifestTrackingNumber"
                FROM {GRAPH_SCHEMA}.stg_waste_line_fact_qtr
                WHERE year_quarter = CAST(? AS integer)
            ) x
            ON x."ManifestTrackingNumber" = t."ManifestTrackingNumber"
            JOIN {GRAPH_SCHEMA}.stg_top_transporters tt
            ON tt."TransporterEPAID" = t."TransporterEPAID"
            WHERE t."TransporterEPAID" IS NOT NULL;
            """,
            params=(q.year_quarter, q.year_quarter),
            label=f"{q.label}: stage manifest-transporter map",
        )
        step_bar.update(1)

        exec_sql_retry(
            conn,
            f"""
            INSERT INTO {GRAPH_SCHEMA}.graph_stage_status (
                year_quarter,
                waste_line_fact_ready,
                manifest_transporter_ready,
                staged_at,
                notes
            )
            VALUES (CAST(? AS integer), true, true, now(), ?)
            ON CONFLICT (year_quarter) DO UPDATE
            SET waste_line_fact_ready = true,
                manifest_transporter_ready = true,
                staged_at = now(),
                notes = EXCLUDED.notes;
            """,
            params=(
                q.year_quarter,
                f"{q.label}: staged with top waste lines={TOP_N_WASTELINE_NUMBERS}, "
                f"facilities={TOP_N_FACILITIES}, generators={TOP_N_GENERATORS}, transporters={TOP_N_TRANSPORTERS}",
            ),
            label=f"{q.label}: mark stage status",
        )

    mark_step(conn, run_id, q.year_quarter, "stage_quarter", "SUCCESS", f"{q.label}: staged")


def load_quarter(conn: pyodbc.Connection, run_id: int, q: QuarterInfo) -> None:
    mark_step(conn, run_id, q.year_quarter, "load_quarter", "RUNNING", f"Loading {q.label}")

    steps = [
        "upsert waste streams",
        "insert generator->facility",
        "insert generator->transporter",
        "insert transporter->facility",
        "summarize",
    ]

    with tqdm(total=len(steps), desc=f"{q.label} load", unit="step", leave=False) as step_bar:
        exec_sql_retry(
            conn,
            f"""
            INSERT INTO {GRAPH_SCHEMA}.graph_waste_stream (
                waste_stream_key,
                usdot_hazardous_indicator,
                primary_description,
                usdot_description,
                non_hazardous_waste_description,
                management_method_code,
                management_method_description,
                form_code,
                form_code_description,
                source_code,
                source_code_description,
                container_type_code,
                container_type_description,
                display_name
            )
            SELECT DISTINCT
                s.waste_stream_key,
                s.usdot_hazardous_indicator,
                s.primary_description,
                s.usdot_description,
                s.non_hazardous_waste_description,
                s.management_method_code,
                s.management_method_description,
                s.form_code,
                s.form_code_description,
                s.source_code,
                s.source_code_description,
                s.container_type_code,
                s.container_type_description,
                COALESCE(s.primary_description, 'Unknown Waste')
                    || ' | Form: ' || COALESCE(s.form_code_description, s.form_code, 'Unknown')
                    || ' | Mgmt: ' || COALESCE(s.management_method_description, s.management_method_code, 'Unknown')
                    || ' | Source: ' || COALESCE(s.source_code_description, s.source_code, 'Unknown')
                    || ' | Container: ' || COALESCE(s.container_type_description, s.container_type_code, 'Unknown')
            FROM {GRAPH_SCHEMA}.stg_waste_line_fact_qtr s
            WHERE s.year_quarter = ?
            ON CONFLICT (waste_stream_key) DO UPDATE
            SET
                usdot_hazardous_indicator = EXCLUDED.usdot_hazardous_indicator,
                primary_description = EXCLUDED.primary_description,
                usdot_description = EXCLUDED.usdot_description,
                non_hazardous_waste_description = EXCLUDED.non_hazardous_waste_description,
                management_method_code = EXCLUDED.management_method_code,
                management_method_description = EXCLUDED.management_method_description,
                form_code = EXCLUDED.form_code,
                form_code_description = EXCLUDED.form_code_description,
                source_code = EXCLUDED.source_code,
                source_code_description = EXCLUDED.source_code_description,
                container_type_code = EXCLUDED.container_type_code,
                container_type_description = EXCLUDED.container_type_description,
                display_name = EXCLUDED.display_name;
            """,
            params=(q.year_quarter,),
            label=f"{q.label}: upsert graph_waste_stream",
        )
        step_bar.update(1)

        exec_sql_retry(
            conn,
            f"""
            INSERT INTO {GRAPH_SCHEMA}.graph_edge_generator_facility_stream_qtr (
                year, quarter, year_quarter,
                generator_node_id, facility_node_id, waste_stream_id,
                manifest_count, waste_line_count, total_waste_tons, total_waste_kg,
                unique_transporters, first_shipped_date, last_shipped_date
            )
            SELECT
                s.year,
                s.quarter,
                s.year_quarter,
                gn.graph_node_id,
                fn.graph_node_id,
                gws.waste_stream_id,
                COUNT(DISTINCT s."ManifestTrackingNumber"),
                COUNT(*),
                SUM(s.waste_quantity_tons),
                SUM(s.waste_quantity_kg),
                SUM(s.unique_transporters),
                MIN(s."ShippedDate"),
                MAX(s."ShippedDate")
            FROM {GRAPH_SCHEMA}.stg_waste_line_fact_qtr s
            JOIN {GRAPH_SCHEMA}.graph_node gn
              ON gn.node_type = 'GENERATOR'
             AND gn.business_id = s."GeneratorEPAID"
            JOIN {GRAPH_SCHEMA}.graph_node fn
              ON fn.node_type = 'FACILITY'
             AND fn.business_id = s."DesignatedFacilityEPAID"
            JOIN {GRAPH_SCHEMA}.graph_waste_stream gws
              ON gws.waste_stream_key = s.waste_stream_key
            WHERE s.year_quarter = ?
            GROUP BY
                s.year, s.quarter, s.year_quarter,
                gn.graph_node_id, fn.graph_node_id, gws.waste_stream_id
            ;
            """,
            params=(q.year_quarter,),
            label=f"{q.label}: insert generator->facility edges",
        )
        step_bar.update(1)

        exec_sql_retry(
            conn,
            f"""
            INSERT INTO {GRAPH_SCHEMA}.graph_edge_generator_transporter_stream_qtr (
                year, quarter, year_quarter,
                generator_node_id, transporter_node_id, waste_stream_id,
                manifest_count, waste_line_count, total_waste_tons, total_waste_kg,
                unique_facilities, first_shipped_date, last_shipped_date
            )
            SELECT
                s.year,
                s.quarter,
                s.year_quarter,
                gn.graph_node_id,
                tn.graph_node_id,
                gws.waste_stream_id,
                COUNT(DISTINCT s."ManifestTrackingNumber"),
                COUNT(*),
                SUM(s.waste_quantity_tons),
                SUM(s.waste_quantity_kg),
                COUNT(DISTINCT s."DesignatedFacilityEPAID"),
                MIN(s."ShippedDate"),
                MAX(s."ShippedDate")
            FROM {GRAPH_SCHEMA}.stg_waste_line_fact_qtr s
            JOIN {GRAPH_SCHEMA}.stg_manifest_transporter_qtr mt
              ON mt.year_quarter = s.year_quarter
             AND mt."ManifestTrackingNumber" = s."ManifestTrackingNumber"
            JOIN {GRAPH_SCHEMA}.graph_node gn
              ON gn.node_type = 'GENERATOR'
             AND gn.business_id = s."GeneratorEPAID"
            JOIN {GRAPH_SCHEMA}.graph_node tn
              ON tn.node_type = 'TRANSPORTER'
             AND tn.business_id = mt."TransporterEPAID"
            JOIN {GRAPH_SCHEMA}.graph_waste_stream gws
              ON gws.waste_stream_key = s.waste_stream_key
            WHERE s.year_quarter = ?
            GROUP BY
                s.year, s.quarter, s.year_quarter,
                gn.graph_node_id, tn.graph_node_id, gws.waste_stream_id
            ;
            """,
            params=(q.year_quarter,),
            label=f"{q.label}: insert generator->transporter edges",
        )
        step_bar.update(1)

        exec_sql_retry(
            conn,
            f"""
            INSERT INTO {GRAPH_SCHEMA}.graph_edge_transporter_facility_stream_qtr (
                year, quarter, year_quarter,
                transporter_node_id, facility_node_id, waste_stream_id,
                manifest_count, waste_line_count, total_waste_tons, total_waste_kg,
                unique_generators, first_shipped_date, last_shipped_date
            )
            SELECT
                s.year,
                s.quarter,
                s.year_quarter,
                tn.graph_node_id,
                fn.graph_node_id,
                gws.waste_stream_id,
                COUNT(DISTINCT s."ManifestTrackingNumber"),
                COUNT(*),
                SUM(s.waste_quantity_tons),
                SUM(s.waste_quantity_kg),
                COUNT(DISTINCT s."GeneratorEPAID"),
                MIN(s."ShippedDate"),
                MAX(s."ShippedDate")
            FROM {GRAPH_SCHEMA}.stg_waste_line_fact_qtr s
            JOIN {GRAPH_SCHEMA}.stg_manifest_transporter_qtr mt
              ON mt.year_quarter = s.year_quarter
             AND mt."ManifestTrackingNumber" = s."ManifestTrackingNumber"
            JOIN {GRAPH_SCHEMA}.graph_node tn
              ON tn.node_type = 'TRANSPORTER'
             AND tn.business_id = mt."TransporterEPAID"
            JOIN {GRAPH_SCHEMA}.graph_node fn
              ON fn.node_type = 'FACILITY'
             AND fn.business_id = s."DesignatedFacilityEPAID"
            JOIN {GRAPH_SCHEMA}.graph_waste_stream gws
              ON gws.waste_stream_key = s.waste_stream_key
            WHERE s.year_quarter = ?
            GROUP BY
                s.year, s.quarter, s.year_quarter,
                tn.graph_node_id, fn.graph_node_id, gws.waste_stream_id
            ;
            """,
            params=(q.year_quarter,),
            label=f"{q.label}: insert transporter->facility edges",
        )
        step_bar.update(1)

        summary = counts_after_quarter(conn, q)
        detail = (
            f"{q.label} loaded | gf_edges={summary['gf_edges']:,}, "
            f"gt_edges={summary['gt_edges']:,}, tf_edges={summary['tf_edges']:,}"
        )
        mark_step(conn, run_id, q.year_quarter, "load_quarter", "SUCCESS", detail)
        log(detail)
        step_bar.update(1)


def counts_after_quarter(conn: pyodbc.Connection, q: QuarterInfo) -> dict:
    rows = exec_sql_retry(
        conn,
        f"""
        SELECT
            (SELECT COUNT(*) FROM {GRAPH_SCHEMA}.graph_edge_generator_facility_stream_qtr WHERE year_quarter = ?) AS gf_edges,
            (SELECT COUNT(*) FROM {GRAPH_SCHEMA}.graph_edge_generator_transporter_stream_qtr WHERE year_quarter = ?) AS gt_edges,
            (SELECT COUNT(*) FROM {GRAPH_SCHEMA}.graph_edge_transporter_facility_stream_qtr WHERE year_quarter = ?) AS tf_edges;
        """,
        params=(q.year_quarter, q.year_quarter, q.year_quarter),
        fetch=True,
    )
    return rows[0]


def final_indexes(conn: pyodbc.Connection) -> None:
    sqls = [
        f'CREATE INDEX IF NOT EXISTS ix_gef_sq_yearq_generator ON {GRAPH_SCHEMA}.graph_edge_generator_facility_stream_qtr (year_quarter, generator_node_id);',
        f'CREATE INDEX IF NOT EXISTS ix_gef_sq_yearq_facility ON {GRAPH_SCHEMA}.graph_edge_generator_facility_stream_qtr (year_quarter, facility_node_id);',
        f'CREATE INDEX IF NOT EXISTS ix_gef_sq_yearq_stream ON {GRAPH_SCHEMA}.graph_edge_generator_facility_stream_qtr (year_quarter, waste_stream_id);',
        f'CREATE INDEX IF NOT EXISTS ix_get_sq_yearq_generator ON {GRAPH_SCHEMA}.graph_edge_generator_transporter_stream_qtr (year_quarter, generator_node_id);',
        f'CREATE INDEX IF NOT EXISTS ix_get_sq_yearq_transporter ON {GRAPH_SCHEMA}.graph_edge_generator_transporter_stream_qtr (year_quarter, transporter_node_id);',
        f'CREATE INDEX IF NOT EXISTS ix_get_sq_yearq_stream ON {GRAPH_SCHEMA}.graph_edge_generator_transporter_stream_qtr (year_quarter, waste_stream_id);',
        f'CREATE INDEX IF NOT EXISTS ix_gtf_sq_yearq_transporter ON {GRAPH_SCHEMA}.graph_edge_transporter_facility_stream_qtr (year_quarter, transporter_node_id);',
        f'CREATE INDEX IF NOT EXISTS ix_gtf_sq_yearq_facility ON {GRAPH_SCHEMA}.graph_edge_transporter_facility_stream_qtr (year_quarter, facility_node_id);',
        f'CREATE INDEX IF NOT EXISTS ix_gtf_sq_yearq_stream ON {GRAPH_SCHEMA}.graph_edge_transporter_facility_stream_qtr (year_quarter, waste_stream_id);',
    ]

    with tqdm(total=len(sqls), desc="Final index creation", unit="index") as pbar:
        for sql in sqls:
            exec_sql_retry(conn, sql)
            pbar.update(1)


def run_stage_mode(conn: pyodbc.Connection, run_id: int) -> None:
    #rebuild_top_filter_tables(conn)

    if os.path.exists("quarters.json"):
        with open("quarters.json", "r") as f:
            quarters_data = json.load(f)
            quarters = [QuarterInfo(**qd) for qd in quarters_data]
        log(f"Stage mode: loaded {len(quarters)} quarters from quarters.json.")

    else:
        quarters = get_stageable_quarters(conn)
        if not quarters:
            log("No quarters found for staging.")
            return

    # write list of quarters to file so that on the next run we can directly read from file instead of hitting the DB again
    with open("quarters.json", "w") as f:
        json.dump([q._asdict() for q in quarters], f, indent=2)
    log(f"Stage mode: {len(quarters)} quarters found for staging. Quarters written to quarters.json for future reference.")

    total_waste_lines = sum(q.waste_line_count for q in quarters)
    log(
        f"Stage mode: {len(quarters)} quarters from {quarters[0].label} to {quarters[-1].label}; "
        f"estimated filtered waste lines={total_waste_lines:,}"
    )

    completed_times = []
    with tqdm(total=len(quarters), desc="Stage quarter progress", unit="qtr") as quarter_bar:
        for idx, q in enumerate(quarters, start=1):
            if is_quarter_staged(conn, q.year_quarter):
                log(f"Skipping {q.label}: already staged.")
                quarter_bar.update(1)
                continue

            quarter_bar.set_postfix_str(
                f"{q.label} | manifests≈{q.manifest_count:,} | waste_lines≈{q.waste_line_count:,}"
            )

            q_started = time.time()
            stage_quarter(conn, run_id, q)
            q_elapsed = time.time() - q_started
            completed_times.append(q_elapsed)

            quarter_bar.update(1)

            avg = sum(completed_times) / len(completed_times)
            remaining = len(quarters) - idx
            eta_seconds = avg * remaining
            quarter_bar.set_postfix_str(
                f"last={q.label} | avg={format_seconds(avg)} | ETA={format_seconds(eta_seconds)}"
            )


def run_load_mode(conn: pyodbc.Connection, run_id: int) -> None:
    if os.path.exists("quarters.json"):
        with open("quarters.json", "r") as f:
            quarters_data = json.load(f)
            quarters = [QuarterInfo(**qd) for qd in quarters_data]
        log(f"Stage mode: loaded {len(quarters)} quarters from quarters.json.")

    else:
        quarters = get_stageable_quarters(conn)
        if not quarters:
            log("No quarters found for staging.")
            return

    completed_times = []
    with tqdm(total=len(quarters), desc="Load quarter progress", unit="qtr") as quarter_bar:
        for idx, q in enumerate(quarters, start=1):
            if not is_quarter_staged(conn, q.year_quarter):
                log(f"Skipping {q.label}: not staged yet.")
                quarter_bar.update(1)
                continue

            if is_quarter_loaded(conn, q.year_quarter):
                log(f"Skipping {q.label}: already loaded.")
                quarter_bar.update(1)
                continue

            quarter_bar.set_postfix_str(
                f"{q.label} | manifests≈{q.manifest_count:,} | waste_lines≈{q.waste_line_count:,}"
            )

            q_started = time.time()
            load_quarter(conn, run_id, q)
            q_elapsed = time.time() - q_started
            completed_times.append(q_elapsed)

            quarter_bar.update(1)

            avg = sum(completed_times) / len(completed_times)
            remaining = len(quarters) - idx
            eta_seconds = avg * remaining
            quarter_bar.set_postfix_str(
                f"last={q.label} | avg={format_seconds(avg)} | ETA={format_seconds(eta_seconds)}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build quarterly graph with persistent staging.")
    parser.add_argument(
        "--mode",
        choices=["stage", "load", "all"],
        default="all",
        help="stage = build persistent staging only; load = load graph from existing staging; all = both",
    )
    return parser.parse_args()


def main() -> int:
    conn = None
    run_id = None
    try:
        args = parse_args()
        cfg = load_config(CONFIG_PATH)
        conn = get_pg_connection(cfg)
        log("Connected to Postgres through pyodbc using config.yaml.")

        setup_steps = [
            ("Create metadata tables", lambda: create_metadata_tables(conn)),
            ("Create target tables", lambda: create_target_tables(conn)),
            ("Create staging tables", lambda: create_staging_tables(conn)),
            # ("Create indexes", lambda: create_core_indexes(conn)),
            # ("Refresh graph nodes", lambda: refresh_nodes(conn)),
        ]
        run_steps_with_tqdm("Setup", setup_steps)

        run_id = start_run(conn, args.mode)

        # if args.mode in {"stage", "all"}:
        #     run_stage_mode(conn, run_id)

        if args.mode in {"load", "all"}:
            run_load_mode(conn, run_id)
            final_indexes(conn)

        finish_run(conn, run_id, "SUCCESS")
        log("Build completed successfully.")
        return 0

    except KeyboardInterrupt:
        log("Interrupted by user.")
        if conn is not None and run_id is not None:
            try:
                finish_run(conn, run_id, "INTERRUPTED")
            except Exception:
                pass
        return 130

    except Exception:
        log("Build failed.")
        traceback.print_exc()
        if conn is not None and run_id is not None:
            try:
                finish_run(conn, run_id, "FAILED")
            except Exception:
                pass
        return 1

    finally:
        if conn is not None:
            conn.close()
            log("Connection closed.")


if __name__ == "__main__":
    sys.exit(main())
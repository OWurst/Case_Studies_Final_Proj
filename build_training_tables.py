import time
from pathlib import Path

import pyodbc
import yaml


# ============================================================
# CONFIG
# ============================================================
def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
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


# ============================================================
# SETTINGS
# ============================================================
ML_SCHEMA = "ml"

TOP_WASTE_CODES = 25
TOP_FACILITIES = 100
TOP_GENERATORS = 50
TOP_TRANSPORTERS = 500

NUM_LAGS = 4
CHANGE_THRESHOLD = 0.05  # 5%


# ============================================================
# SQL: SIMPLE VIEW
# wts-only
# row grain = facility + federal waste code + quarter
# ============================================================
def build_simple_mv_sql(
    top_waste_codes: int = TOP_WASTE_CODES,
    top_facilities: int = TOP_FACILITIES,
    top_generators: int = TOP_GENERATORS,
    num_lags: int = NUM_LAGS,
    change_threshold: float = CHANGE_THRESHOLD,
) -> str:
    lag_cols = ",\n            ".join(
        [
            f'LAG(a.qty_tons, {i}) OVER (PARTITION BY a.facility_epaid, a.waste_code ORDER BY a.year_quarter) AS qty_lag_{i}'
            for i in range(1, num_lags + 1)
        ]
    )

    lag_names = ",\n        ".join([f"qty_lag_{i}" for i in range(1, num_lags + 1)])
    lag_sum = " + ".join([f"COALESCE(qty_lag_{i}, 0)" for i in range(1, num_lags + 1)])

    return f"""
    DROP MATERIALIZED VIEW IF EXISTS {ML_SCHEMA}.mv_facility_waste_simple_train;

    CREATE MATERIALIZED VIEW {ML_SCHEMA}.mv_facility_waste_simple_train AS
    WITH base_lines AS (
        SELECT
            (EXTRACT(YEAR FROM m."ShippedDate")::int * 10 + EXTRACT(QUARTER FROM m."ShippedDate")::int) AS year_quarter,
            DATE_TRUNC('quarter', m."ShippedDate")::date AS quarter_start,
            m."GeneratorEPAID" AS generator_epaid,
            m."DesignatedFacilityEPAID" AS facility_epaid,
            wl."WasteLineNumber" AS waste_line_number,
            fwc."FederalWasteCode" AS waste_code,
            wl."WasteQuantityTons" AS qty_tons,
            wl."ManagementMethodCode" AS management_method_code,
            wl."FormCode" AS form_code,
            wl."SourceCode" AS source_code,
            wl."ManifestTrackingNumber" AS manifest_tracking_number
        FROM wts."EM_MANIFEST" m
        JOIN wts."EM_WASTE_LINE" wl
        ON m."ManifestTrackingNumber" = wl."ManifestTrackingNumber"
        LEFT JOIN wts."EM_FEDERAL_WASTE_CODE" fwc
        ON wl."ManifestTrackingNumber" = fwc."ManifestTrackingNumber"
        AND wl."WasteLineNumber" = fwc."WasteLineNumber"
        WHERE m."ShippedDate" IS NOT NULL
        AND m."GeneratorEPAID" IS NOT NULL
        AND m."DesignatedFacilityEPAID" IS NOT NULL
        AND wl."WasteQuantityTons" IS NOT NULL
        AND fwc."FederalWasteCode" IS NOT NULL
    ),

    top_waste AS (
        SELECT waste_code
        FROM base_lines
        GROUP BY waste_code
        ORDER BY SUM(qty_tons) DESC NULLS LAST
        LIMIT {top_waste_codes}
    ),

    top_facility AS (
        SELECT facility_epaid
        FROM base_lines
        GROUP BY facility_epaid
        ORDER BY SUM(qty_tons) DESC NULLS LAST
        LIMIT {top_facilities}
    ),

    top_generator AS (
        SELECT generator_epaid
        FROM base_lines
        GROUP BY generator_epaid
        ORDER BY SUM(qty_tons) DESC NULLS LAST
        LIMIT {top_generators}
    ),

    filtered AS (
        SELECT bl.*
        FROM base_lines bl
        JOIN top_waste tw
        ON bl.waste_code = tw.waste_code
        JOIN top_facility tf
        ON bl.facility_epaid = tf.facility_epaid
        JOIN top_generator tg
        ON bl.generator_epaid = tg.generator_epaid
    ),

    agg AS (
        SELECT
            year_quarter,
            quarter_start,
            facility_epaid,
            waste_code,
            COUNT(*) AS waste_line_row_count,
            COUNT(DISTINCT manifest_tracking_number) AS manifest_count,
            COUNT(DISTINCT generator_epaid) AS generator_count,
            SUM(qty_tons) AS qty_tons,
            AVG(qty_tons) AS avg_line_qty_tons,
            STDDEV_SAMP(qty_tons) AS std_line_qty_tons,
            MODE() WITHIN GROUP (ORDER BY management_method_code) AS mode_management_method_code,
            MODE() WITHIN GROUP (ORDER BY form_code) AS mode_form_code,
            MODE() WITHIN GROUP (ORDER BY source_code) AS mode_source_code
        FROM filtered
        GROUP BY
            year_quarter,
            quarter_start,
            facility_epaid,
            waste_code
    ),

    lagged AS (
        SELECT
            a.*,
            {lag_cols},
            LEAD(a.qty_tons, 1) OVER (
                PARTITION BY a.facility_epaid, a.waste_code
                ORDER BY a.year_quarter
            ) AS next_qty_tons
        FROM agg a
    )

    SELECT
        ROW_NUMBER() OVER (ORDER BY facility_epaid, waste_code, year_quarter) AS uid,
        year_quarter,
        quarter_start,
        facility_epaid,
        waste_code,
        waste_line_row_count,
        manifest_count,
        generator_count,
        qty_tons,
        avg_line_qty_tons,
        std_line_qty_tons,
        mode_management_method_code,
        mode_form_code,
        mode_source_code,
        {lag_names},
        ({lag_sum}) AS lag_qty_sum,
        CASE
            WHEN ({lag_sum}) = 0 THEN NULL
            ELSE qty_tons / NULLIF((({lag_sum})::double precision / {num_lags}), 0)
        END AS qty_vs_lag_avg_ratio,
        next_qty_tons,
        CASE
            WHEN next_qty_tons IS NULL THEN NULL
            WHEN qty_tons = 0 AND next_qty_tons > 0 THEN 'increase'
            WHEN qty_tons = 0 AND next_qty_tons = 0 THEN 'same'
            WHEN ((next_qty_tons - qty_tons) / NULLIF(qty_tons, 0)) >= {change_threshold} THEN 'increase'
            WHEN ((next_qty_tons - qty_tons) / NULLIF(qty_tons, 0)) <= -{change_threshold} THEN 'decrease'
            ELSE 'same'
        END AS target_class
    FROM lagged
    WHERE next_qty_tons IS NOT NULL;

    CREATE INDEX IF NOT EXISTS ix_mv_facility_waste_simple_train_key
        ON {ML_SCHEMA}.mv_facility_waste_simple_train (facility_epaid, waste_code, year_quarter);
    """


# ============================================================
# SQL: GRAPH VIEW
# uses only net quarter graph tables + graph dimensions
# no year tables, no stg tables
# row grain = facility node/business_id + waste_stream_id + quarter
# ============================================================
def build_graph_mv_sql(
    top_facilities: int = TOP_FACILITIES,
    top_generators: int = TOP_GENERATORS,
    top_transporters: int = TOP_TRANSPORTERS,
    num_lags: int = NUM_LAGS,
    change_threshold: float = CHANGE_THRESHOLD,
) -> str:
    lag_cols = ",\n            ".join(
        [
            f'LAG(gf.total_waste_tons, {i}) OVER (PARTITION BY gf.facility_epaid, gf.waste_stream_id ORDER BY gf.year_quarter) AS qty_lag_{i}'
            for i in range(1, num_lags + 1)
        ]
    )

    lag_names = ",\n        ".join([f"qty_lag_{i}" for i in range(1, num_lags + 1)])
    lag_sum = " + ".join([f"COALESCE(qty_lag_{i}, 0)" for i in range(1, num_lags + 1)])

    return f"""
DROP MATERIALIZED VIEW IF EXISTS {ML_SCHEMA}.mv_facility_waste_graph_train;

CREATE MATERIALIZED VIEW {ML_SCHEMA}.mv_facility_waste_graph_train AS
WITH
facility_totals AS (
    SELECT
        gn.business_id AS facility_epaid,
        SUM(gef.total_waste_tons) AS total_waste_tons
    FROM net.graph_edge_generator_facility_stream_qtr gef
    JOIN net.graph_node gn
      ON gef.facility_node_id = gn.graph_node_id
    GROUP BY gn.business_id
    ORDER BY SUM(gef.total_waste_tons) DESC NULLS LAST
    LIMIT {top_facilities}
),

generator_totals AS (
    SELECT
        gn.business_id AS generator_epaid,
        SUM(gef.total_waste_tons) AS total_waste_tons
    FROM net.graph_edge_generator_facility_stream_qtr gef
    JOIN net.graph_node gn
      ON gef.generator_node_id = gn.graph_node_id
    GROUP BY gn.business_id
    ORDER BY SUM(gef.total_waste_tons) DESC NULLS LAST
    LIMIT {top_generators}
),

transporter_totals AS (
    SELECT
        gn.business_id AS transporter_epaid,
        SUM(gtf.total_waste_tons) AS total_waste_tons
    FROM net.graph_edge_transporter_facility_stream_qtr gtf
    JOIN net.graph_node gn
      ON gtf.transporter_node_id = gn.graph_node_id
    GROUP BY gn.business_id
    ORDER BY SUM(gtf.total_waste_tons) DESC NULLS LAST
    LIMIT {top_transporters}
),

facility_stream_qtr AS (
    SELECT
        gef.year_quarter,
        gef.year,
        gef.quarter,
        gf_node.business_id AS facility_epaid,
        gef.facility_node_id,
        gef.waste_stream_id,
        gws.waste_stream_key,
        gws.display_name,
        gws.management_method_code,
        gws.form_code,
        gws.source_code,
        SUM(gef.manifest_count) AS manifest_count,
        SUM(gef.waste_line_count) AS waste_line_count,
        SUM(gef.total_waste_tons) AS total_waste_tons,
        SUM(gef.total_waste_kg) AS total_waste_kg,
        SUM(gef.unique_transporters) AS unique_transporters_from_gf,
        MIN(gef.first_shipped_date) AS first_shipped_date,
        MAX(gef.last_shipped_date) AS last_shipped_date
    FROM net.graph_edge_generator_facility_stream_qtr gef
    JOIN net.graph_node gf_node
      ON gef.facility_node_id = gf_node.graph_node_id
    JOIN net.graph_waste_stream gws
      ON gef.waste_stream_id = gws.waste_stream_id
    JOIN facility_totals tf
      ON gf_node.business_id = tf.facility_epaid
    GROUP BY
        gef.year_quarter,
        gef.year,
        gef.quarter,
        gf_node.business_id,
        gef.facility_node_id,
        gef.waste_stream_id,
        gws.waste_stream_key,
        gws.display_name,
        gws.management_method_code,
        gws.form_code,
        gws.source_code
),

gen_fac_features AS (
    SELECT
        gef.year_quarter,
        fac.business_id AS facility_epaid,
        gef.waste_stream_id,
        COUNT(*) AS generator_pair_count,
        COUNT(DISTINCT gen.business_id) AS generator_count,
        COALESCE(SUM(gef.manifest_count), 0) AS pair_manifest_count_sum,
        COALESCE(SUM(gef.waste_line_count), 0) AS pair_waste_line_count_sum,
        COALESCE(MAX(gef.total_waste_tons), 0) AS max_generator_pair_tons,
        COALESCE(AVG(gef.total_waste_tons), 0) AS avg_generator_pair_tons,
        COUNT(*) FILTER (WHERE COALESCE(gef.manifest_count, 0) >= 2) AS repeated_generator_pairs,
        COALESCE(SUM(gef.unique_transporters), 0) AS summed_unique_transporters_from_pairs
    FROM net.graph_edge_generator_facility_stream_qtr gef
    JOIN net.graph_node fac
      ON gef.facility_node_id = fac.graph_node_id
    JOIN net.graph_node gen
      ON gef.generator_node_id = gen.graph_node_id
    JOIN facility_totals tf
      ON fac.business_id = tf.facility_epaid
    JOIN generator_totals tg
      ON gen.business_id = tg.generator_epaid
    GROUP BY
        gef.year_quarter,
        fac.business_id,
        gef.waste_stream_id
),

gen_trans_features AS (
    SELECT
        getq.year_quarter,
        getq.waste_stream_id,
        COUNT(*) AS gen_trans_pair_count,
        COUNT(DISTINCT gen.business_id) AS gen_trans_generator_count,
        COUNT(DISTINCT trn.business_id) AS gen_trans_transporter_count,
        COALESCE(SUM(getq.manifest_count), 0) AS gen_trans_manifest_count_sum,
        COALESCE(SUM(getq.waste_line_count), 0) AS gen_trans_waste_line_count_sum,
        COALESCE(MAX(getq.total_waste_tons), 0) AS max_gen_trans_pair_tons
    FROM net.graph_edge_generator_transporter_stream_qtr getq
    JOIN net.graph_node gen
      ON getq.generator_node_id = gen.graph_node_id
    JOIN net.graph_node trn
      ON getq.transporter_node_id = trn.graph_node_id
    JOIN generator_totals tg
      ON gen.business_id = tg.generator_epaid
    JOIN transporter_totals tt
      ON trn.business_id = tt.transporter_epaid
    GROUP BY
        getq.year_quarter,
        getq.waste_stream_id
),

trans_fac_features AS (
    SELECT
        gtf.year_quarter,
        fac.business_id AS facility_epaid,
        gtf.waste_stream_id,
        COUNT(*) AS transporter_pair_count,
        COUNT(DISTINCT trn.business_id) AS transporter_count,
        COALESCE(SUM(gtf.manifest_count), 0) AS trans_fac_manifest_count_sum,
        COALESCE(SUM(gtf.waste_line_count), 0) AS trans_fac_waste_line_count_sum,
        COALESCE(MAX(gtf.total_waste_tons), 0) AS max_transporter_pair_tons,
        COALESCE(AVG(gtf.total_waste_tons), 0) AS avg_transporter_pair_tons,
        COUNT(*) FILTER (WHERE COALESCE(gtf.manifest_count, 0) >= 2) AS repeated_transporter_pairs,
        COALESCE(SUM(gtf.unique_generators), 0) AS summed_unique_generators_from_pairs
    FROM net.graph_edge_transporter_facility_stream_qtr gtf
    JOIN net.graph_node fac
      ON gtf.facility_node_id = fac.graph_node_id
    JOIN net.graph_node trn
      ON gtf.transporter_node_id = trn.graph_node_id
    JOIN facility_totals tf
      ON fac.business_id = tf.facility_epaid
    JOIN transporter_totals tt
      ON trn.business_id = tt.transporter_epaid
    GROUP BY
        gtf.year_quarter,
        fac.business_id,
        gtf.waste_stream_id
),

combined AS (
    SELECT
        fsq.year_quarter,
        fsq.year,
        fsq.quarter,
        fsq.facility_epaid,
        fsq.facility_node_id,
        fsq.waste_stream_id,
        fsq.waste_stream_key,
        fsq.display_name,
        fsq.management_method_code,
        fsq.form_code,
        fsq.source_code,
        fsq.manifest_count,
        fsq.waste_line_count,
        fsq.total_waste_tons,
        fsq.total_waste_kg,
        fsq.unique_transporters_from_gf,
        fsq.first_shipped_date,
        fsq.last_shipped_date,

        COALESCE(gff.generator_pair_count, 0) AS generator_pair_count,
        COALESCE(gff.generator_count, 0) AS generator_count,
        COALESCE(gff.pair_manifest_count_sum, 0) AS pair_manifest_count_sum,
        COALESCE(gff.pair_waste_line_count_sum, 0) AS pair_waste_line_count_sum,
        COALESCE(gff.max_generator_pair_tons, 0) AS max_generator_pair_tons,
        COALESCE(gff.avg_generator_pair_tons, 0) AS avg_generator_pair_tons,
        COALESCE(gff.repeated_generator_pairs, 0) AS repeated_generator_pairs,
        COALESCE(gff.summed_unique_transporters_from_pairs, 0) AS summed_unique_transporters_from_pairs,

        COALESCE(tff.transporter_pair_count, 0) AS transporter_pair_count,
        COALESCE(tff.transporter_count, 0) AS transporter_count,
        COALESCE(tff.trans_fac_manifest_count_sum, 0) AS trans_fac_manifest_count_sum,
        COALESCE(tff.trans_fac_waste_line_count_sum, 0) AS trans_fac_waste_line_count_sum,
        COALESCE(tff.max_transporter_pair_tons, 0) AS max_transporter_pair_tons,
        COALESCE(tff.avg_transporter_pair_tons, 0) AS avg_transporter_pair_tons,
        COALESCE(tff.repeated_transporter_pairs, 0) AS repeated_transporter_pairs,
        COALESCE(tff.summed_unique_generators_from_pairs, 0) AS summed_unique_generators_from_pairs,

        COALESCE(gtf2.gen_trans_pair_count, 0) AS global_gen_trans_pair_count_for_stream_qtr,
        COALESCE(gtf2.gen_trans_generator_count, 0) AS global_gen_trans_generator_count_for_stream_qtr,
        COALESCE(gtf2.gen_trans_transporter_count, 0) AS global_gen_trans_transporter_count_for_stream_qtr,
        COALESCE(gtf2.gen_trans_manifest_count_sum, 0) AS global_gen_trans_manifest_count_sum_for_stream_qtr,
        COALESCE(gtf2.gen_trans_waste_line_count_sum, 0) AS global_gen_trans_waste_line_count_sum_for_stream_qtr,
        COALESCE(gtf2.max_gen_trans_pair_tons, 0) AS global_max_gen_trans_pair_tons_for_stream_qtr
    FROM facility_stream_qtr fsq
    LEFT JOIN gen_fac_features gff
      ON fsq.year_quarter = gff.year_quarter
     AND fsq.facility_epaid = gff.facility_epaid
     AND fsq.waste_stream_id = gff.waste_stream_id
    LEFT JOIN trans_fac_features tff
      ON fsq.year_quarter = tff.year_quarter
     AND fsq.facility_epaid = tff.facility_epaid
     AND fsq.waste_stream_id = tff.waste_stream_id
    LEFT JOIN gen_trans_features gtf2
      ON fsq.year_quarter = gtf2.year_quarter
     AND fsq.waste_stream_id = gtf2.waste_stream_id
),

lagged AS (
    SELECT
        gf.*,
        {lag_cols},
        LAG(gf.manifest_count, 1) OVER (
            PARTITION BY gf.facility_epaid, gf.waste_stream_id
            ORDER BY gf.year_quarter
        ) AS manifest_count_lag_1,
        LAG(gf.generator_count, 1) OVER (
            PARTITION BY gf.facility_epaid, gf.waste_stream_id
            ORDER BY gf.year_quarter
        ) AS generator_count_lag_1,
        LAG(gf.transporter_count, 1) OVER (
            PARTITION BY gf.facility_epaid, gf.waste_stream_id
            ORDER BY gf.year_quarter
        ) AS transporter_count_lag_1,
        LAG(gf.generator_pair_count, 1) OVER (
            PARTITION BY gf.facility_epaid, gf.waste_stream_id
            ORDER BY gf.year_quarter
        ) AS generator_pair_count_lag_1,
        LAG(gf.transporter_pair_count, 1) OVER (
            PARTITION BY gf.facility_epaid, gf.waste_stream_id
            ORDER BY gf.year_quarter
        ) AS transporter_pair_count_lag_1,
        LEAD(gf.total_waste_tons, 1) OVER (
            PARTITION BY gf.facility_epaid, gf.waste_stream_id
            ORDER BY gf.year_quarter
        ) AS next_qty_tons
    FROM combined gf
)

SELECT
    ROW_NUMBER() OVER (ORDER BY facility_epaid, waste_stream_id, year_quarter) AS uid,
    year_quarter,
    year,
    quarter,
    facility_epaid,
    facility_node_id,
    waste_stream_id,
    waste_stream_key,
    display_name,
    management_method_code,
    form_code,
    source_code,

    manifest_count,
    waste_line_count,
    total_waste_tons,
    total_waste_kg,
    unique_transporters_from_gf,
    first_shipped_date,
    last_shipped_date,

    generator_pair_count,
    generator_count,
    pair_manifest_count_sum,
    pair_waste_line_count_sum,
    max_generator_pair_tons,
    avg_generator_pair_tons,
    repeated_generator_pairs,
    summed_unique_transporters_from_pairs,

    transporter_pair_count,
    transporter_count,
    trans_fac_manifest_count_sum,
    trans_fac_waste_line_count_sum,
    max_transporter_pair_tons,
    avg_transporter_pair_tons,
    repeated_transporter_pairs,
    summed_unique_generators_from_pairs,

    global_gen_trans_pair_count_for_stream_qtr,
    global_gen_trans_generator_count_for_stream_qtr,
    global_gen_trans_transporter_count_for_stream_qtr,
    global_gen_trans_manifest_count_sum_for_stream_qtr,
    global_gen_trans_waste_line_count_sum_for_stream_qtr,
    global_max_gen_trans_pair_tons_for_stream_qtr,

    {lag_names},

    manifest_count_lag_1,
    generator_count_lag_1,
    transporter_count_lag_1,
    generator_pair_count_lag_1,
    transporter_pair_count_lag_1,

    ({lag_sum}) AS lag_qty_sum,

    CASE
        WHEN ({lag_sum}) = 0 THEN NULL
        ELSE total_waste_tons / NULLIF((({lag_sum})::double precision / {num_lags}), 0)
    END AS qty_vs_lag_avg_ratio,

    CASE
        WHEN manifest_count_lag_1 IS NULL THEN NULL
        ELSE manifest_count - manifest_count_lag_1
    END AS manifest_count_qoq_delta,

    CASE
        WHEN generator_count_lag_1 IS NULL THEN NULL
        ELSE generator_count - generator_count_lag_1
    END AS generator_count_qoq_delta,

    CASE
        WHEN transporter_count_lag_1 IS NULL THEN NULL
        ELSE transporter_count - transporter_count_lag_1
    END AS transporter_count_qoq_delta,

    CASE
        WHEN generator_pair_count_lag_1 IS NULL THEN NULL
        ELSE generator_pair_count - generator_pair_count_lag_1
    END AS generator_pair_count_qoq_delta,

    CASE
        WHEN transporter_pair_count_lag_1 IS NULL THEN NULL
        ELSE transporter_pair_count - transporter_pair_count_lag_1
    END AS transporter_pair_count_qoq_delta,

    next_qty_tons,

    CASE
        WHEN next_qty_tons IS NULL THEN NULL
        WHEN total_waste_tons = 0 AND next_qty_tons > 0 THEN 'increase'
        WHEN total_waste_tons = 0 AND next_qty_tons = 0 THEN 'same'
        WHEN ((next_qty_tons - total_waste_tons) / NULLIF(total_waste_tons, 0)) >= {change_threshold} THEN 'increase'
        WHEN ((next_qty_tons - total_waste_tons) / NULLIF(total_waste_tons, 0)) <= -{change_threshold} THEN 'decrease'
        ELSE 'same'
    END AS target_class

FROM lagged
WHERE next_qty_tons IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_mv_facility_waste_graph_train_key
    ON {ML_SCHEMA}.mv_facility_waste_graph_train (facility_epaid, waste_stream_id, year_quarter);
"""


# ============================================================
# EXEC HELPERS
# ============================================================
def exec_sql_block(conn: pyodbc.Connection, label: str, sql: str) -> None:
    cur = conn.cursor()
    t0 = time.time()
    print(f"Starting: {label}")
    cur.execute(sql)
    elapsed = time.time() - t0
    print(f"Finished: {label} in {elapsed:.2f}s")


def analyze_mv(conn: pyodbc.Connection, mv_name: str) -> None:
    cur = conn.cursor()
    cur.execute(f"ANALYZE {mv_name};")


def count_rows(conn: pyodbc.Connection, mv_name: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {mv_name};")
    row = cur.fetchone()
    return int(row[0])


# ============================================================
# BUILDERS
# ============================================================
def build_simple_mv(conn: pyodbc.Connection) -> None:
    sql = build_simple_mv_sql()
    exec_sql_block(conn, "simple MV", sql)
    analyze_mv(conn, f"{ML_SCHEMA}.mv_facility_waste_simple_train")
    n = count_rows(conn, f"{ML_SCHEMA}.mv_facility_waste_simple_train")
    print(f"simple MV row count: {n}")


def build_graph_mv(conn: pyodbc.Connection) -> None:
    sql = build_graph_mv_sql()
    exec_sql_block(conn, "graph MV", sql)
    analyze_mv(conn, f"{ML_SCHEMA}.mv_facility_waste_graph_train")
    n = count_rows(conn, f"{ML_SCHEMA}.mv_facility_waste_graph_train")
    print(f"graph MV row count: {n}")


# ============================================================
# MAIN
# ============================================================
def main():
    cfg = load_config("config.yaml")
    conn = get_pg_connection(cfg)

    # Build each separately so one failure does not block the other.
    try:
        build_simple_mv(conn)
    except Exception as e:
        print(f"SIMPLE MV FAILED: {e}")

    try:
        build_graph_mv(conn)
    except Exception as e:
        print(f"GRAPH MV FAILED: {e}")

    conn.close()


if __name__ == "__main__":
    main()
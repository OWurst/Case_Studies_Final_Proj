import pyodbc
import yaml
import time
from pathlib import Path


# ============================================================
# CONFIG LOADING
# ============================================================
def load_config(path="config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ============================================================
# CONNECTION (YOUR EXACT PATTERN)
# ============================================================
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
# SQL BUILDERS
# ============================================================
def build_simple_mv_sql(num_lags=4, threshold=0.05) -> str:
    lag_cols = []
    for i in range(1, num_lags + 1):
        lag_cols.append(
            f"LAG(qty_tons, {i}) OVER (PARTITION BY facility_epaid, waste_code ORDER BY year_quarter) AS qty_lag_{i}"
        )

    lag_sql = ",\n        ".join(lag_cols)

    return f"""
    DROP MATERIALIZED VIEW IF EXISTS ml.mv_facility_waste_simple_train;

    CREATE MATERIALIZED VIEW ml.mv_facility_waste_simple_train AS
    WITH base AS (
        SELECT
            (EXTRACT(YEAR FROM m.receiveddate)::int * 10
                + EXTRACT(QUARTER FROM m.receiveddate)::int) AS year_quarter,
            m.designatedfacilityepaidnumber AS facility_epaid,
            m.generatorepaidnumber AS generator_epaid,
            fwc.federalwastecode AS waste_code,
            wl.quantity::numeric AS qty_tons
        FROM wts."EM_MANIFEST" m
        JOIN wts."EM_WASTE_LINE" wl
          ON m."ManifestTrackingNumber" = wl."ManifestTrackingNumber"
        LEFT JOIN wts.em_federal_waste_code fwc
          ON wl."ManifestTrackingNumber" = fwc."ManifestTrackingNumber"
         AND wl.wastelinenumber = fwc.wastelinenumber
        WHERE m.receiveddate IS NOT NULL
          AND fwc.federalwastecode IS NOT NULL
    ),

    top_waste AS (
        SELECT waste_code
        FROM base
        GROUP BY waste_code
        ORDER BY SUM(qty_tons) DESC
        LIMIT 25
    ),

    top_facilities AS (
        SELECT facility_epaid
        FROM base
        GROUP BY facility_epaid
        ORDER BY SUM(qty_tons) DESC
        LIMIT 100
    ),

    filtered AS (
        SELECT *
        FROM base
        WHERE waste_code IN (SELECT waste_code FROM top_waste)
          AND facility_epaid IN (SELECT facility_epaid FROM top_facilities)
    ),

    agg AS (
        SELECT
            year_quarter,
            facility_epaid,
            waste_code,
            SUM(qty_tons) AS qty_tons,
            COUNT(*) AS line_count,
            COUNT(DISTINCT generator_epaid) AS generator_count
        FROM filtered
        GROUP BY year_quarter, facility_epaid, waste_code
    ),

    lagged AS (
        SELECT
            *,
            {lag_sql},
            LEAD(qty_tons, 1) OVER (PARTITION BY facility_epaid, waste_code ORDER BY year_quarter) AS next_qty
        FROM agg
    )

    SELECT *,
        CASE
            WHEN next_qty IS NULL THEN NULL
            WHEN qty_tons = 0 AND next_qty > 0 THEN 'increase'
            WHEN qty_tons = 0 THEN 'same'
            WHEN ((next_qty - qty_tons) / NULLIF(qty_tons, 0)) >= {threshold} THEN 'increase'
            WHEN ((next_qty - qty_tons) / NULLIF(qty_tons, 0)) <= -{threshold} THEN 'decrease'
            ELSE 'same'
        END AS target_class
    FROM lagged
    WHERE next_qty IS NOT NULL;
    """


def build_graph_mv_sql(num_lags=4, threshold=0.05) -> str:
    lag_cols = []
    for i in range(1, num_lags + 1):
        lag_cols.append(
            f"LAG(qty_tons, {i}) OVER (PARTITION BY facility_epaid, waste_code ORDER BY year_quarter) AS qty_lag_{i}"
        )

    lag_sql = ",\n        ".join(lag_cols)

    return f"""
    DROP MATERIALIZED VIEW IF EXISTS ml.mv_facility_waste_graph_train;

    CREATE MATERIALIZED VIEW ml.mv_facility_waste_graph_train AS
    WITH base AS (
        SELECT
            (EXTRACT(YEAR FROM m.receiveddate)::int * 10
                + EXTRACT(QUARTER FROM m.receiveddate)::int) AS year_quarter,
            m.designatedfacilityepaidnumber AS facility_epaid,
            m.generatorepaidnumber AS generator_epaid,
            fwc.federalwastecode AS waste_code,
            t.transporterepaidnumber AS transporter_epaid,
            wl.quantity::numeric AS qty_tons
        FROM wts."EM_MANIFEST" m
        JOIN wts."EM_WASTE_LINE" wl
          ON m.manifesttrackingnumber = wl.manifesttrackingnumber
        LEFT JOIN wts.em_federal_waste_code fwc
          ON wl.manifesttrackingnumber = fwc.manifesttrackingnumber
         AND wl.wastelinenumber = fwc.wastelinenumber
        LEFT JOIN wts.em_transporter t
          ON m.manifesttrackingnumber = t.manifesttrackingnumber
        WHERE m.receiveddate IS NOT NULL
          AND fwc.federalwastecode IS NOT NULL
    ),

    agg AS (
        SELECT
            year_quarter,
            facility_epaid,
            waste_code,
            COUNT(DISTINCT generator_epaid) AS generator_count,
            COUNT(DISTINCT transporter_epaid) AS transporter_count,
            SUM(qty_tons) AS qty_tons
        FROM base
        GROUP BY year_quarter, facility_epaid, waste_code
    ),

    lagged AS (
        SELECT
            *,
            {lag_sql},
            LEAD(qty_tons, 1) OVER (PARTITION BY facility_epaid, waste_code ORDER BY year_quarter) AS next_qty
        FROM agg
    )

    SELECT *,
        CASE
            WHEN next_qty IS NULL THEN NULL
            WHEN ((next_qty - qty_tons) / NULLIF(qty_tons, 0)) >= {threshold} THEN 'increase'
            WHEN ((next_qty - qty_tons) / NULLIF(qty_tons, 0)) <= -{threshold} THEN 'decrease'
            ELSE 'same'
        END AS target_class
    FROM lagged
    WHERE next_qty IS NOT NULL;
    """


# ============================================================
# EXECUTION
# ============================================================
def run():
    cfg = load_config()
    conn = get_pg_connection(cfg)
    cursor = conn.cursor()

    print("Building SIMPLE materialized view...")
    t0 = time.time()
    cursor.execute(build_simple_mv_sql())
    print(f"Done in {round(time.time() - t0, 2)}s")

    print("Building GRAPH materialized view...")
    t0 = time.time()
    cursor.execute(build_graph_mv_sql())
    print(f"Done in {round(time.time() - t0, 2)}s")

    print("Refreshing stats...")
    cursor.execute("ANALYZE ml.mv_facility_waste_simple_train;")
    cursor.execute("ANALYZE ml.mv_facility_waste_graph_train;")

    print("Done.")


if __name__ == "__main__":
    run()
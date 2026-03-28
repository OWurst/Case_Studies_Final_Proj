BEGIN;

----------------------------------------------------------------------
-- 1) SCHEMA
----------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS net;

----------------------------------------------------------------------
-- 2) TABLES
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS net.graph_node (
    graph_node_id      bigserial PRIMARY KEY,
    node_type          varchar(30) NOT NULL,   -- GENERATOR, TRANSPORTER, FACILITY
    business_id        varchar(30) NOT NULL,   -- EPA ID / business key
    business_name      varchar(255),
    state_code         varchar(2),
    source_table       varchar(50),
    is_active          boolean NOT NULL DEFAULT true,
    created_at         timestamp NOT NULL DEFAULT now(),
    CONSTRAINT uq_graph_node UNIQUE (node_type, business_id)
);

CREATE TABLE IF NOT EXISTS net.graph_edge_generator_facility_year (
    edge_id                    bigserial PRIMARY KEY,
    year                       int NOT NULL,
    generator_node_id          bigint NOT NULL REFERENCES net.graph_node(graph_node_id),
    facility_node_id           bigint NOT NULL REFERENCES net.graph_node(graph_node_id),
    manifest_count             bigint,
    total_waste_tons           double precision,
    total_waste_kg             double precision,
    unique_transporters        bigint,
    unique_waste_codes         bigint,
    rejection_manifest_count   bigint,
    import_manifest_count      bigint,
    first_shipped_date         timestamp,
    last_shipped_date          timestamp,
    created_at                 timestamp NOT NULL DEFAULT now(),
    CONSTRAINT uq_gef UNIQUE (year, generator_node_id, facility_node_id)
);

CREATE TABLE IF NOT EXISTS net.graph_edge_generator_transporter_year (
    edge_id                    bigserial PRIMARY KEY,
    year                       int NOT NULL,
    generator_node_id          bigint NOT NULL REFERENCES net.graph_node(graph_node_id),
    transporter_node_id        bigint NOT NULL REFERENCES net.graph_node(graph_node_id),
    manifest_count             bigint,
    total_waste_tons           double precision,
    total_waste_kg             double precision,
    unique_facilities          bigint,
    unique_waste_codes         bigint,
    first_shipped_date         timestamp,
    last_shipped_date          timestamp,
    created_at                 timestamp NOT NULL DEFAULT now(),
    CONSTRAINT uq_get UNIQUE (year, generator_node_id, transporter_node_id)
);

CREATE TABLE IF NOT EXISTS net.graph_edge_transporter_facility_year (
    edge_id                    bigserial PRIMARY KEY,
    year                       int NOT NULL,
    transporter_node_id        bigint NOT NULL REFERENCES net.graph_node(graph_node_id),
    facility_node_id           bigint NOT NULL REFERENCES net.graph_node(graph_node_id),
    manifest_count             bigint,
    total_waste_tons           double precision,
    total_waste_kg             double precision,
    unique_generators          bigint,
    unique_waste_codes         bigint,
    first_shipped_date         timestamp,
    last_shipped_date          timestamp,
    created_at                 timestamp NOT NULL DEFAULT now(),
    CONSTRAINT uq_gtf UNIQUE (year, transporter_node_id, facility_node_id)
);

----------------------------------------------------------------------
-- 3) INDEXES ON net TABLES
----------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS ix_graph_node_type_business_id
    ON net.graph_node (node_type, business_id);

CREATE INDEX IF NOT EXISTS ix_graph_node_business_id
    ON net.graph_node (business_id);

CREATE INDEX IF NOT EXISTS ix_gef_year_generator
    ON net.graph_edge_generator_facility_year (year, generator_node_id);

CREATE INDEX IF NOT EXISTS ix_gef_year_facility
    ON net.graph_edge_generator_facility_year (year, facility_node_id);

CREATE INDEX IF NOT EXISTS ix_gef_year
    ON net.graph_edge_generator_facility_year (year);

CREATE INDEX IF NOT EXISTS ix_get_year_generator
    ON net.graph_edge_generator_transporter_year (year, generator_node_id);

CREATE INDEX IF NOT EXISTS ix_get_year_transporter
    ON net.graph_edge_generator_transporter_year (year, transporter_node_id);

CREATE INDEX IF NOT EXISTS ix_get_year
    ON net.graph_edge_generator_transporter_year (year);

CREATE INDEX IF NOT EXISTS ix_gtf_year_transporter
    ON net.graph_edge_transporter_facility_year (year, transporter_node_id);

CREATE INDEX IF NOT EXISTS ix_gtf_year_facility
    ON net.graph_edge_transporter_facility_year (year, facility_node_id);

CREATE INDEX IF NOT EXISTS ix_gtf_year
    ON net.graph_edge_transporter_facility_year (year);

----------------------------------------------------------------------
-- 4) OPTIONAL SOURCE INDEXES TO SPEED UP BUILD
----------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS ix_em_manifest_tracking
    ON wts."EM_MANIFEST" ("ManifestTrackingNumber");

CREATE INDEX IF NOT EXISTS ix_em_manifest_shipped_date
    ON wts."EM_MANIFEST" ("ShippedDate");

CREATE INDEX IF NOT EXISTS ix_em_manifest_generator
    ON wts."EM_MANIFEST" ("GeneratorEPAID");

CREATE INDEX IF NOT EXISTS ix_em_manifest_facility
    ON wts."EM_MANIFEST" ("DesignatedFacilityEPAID");

CREATE INDEX IF NOT EXISTS ix_em_import_tracking
    ON wts."EM_IMPORT" ("ManifestTrackingNumber");

CREATE INDEX IF NOT EXISTS ix_em_waste_line_tracking
    ON wts."EM_WASTE_LINE" ("ManifestTrackingNumber");

CREATE INDEX IF NOT EXISTS ix_em_transporter_tracking
    ON wts."EM_TRANSPORTER" ("ManifestTrackingNumber");

CREATE INDEX IF NOT EXISTS ix_em_transporter_epaid
    ON wts."EM_TRANSPORTER" ("TransporterEPAID");

CREATE INDEX IF NOT EXISTS ix_em_federal_waste_code_tracking
    ON wts."EM_FEDERAL_WASTE_CODE" ("ManifestTrackingNumber");

CREATE INDEX IF NOT EXISTS ix_em_state_waste_code_tracking
    ON wts."EM_STATE_WASTE_CODE" ("ManifestTrackingNumber");

----------------------------------------------------------------------
-- 5) CLEAR EDGE TABLES FOR REBUILD
--    Keep graph_node and upsert into it; rebuild edges from scratch.
----------------------------------------------------------------------

TRUNCATE TABLE
    net.graph_edge_generator_facility_year,
    net.graph_edge_generator_transporter_year,
    net.graph_edge_transporter_facility_year
RESTART IDENTITY;

----------------------------------------------------------------------
-- 6) LOAD graph_node
----------------------------------------------------------------------

-- Generators
INSERT INTO net.graph_node (node_type, business_id, business_name, state_code, source_table)
SELECT
    'GENERATOR',
    m."GeneratorEPAID",
    MAX(m."GeneratorName"),
    MAX(m."GeneratorLocationState"),
    'EM_MANIFEST'
FROM wts."EM_MANIFEST" m
WHERE m."GeneratorEPAID" IS NOT NULL
  AND m."ShippedDate" >= DATE '2018-01-01'
GROUP BY m."GeneratorEPAID"
ON CONFLICT (node_type, business_id) DO UPDATE
SET
    business_name = EXCLUDED.business_name,
    state_code = EXCLUDED.state_code,
    source_table = EXCLUDED.source_table,
    is_active = true;

-- Facilities
INSERT INTO net.graph_node (node_type, business_id, business_name, state_code, source_table)
SELECT
    'FACILITY',
    m."DesignatedFacilityEPAID",
    MAX(m."DesignatedFacilityName"),
    MAX(m."DesignatedFacilityLocationState"),
    'EM_MANIFEST'
FROM wts."EM_MANIFEST" m
WHERE m."DesignatedFacilityEPAID" IS NOT NULL
  AND m."ShippedDate" >= DATE '2018-01-01'
GROUP BY m."DesignatedFacilityEPAID"
ON CONFLICT (node_type, business_id) DO UPDATE
SET
    business_name = EXCLUDED.business_name,
    state_code = EXCLUDED.state_code,
    source_table = EXCLUDED.source_table,
    is_active = true;

-- Transporters
INSERT INTO net.graph_node (node_type, business_id, business_name, state_code, source_table)
SELECT
    'TRANSPORTER',
    t."TransporterEPAID",
    MAX(t."TransporterName"),
    NULL,
    'EM_TRANSPORTER'
FROM wts."EM_TRANSPORTER" t
JOIN wts."EM_MANIFEST" m
  ON m."ManifestTrackingNumber" = t."ManifestTrackingNumber"
WHERE t."TransporterEPAID" IS NOT NULL
  AND m."ShippedDate" >= DATE '2018-01-01'
GROUP BY t."TransporterEPAID"
ON CONFLICT (node_type, business_id) DO UPDATE
SET
    business_name = EXCLUDED.business_name,
    source_table = EXCLUDED.source_table,
    is_active = true;

----------------------------------------------------------------------
-- 7) BUILD generator -> facility yearly edges
----------------------------------------------------------------------

WITH manifest_base AS (
    SELECT
        m."ManifestTrackingNumber",
        EXTRACT(YEAR FROM m."ShippedDate")::int AS year,
        m."GeneratorEPAID",
        m."DesignatedFacilityEPAID",
        m."ShippedDate",
        m."RejectionIndicator",
        CASE
            WHEN i."ManifestTrackingNumber" IS NOT NULL THEN 1
            ELSE 0
        END AS is_import
    FROM wts."EM_MANIFEST" m
    LEFT JOIN wts."EM_IMPORT" i
        ON i."ManifestTrackingNumber" = m."ManifestTrackingNumber"
    WHERE m."ShippedDate" >= DATE '2018-01-01'
      AND m."GeneratorEPAID" IS NOT NULL
      AND m."DesignatedFacilityEPAID" IS NOT NULL
),
waste_agg AS (
    SELECT
        w."ManifestTrackingNumber",
        SUM(COALESCE(w."WasteQuantityTons", 0)) AS total_waste_tons,
        SUM(COALESCE(w."WasteQuantityKilograms", 0)) AS total_waste_kg
    FROM wts."EM_WASTE_LINE" w
    GROUP BY w."ManifestTrackingNumber"
),
transporter_agg AS (
    SELECT
        t."ManifestTrackingNumber",
        COUNT(DISTINCT t."TransporterEPAID") AS unique_transporters
    FROM wts."EM_TRANSPORTER" t
    WHERE t."TransporterEPAID" IS NOT NULL
    GROUP BY t."ManifestTrackingNumber"
),
waste_code_agg AS (
    SELECT
        x."ManifestTrackingNumber",
        COUNT(DISTINCT x.code_value) AS unique_waste_codes
    FROM (
        SELECT "ManifestTrackingNumber", "FederalWasteCode" AS code_value
        FROM wts."EM_FEDERAL_WASTE_CODE"
        WHERE "FederalWasteCode" IS NOT NULL

        UNION ALL

        SELECT "ManifestTrackingNumber", "StateWasteCode" AS code_value
        FROM wts."EM_STATE_WASTE_CODE"
        WHERE "StateWasteCode" IS NOT NULL
    ) x
    GROUP BY x."ManifestTrackingNumber"
)
INSERT INTO net.graph_edge_generator_facility_year (
    year,
    generator_node_id,
    facility_node_id,
    manifest_count,
    total_waste_tons,
    total_waste_kg,
    unique_transporters,
    unique_waste_codes,
    rejection_manifest_count,
    import_manifest_count,
    first_shipped_date,
    last_shipped_date
)
SELECT
    mb.year,
    gn.graph_node_id AS generator_node_id,
    fn.graph_node_id AS facility_node_id,
    COUNT(*) AS manifest_count,
    SUM(COALESCE(wa.total_waste_tons, 0)) AS total_waste_tons,
    SUM(COALESCE(wa.total_waste_kg, 0)) AS total_waste_kg,
    SUM(COALESCE(ta.unique_transporters, 0)) AS unique_transporters,
    SUM(COALESCE(wca.unique_waste_codes, 0)) AS unique_waste_codes,
    SUM(CASE WHEN mb."RejectionIndicator" = 'Y' THEN 1 ELSE 0 END) AS rejection_manifest_count,
    SUM(mb.is_import) AS import_manifest_count,
    MIN(mb."ShippedDate") AS first_shipped_date,
    MAX(mb."ShippedDate") AS last_shipped_date
FROM manifest_base mb
JOIN net.graph_node gn
  ON gn.node_type = 'GENERATOR'
 AND gn.business_id = mb."GeneratorEPAID"
JOIN net.graph_node fn
  ON fn.node_type = 'FACILITY'
 AND fn.business_id = mb."DesignatedFacilityEPAID"
LEFT JOIN waste_agg wa
  ON wa."ManifestTrackingNumber" = mb."ManifestTrackingNumber"
LEFT JOIN transporter_agg ta
  ON ta."ManifestTrackingNumber" = mb."ManifestTrackingNumber"
LEFT JOIN waste_code_agg wca
  ON wca."ManifestTrackingNumber" = mb."ManifestTrackingNumber"
GROUP BY
    mb.year,
    gn.graph_node_id,
    fn.graph_node_id
ON CONFLICT (year, generator_node_id, facility_node_id) DO UPDATE
SET
    manifest_count = EXCLUDED.manifest_count,
    total_waste_tons = EXCLUDED.total_waste_tons,
    total_waste_kg = EXCLUDED.total_waste_kg,
    unique_transporters = EXCLUDED.unique_transporters,
    unique_waste_codes = EXCLUDED.unique_waste_codes,
    rejection_manifest_count = EXCLUDED.rejection_manifest_count,
    import_manifest_count = EXCLUDED.import_manifest_count,
    first_shipped_date = EXCLUDED.first_shipped_date,
    last_shipped_date = EXCLUDED.last_shipped_date;

----------------------------------------------------------------------
-- 8) BUILD generator -> transporter yearly edges
----------------------------------------------------------------------

WITH manifest_base AS (
    SELECT
        m."ManifestTrackingNumber",
        EXTRACT(YEAR FROM m."ShippedDate")::int AS year,
        m."GeneratorEPAID",
        t."TransporterEPAID",
        m."DesignatedFacilityEPAID",
        m."ShippedDate"
    FROM wts."EM_MANIFEST" m
    JOIN wts."EM_TRANSPORTER" t
      ON t."ManifestTrackingNumber" = m."ManifestTrackingNumber"
    WHERE m."ShippedDate" >= DATE '2018-01-01'
      AND m."GeneratorEPAID" IS NOT NULL
      AND t."TransporterEPAID" IS NOT NULL
),
waste_agg AS (
    SELECT
        w."ManifestTrackingNumber",
        SUM(COALESCE(w."WasteQuantityTons", 0)) AS total_waste_tons,
        SUM(COALESCE(w."WasteQuantityKilograms", 0)) AS total_waste_kg
    FROM wts."EM_WASTE_LINE" w
    GROUP BY w."ManifestTrackingNumber"
),
waste_code_agg AS (
    SELECT
        x."ManifestTrackingNumber",
        COUNT(DISTINCT x.code_value) AS unique_waste_codes
    FROM (
        SELECT "ManifestTrackingNumber", "FederalWasteCode" AS code_value
        FROM wts."EM_FEDERAL_WASTE_CODE"
        WHERE "FederalWasteCode" IS NOT NULL

        UNION ALL

        SELECT "ManifestTrackingNumber", "StateWasteCode" AS code_value
        FROM wts."EM_STATE_WASTE_CODE"
        WHERE "StateWasteCode" IS NOT NULL
    ) x
    GROUP BY x."ManifestTrackingNumber"
)
INSERT INTO net.graph_edge_generator_transporter_year (
    year,
    generator_node_id,
    transporter_node_id,
    manifest_count,
    total_waste_tons,
    total_waste_kg,
    unique_facilities,
    unique_waste_codes,
    first_shipped_date,
    last_shipped_date
)
SELECT
    mb.year,
    gn.graph_node_id,
    tn.graph_node_id,
    COUNT(*) AS manifest_count,
    SUM(COALESCE(wa.total_waste_tons, 0)) AS total_waste_tons,
    SUM(COALESCE(wa.total_waste_kg, 0)) AS total_waste_kg,
    COUNT(DISTINCT mb."DesignatedFacilityEPAID") AS unique_facilities,
    SUM(COALESCE(wca.unique_waste_codes, 0)) AS unique_waste_codes,
    MIN(mb."ShippedDate"),
    MAX(mb."ShippedDate")
FROM manifest_base mb
JOIN net.graph_node gn
  ON gn.node_type = 'GENERATOR'
 AND gn.business_id = mb."GeneratorEPAID"
JOIN net.graph_node tn
  ON tn.node_type = 'TRANSPORTER'
 AND tn.business_id = mb."TransporterEPAID"
LEFT JOIN waste_agg wa
  ON wa."ManifestTrackingNumber" = mb."ManifestTrackingNumber"
LEFT JOIN waste_code_agg wca
  ON wca."ManifestTrackingNumber" = mb."ManifestTrackingNumber"
GROUP BY
    mb.year,
    gn.graph_node_id,
    tn.graph_node_id
ON CONFLICT (year, generator_node_id, transporter_node_id) DO UPDATE
SET
    manifest_count = EXCLUDED.manifest_count,
    total_waste_tons = EXCLUDED.total_waste_tons,
    total_waste_kg = EXCLUDED.total_waste_kg,
    unique_facilities = EXCLUDED.unique_facilities,
    unique_waste_codes = EXCLUDED.unique_waste_codes,
    first_shipped_date = EXCLUDED.first_shipped_date,
    last_shipped_date = EXCLUDED.last_shipped_date;

----------------------------------------------------------------------
-- 9) BUILD transporter -> facility yearly edges
----------------------------------------------------------------------

WITH manifest_base AS (
    SELECT
        m."ManifestTrackingNumber",
        EXTRACT(YEAR FROM m."ShippedDate")::int AS year,
        t."TransporterEPAID",
        m."DesignatedFacilityEPAID",
        m."GeneratorEPAID",
        m."ShippedDate"
    FROM wts."EM_MANIFEST" m
    JOIN wts."EM_TRANSPORTER" t
      ON t."ManifestTrackingNumber" = m."ManifestTrackingNumber"
    WHERE m."ShippedDate" >= DATE '2018-01-01'
      AND t."TransporterEPAID" IS NOT NULL
      AND m."DesignatedFacilityEPAID" IS NOT NULL
),
waste_agg AS (
    SELECT
        w."ManifestTrackingNumber",
        SUM(COALESCE(w."WasteQuantityTons", 0)) AS total_waste_tons,
        SUM(COALESCE(w."WasteQuantityKilograms", 0)) AS total_waste_kg
    FROM wts."EM_WASTE_LINE" w
    GROUP BY w."ManifestTrackingNumber"
),
waste_code_agg AS (
    SELECT
        x."ManifestTrackingNumber",
        COUNT(DISTINCT x.code_value) AS unique_waste_codes
    FROM (
        SELECT "ManifestTrackingNumber", "FederalWasteCode" AS code_value
        FROM wts."EM_FEDERAL_WASTE_CODE"
        WHERE "FederalWasteCode" IS NOT NULL

        UNION ALL

        SELECT "ManifestTrackingNumber", "StateWasteCode" AS code_value
        FROM wts."EM_STATE_WASTE_CODE"
        WHERE "StateWasteCode" IS NOT NULL
    ) x
    GROUP BY x."ManifestTrackingNumber"
)
INSERT INTO net.graph_edge_transporter_facility_year (
    year,
    transporter_node_id,
    facility_node_id,
    manifest_count,
    total_waste_tons,
    total_waste_kg,
    unique_generators,
    unique_waste_codes,
    first_shipped_date,
    last_shipped_date
)
SELECT
    mb.year,
    tn.graph_node_id,
    fn.graph_node_id,
    COUNT(*) AS manifest_count,
    SUM(COALESCE(wa.total_waste_tons, 0)) AS total_waste_tons,
    SUM(COALESCE(wa.total_waste_kg, 0)) AS total_waste_kg,
    COUNT(DISTINCT mb."GeneratorEPAID") AS unique_generators,
    SUM(COALESCE(wca.unique_waste_codes, 0)) AS unique_waste_codes,
    MIN(mb."ShippedDate"),
    MAX(mb."ShippedDate")
FROM manifest_base mb
JOIN net.graph_node tn
  ON tn.node_type = 'TRANSPORTER'
 AND tn.business_id = mb."TransporterEPAID"
JOIN net.graph_node fn
  ON fn.node_type = 'FACILITY'
 AND fn.business_id = mb."DesignatedFacilityEPAID"
LEFT JOIN waste_agg wa
  ON wa."ManifestTrackingNumber" = mb."ManifestTrackingNumber"
LEFT JOIN waste_code_agg wca
  ON wca."ManifestTrackingNumber" = mb."ManifestTrackingNumber"
GROUP BY
    mb.year,
    tn.graph_node_id,
    fn.graph_node_id
ON CONFLICT (year, transporter_node_id, facility_node_id) DO UPDATE
SET
    manifest_count = EXCLUDED.manifest_count,
    total_waste_tons = EXCLUDED.total_waste_tons,
    total_waste_kg = EXCLUDED.total_waste_kg,
    unique_generators = EXCLUDED.unique_generators,
    unique_waste_codes = EXCLUDED.unique_waste_codes,
    first_shipped_date = EXCLUDED.first_shipped_date,
    last_shipped_date = EXCLUDED.last_shipped_date;

COMMIT;

----------------------------------------------------------------------
-- 10) SANITY CHECKS
----------------------------------------------------------------------

-- Node counts by type
SELECT
    node_type,
    COUNT(*) AS cnt
FROM net.graph_node
GROUP BY node_type
ORDER BY node_type;

-- Generator -> Facility edge counts by year
SELECT
    year,
    COUNT(*) AS edge_count,
    SUM(manifest_count) AS total_manifest_count
FROM net.graph_edge_generator_facility_year
GROUP BY year
ORDER BY year;

-- Generator -> Transporter edge counts by year
SELECT
    year,
    COUNT(*) AS edge_count,
    SUM(manifest_count) AS total_manifest_count
FROM net.graph_edge_generator_transporter_year
GROUP BY year
ORDER BY year;

-- Transporter -> Facility edge counts by year
SELECT
    year,
    COUNT(*) AS edge_count,
    SUM(manifest_count) AS total_manifest_count
FROM net.graph_edge_transporter_facility_year
GROUP BY year
ORDER BY year;

-- Top generator/facility pairs
SELECT
    e.year,
    g.business_id AS generator_epaid,
    g.business_name AS generator_name,
    f.business_id AS facility_epaid,
    f.business_name AS facility_name,
    e.manifest_count,
    e.total_waste_tons
FROM net.graph_edge_generator_facility_year e
JOIN net.graph_node g
  ON g.graph_node_id = e.generator_node_id
JOIN net.graph_node f
  ON f.graph_node_id = e.facility_node_id
ORDER BY e.manifest_count DESC
LIMIT 25;
-- ---------------------------------------------------------------------------
-- Gold: dimensional marts served to the three consumer profiles.
--
-- Ops     -> fact_shipment_status_current  (one row per shipment, pre-aggregated)
-- Finance -> fact_daily_shipment_recon     (daily grain, restatement-aware)
-- DS      -> fact_shipment_events          (atomic grain, never aggregated on write)
-- ---------------------------------------------------------------------------

-- Atomic event fact. Grain: exactly one row per physical event.
-- At production volume this is ~120M rows/day; the equivalent Snowflake table
-- would be CLUSTER BY (event_date, event_type) -- deliberately not shipment_id,
-- whose cardinality makes automatic reclustering cost more than it saves.
CREATE TABLE IF NOT EXISTS gold.fact_shipment_events (
    event_id              VARCHAR PRIMARY KEY,
    event_date            DATE NOT NULL,
    event_ts              TIMESTAMP NOT NULL,
    ingest_ts             TIMESTAMP NOT NULL,
    shipment_key          VARCHAR NOT NULL,
    vehicle_key           VARCHAR,
    facility_key          VARCHAR,
    event_type            VARCHAR NOT NULL,
    status                VARCHAR,
    geo_lat               DOUBLE,
    geo_lon               DOUBLE,
    temperature_c         DOUBLE,
    delivery_window_start TIMESTAMP,
    delivery_window_end   TIMESTAMP,
    source                VARCHAR,
    schema_version        VARCHAR,
    payload_hash          VARCHAR,
    updated_at            TIMESTAMP NOT NULL
);

-- Accumulating snapshot. Grain: one row per shipment. This is what the Ops
-- dashboard queries, which is how a sub-10s SLA over 90 days of history is met
-- without scanning the atomic fact at all.
CREATE TABLE IF NOT EXISTS gold.fact_shipment_status_current (
    shipment_key          VARCHAR PRIMARY KEY,
    shipment_id           VARCHAR NOT NULL,
    current_status        VARCHAR,
    last_event_ts         TIMESTAMP,
    pickup_ts             TIMESTAMP,
    in_transit_ts         TIMESTAMP,
    exception_ts          TIMESTAMP,
    delivered_ts          TIMESTAMP,
    last_vehicle_id       VARCHAR,
    last_hub_code         VARCHAR,
    last_geo_lat          DOUBLE,
    last_geo_lon          DOUBLE,
    delivery_window_start TIMESTAMP,
    delivery_window_end   TIMESTAMP,
    event_count           BIGINT DEFAULT 0,
    is_window_breached    BOOLEAN,
    updated_at            TIMESTAMP NOT NULL
);

-- Finance. Restatements are explicit and logged, never a silent overwrite.
CREATE TABLE IF NOT EXISTS gold.fact_daily_shipment_recon (
    recon_date       DATE NOT NULL,
    hub_code         VARCHAR NOT NULL,
    shipments        BIGINT,
    events           BIGINT,
    delivered        BIGINT,
    exceptions       BIGINT,
    window_breaches  BIGINT,
    restated_at      TIMESTAMP,
    restatement_note VARCHAR,
    updated_at       TIMESTAMP NOT NULL,
    PRIMARY KEY (recon_date, hub_code)
);

-- Vehicle-to-shipment assignment is a high-churn relationship, so it is an
-- assignment fact with an effective window -- NOT SCD2 on dim_vehicle, which
-- 85,000 daily reassignments would bloat into millions of rows.
CREATE TABLE IF NOT EXISTS gold.fact_vehicle_assignment (
    assignment_key VARCHAR PRIMARY KEY,
    shipment_key   VARCHAR NOT NULL,
    vehicle_key    VARCHAR NOT NULL,
    effective_from TIMESTAMP NOT NULL,
    effective_to   TIMESTAMP,
    is_current     BOOLEAN DEFAULT TRUE
);

-- Dimensions ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_vehicle (
    vehicle_key    VARCHAR NOT NULL,
    vehicle_id     VARCHAR NOT NULL,
    capacity_class VARCHAR,
    is_cold_chain  BOOLEAN,
    valid_from     TIMESTAMP NOT NULL,
    valid_to       TIMESTAMP,
    is_current     BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (vehicle_key, valid_from)
);

CREATE TABLE IF NOT EXISTS gold.dim_facility (
    facility_key VARCHAR NOT NULL,
    hub_code     VARCHAR NOT NULL,
    country      VARCHAR,
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP,
    is_current   BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (facility_key, valid_from)
);

CREATE TABLE IF NOT EXISTS gold.dim_shipment (
    shipment_key VARCHAR NOT NULL,
    shipment_id  VARCHAR NOT NULL,
    origin_hub   VARCHAR,
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP,
    is_current   BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (shipment_key, valid_from)
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key   DATE PRIMARY KEY,
    year       INTEGER,
    quarter    INTEGER,
    month      INTEGER,
    day        INTEGER,
    day_name   VARCHAR,
    is_weekend BOOLEAN
);

-- ---------------------------------------------------------------------------
-- Raw Data Vault
--
-- Insert-only, fully lineaged, and structurally immune to upstream renames:
-- a new or moved source field lands in a new satellite or in the VARIANT
-- payload without breaking any existing satellite or downstream mart. That is
-- the property the monolithic `shipment_events` table did not have, and why
-- one renamed field took the warehouse down for 11 days.
--
-- All surrogate keys are deterministic SHA-1 hashes computed at ingest, not
-- warehouse sequences. Sequences would serialise the load and become the
-- bottleneck at 4,500 events/sec; hash keys let hubs, links and satellites
-- load in parallel with no lookup and no ordering dependency.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- Hubs: the business keys, and nothing else. --------------------------------
CREATE TABLE IF NOT EXISTS raw.hub_shipment (
    shipment_hk VARCHAR PRIMARY KEY,
    shipment_id VARCHAR NOT NULL,
    load_dts    TIMESTAMP NOT NULL,
    rec_src     VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.hub_vehicle (
    vehicle_hk VARCHAR PRIMARY KEY,
    vehicle_id VARCHAR NOT NULL,
    load_dts   TIMESTAMP NOT NULL,
    rec_src    VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.hub_facility (
    facility_hk VARCHAR PRIMARY KEY,
    hub_code    VARCHAR NOT NULL,
    country     VARCHAR,
    load_dts    TIMESTAMP NOT NULL,
    rec_src     VARCHAR NOT NULL
);

-- Link: the relationship, at its own grain. ---------------------------------
CREATE TABLE IF NOT EXISTS raw.lnk_shipment_vehicle_facility (
    link_hk     VARCHAR PRIMARY KEY,
    shipment_hk VARCHAR NOT NULL,
    vehicle_hk  VARCHAR,
    facility_hk VARCHAR,
    load_dts    TIMESTAMP NOT NULL,
    rec_src     VARCHAR NOT NULL
);

-- Satellites: descriptive context, versioned by hash_diff. ------------------
CREATE TABLE IF NOT EXISTS raw.sat_shipment_status (
    shipment_hk           VARCHAR NOT NULL,
    event_ts              TIMESTAMP NOT NULL,
    load_dts              TIMESTAMP NOT NULL,
    hash_diff             VARCHAR NOT NULL,
    status                VARCHAR,
    geo_lat               DOUBLE,
    geo_lon               DOUBLE,
    delivery_window_start TIMESTAMP,
    delivery_window_end   TIMESTAMP,
    schema_version        VARCHAR,
    rec_src               VARCHAR NOT NULL,
    PRIMARY KEY (shipment_hk, event_ts, hash_diff)
);

CREATE TABLE IF NOT EXISTS raw.sat_vehicle_telemetry (
    vehicle_hk    VARCHAR NOT NULL,
    event_ts      TIMESTAMP NOT NULL,
    load_dts      TIMESTAMP NOT NULL,
    hash_diff     VARCHAR NOT NULL,
    temperature_c DOUBLE,
    geo_lat       DOUBLE,
    geo_lon       DOUBLE,
    rec_src       VARCHAR NOT NULL,
    PRIMARY KEY (vehicle_hk, event_ts, hash_diff)
);

-- The VARIANT satellite. Source-specific keys live here untyped, so a partner
-- adding a field is a non-event rather than an incident.
CREATE TABLE IF NOT EXISTS raw.sat_event_metadata (
    event_id VARCHAR PRIMARY KEY,
    load_dts TIMESTAMP NOT NULL,
    payload  JSON,
    rec_src  VARCHAR NOT NULL
);

-- Operational metadata -------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.load_watermark (
    table_name   VARCHAR PRIMARY KEY,
    watermark_ts TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS meta.schema_change_log (
    changed_at     TIMESTAMP NOT NULL,
    schema_version VARCHAR NOT NULL,
    action         VARCHAR NOT NULL,
    actor          VARCHAR,
    note           VARCHAR
);

CREATE TABLE IF NOT EXISTS meta.alerts (
    raised_at TIMESTAMP NOT NULL,
    severity  VARCHAR NOT NULL,
    monitor   VARCHAR NOT NULL,
    message   VARCHAR NOT NULL,
    detail    JSON
);

CREATE TABLE IF NOT EXISTS meta.pipeline_metrics (
    observed_at     TIMESTAMP NOT NULL,
    batch_id        BIGINT,
    records_in      BIGINT,
    records_merged  BIGINT,
    duplicates      BIGINT,
    quarantined     BIGINT,
    late_arrivals   BIGINT,
    merge_ms        DOUBLE,
    scanned_rows    BIGINT,
    promotion_state VARCHAR
);

CREATE TABLE IF NOT EXISTS quarantine.late_arrivals (
    event_id     VARCHAR,
    shipment_id  VARCHAR,
    event_ts     TIMESTAMP,
    ingest_ts    TIMESTAMP,
    age_hours    DOUBLE,
    reason       VARCHAR
);

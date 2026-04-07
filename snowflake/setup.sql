-- =============================================================================
-- Snowflake environment setup for the Marketing Analytics Pipeline
-- Run this as ACCOUNTADMIN (or a role with CREATE DATABASE / CREATE ROLE privs)
-- =============================================================================

-- ── Warehouse ────────────────────────────────────────────────────────────────

CREATE WAREHOUSE IF NOT EXISTS MARKETING_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Warehouse for the marketing analytics pipeline';

-- ── Database & schemas ───────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS MARKETING_ANALYTICS;

USE DATABASE MARKETING_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Bronze layer — raw CSV data loaded as-is';
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Silver layer — cleaned and renamed by dbt staging models';
CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Gold layer — business-level metrics produced by dbt mart models';

-- ── Service role & user ──────────────────────────────────────────────────────

CREATE ROLE IF NOT EXISTS MARKETING_PIPELINE_ROLE
    COMMENT = 'Role used by the pipeline service account';

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE MARKETING_WH TO ROLE MARKETING_PIPELINE_ROLE;

-- Grant database-level privileges
GRANT USAGE          ON DATABASE MARKETING_ANALYTICS TO ROLE MARKETING_PIPELINE_ROLE;
GRANT CREATE SCHEMA  ON DATABASE MARKETING_ANALYTICS TO ROLE MARKETING_PIPELINE_ROLE;
GRANT USAGE          ON ALL SCHEMAS IN DATABASE MARKETING_ANALYTICS TO ROLE MARKETING_PIPELINE_ROLE;

-- RAW: the loader writes here
GRANT CREATE TABLE ON SCHEMA MARKETING_ANALYTICS.RAW     TO ROLE MARKETING_PIPELINE_ROLE;
GRANT CREATE STAGE ON SCHEMA MARKETING_ANALYTICS.RAW     TO ROLE MARKETING_PIPELINE_ROLE;
GRANT SELECT, INSERT, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA MARKETING_ANALYTICS.RAW TO ROLE MARKETING_PIPELINE_ROLE;
GRANT SELECT, INSERT, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA MARKETING_ANALYTICS.RAW TO ROLE MARKETING_PIPELINE_ROLE;

-- Transfer ownership of RAW tables so the pipeline role can use table stages
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA MARKETING_ANALYTICS.RAW
    TO ROLE MARKETING_PIPELINE_ROLE COPY CURRENT GRANTS;

-- STAGING + MARTS: dbt creates and selects here
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA MARKETING_ANALYTICS.STAGING TO ROLE MARKETING_PIPELINE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA MARKETING_ANALYTICS.MARTS   TO ROLE MARKETING_PIPELINE_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MARKETING_ANALYTICS.STAGING TO ROLE MARKETING_PIPELINE_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MARKETING_ANALYTICS.MARTS   TO ROLE MARKETING_PIPELINE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARKETING_ANALYTICS.STAGING TO ROLE MARKETING_PIPELINE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARKETING_ANALYTICS.MARTS   TO ROLE MARKETING_PIPELINE_ROLE;

-- Service user (change the password before running!)
CREATE USER IF NOT EXISTS MARKETING_PIPELINE_USER
    PASSWORD           = 'CHANGE_ME_BEFORE_RUNNING'
    DEFAULT_WAREHOUSE  = MARKETING_WH
    DEFAULT_ROLE       = MARKETING_PIPELINE_ROLE
    DEFAULT_NAMESPACE  = MARKETING_ANALYTICS.RAW
    COMMENT            = 'Service account for the marketing analytics pipeline';

GRANT ROLE MARKETING_PIPELINE_ROLE TO USER MARKETING_PIPELINE_USER;

-- ── Raw tables ───────────────────────────────────────────────────────────────

USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS META_ADS (
    date              DATE,
    channel           VARCHAR(20),
    campaign_id       VARCHAR(20),
    campaign_name     VARCHAR(200),
    objective         VARCHAR(30),
    ad_set_id         VARCHAR(20),
    ad_set_name       VARCHAR(200),
    impressions       INT,
    clicks            INT,
    spend             DECIMAL(12,2),
    conversions       INT,
    conversion_value  DECIMAL(12,2),
    cpc               DECIMAL(10,4),
    currency          VARCHAR(5),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS GOOGLE_ADS (
    date              DATE,
    channel           VARCHAR(20),
    campaign_id       VARCHAR(20),
    campaign_name     VARCHAR(200),
    objective         VARCHAR(30),
    ad_set_id         VARCHAR(20),
    ad_set_name       VARCHAR(200),
    impressions       INT,
    clicks            INT,
    spend             DECIMAL(12,2),
    conversions       INT,
    conversion_value  DECIMAL(12,2),
    cpc               DECIMAL(10,4),
    currency          VARCHAR(5),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS TIKTOK_ADS (
    date              DATE,
    channel           VARCHAR(20),
    campaign_id       VARCHAR(20),
    campaign_name     VARCHAR(200),
    objective         VARCHAR(30),
    ad_set_id         VARCHAR(20),
    ad_set_name       VARCHAR(200),
    impressions       INT,
    clicks            INT,
    spend             DECIMAL(12,2),
    conversions       INT,
    conversion_value  DECIMAL(12,2),
    cpc               DECIMAL(10,4),
    currency          VARCHAR(5),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

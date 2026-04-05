USE FinalProject;
GO


-- ============================================================
-- STEP 1 — DROP VIEWS (must drop before tables)
-- ============================================================

IF OBJECT_ID('dbo.vw_police_response_summary', 'V') IS NOT NULL DROP VIEW dbo.vw_police_response_summary;
IF OBJECT_ID('dbo.vw_crime_by_neighborhood',   'V') IS NOT NULL DROP VIEW dbo.vw_crime_by_neighborhood;
IF OBJECT_ID('dbo.vw_high_risk_incidents',     'V') IS NOT NULL DROP VIEW dbo.vw_high_risk_incidents;
IF OBJECT_ID('dbo.vw_ml_features',             'V') IS NOT NULL DROP VIEW dbo.vw_ml_features;

PRINT '✔ Step 1: All views dropped.';
GO


-- ============================================================
-- STEP 2 — DROP FACT TABLE (must drop before dimensions)
-- Fact table holds all FKs — drop it first to release refs.
-- ============================================================

IF OBJECT_ID('dbo.fact_incident', 'U') IS NOT NULL DROP TABLE dbo.fact_incident;

PRINT '✔ Step 2: fact_incident dropped.';
GO


-- ============================================================
-- STEP 3 — DROP DIMENSION TABLES (reverse dependency order)
-- dim_location references dim_municipality — drop location first.
-- ============================================================

IF OBJECT_ID('dbo.dim_location',        'U') IS NOT NULL DROP TABLE dbo.dim_location;
IF OBJECT_ID('dbo.dim_municipality',    'U') IS NOT NULL DROP TABLE dbo.dim_municipality;
IF OBJECT_ID('dbo.dim_date',            'U') IS NOT NULL DROP TABLE dbo.dim_date;
IF OBJECT_ID('dbo.dim_crime_type',      'U') IS NOT NULL DROP TABLE dbo.dim_crime_type;
IF OBJECT_ID('dbo.dim_weapon',          'U') IS NOT NULL DROP TABLE dbo.dim_weapon;
IF OBJECT_ID('dbo.dim_demographics',    'U') IS NOT NULL DROP TABLE dbo.dim_demographics;
IF OBJECT_ID('dbo.dim_socioeconomic',   'U') IS NOT NULL DROP TABLE dbo.dim_socioeconomic;
IF OBJECT_ID('dbo.dim_police_response', 'U') IS NOT NULL DROP TABLE dbo.dim_police_response;
IF OBJECT_ID('dbo.dim_risk',            'U') IS NOT NULL DROP TABLE dbo.dim_risk;
IF OBJECT_ID('dbo.stg_crime_incidents', 'U') IS NOT NULL DROP TABLE dbo.stg_crime_incidents;

PRINT '✔ Step 3: All dimension and staging tables dropped.';
GO


-- ============================================================
-- STEP 4 — CREATE STAGING TABLE
-- Absorbs raw + ETL-derived data before normalization.
-- No constraints — designed to accept messy input.
-- offender_age_masked, victim_age_masked, crime_risk_level
-- are derived in ETL_Engine.py and written here.
-- ============================================================

CREATE TABLE dbo.stg_crime_incidents (
    incident_id                 INT,
    municipality                NVARCHAR(100),
    postal_code                 NVARCHAR(20),
    latitude                    FLOAT,
    longitude                   FLOAT,
    neighborhood                NVARCHAR(100),
    incident_date               DATE,
    reported_date               DATE,
    year                        INT,
    month                       INT,
    day_of_week                 NVARCHAR(20),
    hour_of_day                 INT,
    crime_category              NVARCHAR(100),
    crime_subtype               NVARCHAR(100),
    weapon_used                 NVARCHAR(50),
    violent_flag                NVARCHAR(10),
    property_damage_value       FLOAT,
    police_service              NVARCHAR(100),
    response_time_minutes       FLOAT,
    units_dispatched            FLOAT,
    arrest_made                 NVARCHAR(10),
    offender_age_masked         NVARCHAR(20),
    victim_age_masked           NVARCHAR(20),
    area_population_estimate    FLOAT,
    median_income_estimate      FLOAT,
    unemployment_rate           FLOAT,
    housing_density             FLOAT,
    commercial_activity_index   FLOAT,
    crime_risk_level            NVARCHAR(10)
);

PRINT '✔ Step 4: Staging table created.';
GO


-- ============================================================
-- STEP 5 — CREATE DIMENSION TABLES
-- ============================================================

-- 5a. dim_municipality
CREATE TABLE dbo.dim_municipality (
    municipality_id     INT             NOT NULL,
    municipality        NVARCHAR(100)   NOT NULL,
    etl_created_at      DATETIME        NULL
);
PRINT '✔ Step 5a: dim_municipality created.';
GO

-- 5b. dim_location
CREATE TABLE dbo.dim_location (
    location_id         INT             NOT NULL,
    neighborhood        NVARCHAR(100)   NULL,
    postal_code         NVARCHAR(20)    NULL,
    latitude            FLOAT           NULL,
    longitude           FLOAT           NULL,
    municipality_id     INT             NOT NULL,
    etl_created_at      DATETIME        NULL
);
PRINT '✔ Step 5b: dim_location created.';
GO

-- 5c. dim_date
CREATE TABLE dbo.dim_date (
    date_id             INT             NOT NULL,
    incident_date       DATE            NULL,
    year                INT             NULL,
    month               INT             NULL,
    day_of_week         NVARCHAR(20)    NULL,
    hour_of_day         INT             NULL,
    etl_created_at      DATETIME        NULL
);
PRINT '✔ Step 5c: dim_date created.';
GO

-- 5d. dim_crime_type
CREATE TABLE dbo.dim_crime_type (
    crime_type_id       INT             NOT NULL,
    crime_category      NVARCHAR(100)   NOT NULL,
    crime_subtype       NVARCHAR(100)   NULL,
    etl_created_at      DATETIME        NULL
);
PRINT '✔ Step 5d: dim_crime_type created.';
GO

-- 5e. dim_weapon
CREATE TABLE dbo.dim_weapon (
    weapon_id           INT             NOT NULL,
    weapon_used         NVARCHAR(50)    NOT NULL,
    etl_created_at      DATETIME        NULL
);
PRINT '✔ Step 5e: dim_weapon created.';
GO

-- 5f. dim_demographics
CREATE TABLE dbo.dim_demographics (
    demographic_id              INT             NOT NULL,
    offender_age_masked         NVARCHAR(20)    NULL,
    victim_age_masked           NVARCHAR(20)    NULL,
    area_population_estimate    FLOAT           NULL,
    etl_created_at              DATETIME        NULL
);
PRINT '✔ Step 5f: dim_demographics created.';
GO

-- 5g. dim_socioeconomic
CREATE TABLE dbo.dim_socioeconomic (
    socioeconomic_id            INT             NOT NULL,
    median_income_estimate      FLOAT           NULL,
    unemployment_rate           FLOAT           NULL,
    housing_density             FLOAT           NULL,
    commercial_activity_index   FLOAT           NULL,
    etl_created_at              DATETIME        NULL
);
PRINT '✔ Step 5g: dim_socioeconomic created.';
GO

-- 5h. dim_police_response
CREATE TABLE dbo.dim_police_response (
    police_response_id      INT             NOT NULL,
    police_service          NVARCHAR(100)   NULL,
    response_time_minutes   FLOAT           NULL,
    units_dispatched        FLOAT           NULL,
    arrest_made             NVARCHAR(10)    NULL,
    etl_created_at          DATETIME        NULL
);
PRINT '✔ Step 5h: dim_police_response created.';
GO

-- 5i. dim_risk
CREATE TABLE dbo.dim_risk (
    risk_id             INT             NOT NULL,
    crime_risk_level    NVARCHAR(10)    NOT NULL,
    etl_created_at      DATETIME        NULL
);
PRINT '✔ Step 5i: dim_risk created.';
GO


-- ============================================================
-- STEP 6 — CREATE FACT TABLE
-- One row per incident. References all 9 dimension tables.
-- ============================================================

CREATE TABLE dbo.fact_incident (
    incident_id             INT             NOT NULL,
    location_id             INT             NULL,
    date_id                 INT             NULL,
    crime_type_id           INT             NULL,
    weapon_id               INT             NULL,
    demographic_id          INT             NULL,
    socioeconomic_id        INT             NULL,
    police_response_id      INT             NULL,
    risk_id                 INT             NULL,
    violent_flag            NVARCHAR(10)    NULL,
    property_damage_value   FLOAT           NULL,
    reported_date           DATE            NULL,
    etl_created_at          DATETIME        NULL
);

PRINT '✔ Step 6: fact_incident created.';
GO


-- ============================================================
-- STEP 7 — ADD PRIMARY KEYS
-- ============================================================

ALTER TABLE dbo.dim_municipality    ADD CONSTRAINT pk_dim_municipality     PRIMARY KEY (municipality_id);
ALTER TABLE dbo.dim_location        ADD CONSTRAINT pk_dim_location         PRIMARY KEY (location_id);
ALTER TABLE dbo.dim_date            ADD CONSTRAINT pk_dim_date             PRIMARY KEY (date_id);
ALTER TABLE dbo.dim_crime_type      ADD CONSTRAINT pk_dim_crime_type       PRIMARY KEY (crime_type_id);
ALTER TABLE dbo.dim_weapon          ADD CONSTRAINT pk_dim_weapon           PRIMARY KEY (weapon_id);
ALTER TABLE dbo.dim_demographics    ADD CONSTRAINT pk_dim_demographics     PRIMARY KEY (demographic_id);
ALTER TABLE dbo.dim_socioeconomic   ADD CONSTRAINT pk_dim_socioeconomic    PRIMARY KEY (socioeconomic_id);
ALTER TABLE dbo.dim_police_response ADD CONSTRAINT pk_dim_police_response  PRIMARY KEY (police_response_id);
ALTER TABLE dbo.dim_risk            ADD CONSTRAINT pk_dim_risk             PRIMARY KEY (risk_id);
ALTER TABLE dbo.fact_incident       ADD CONSTRAINT pk_fact_incident        PRIMARY KEY (incident_id);

PRINT '✔ Step 7: All primary keys added.';
GO


-- ============================================================
-- STEP 8 — ADD FOREIGN KEYS
-- ============================================================

-- dim_location → dim_municipality
ALTER TABLE dbo.dim_location
    ADD CONSTRAINT fk_location_municipality
    FOREIGN KEY (municipality_id) REFERENCES dbo.dim_municipality(municipality_id);

-- fact_incident → all dimensions
ALTER TABLE dbo.fact_incident ADD CONSTRAINT fk_fact_location
    FOREIGN KEY (location_id)       REFERENCES dbo.dim_location(location_id);

ALTER TABLE dbo.fact_incident ADD CONSTRAINT fk_fact_date
    FOREIGN KEY (date_id)           REFERENCES dbo.dim_date(date_id);

ALTER TABLE dbo.fact_incident ADD CONSTRAINT fk_fact_crime_type
    FOREIGN KEY (crime_type_id)     REFERENCES dbo.dim_crime_type(crime_type_id);

ALTER TABLE dbo.fact_incident ADD CONSTRAINT fk_fact_weapon
    FOREIGN KEY (weapon_id)         REFERENCES dbo.dim_weapon(weapon_id);

ALTER TABLE dbo.fact_incident ADD CONSTRAINT fk_fact_demographics
    FOREIGN KEY (demographic_id)    REFERENCES dbo.dim_demographics(demographic_id);

ALTER TABLE dbo.fact_incident ADD CONSTRAINT fk_fact_socioeconomic
    FOREIGN KEY (socioeconomic_id)  REFERENCES dbo.dim_socioeconomic(socioeconomic_id);

ALTER TABLE dbo.fact_incident ADD CONSTRAINT fk_fact_police_response
    FOREIGN KEY (police_response_id) REFERENCES dbo.dim_police_response(police_response_id);

ALTER TABLE dbo.fact_incident ADD CONSTRAINT fk_fact_risk
    FOREIGN KEY (risk_id)           REFERENCES dbo.dim_risk(risk_id);

PRINT '✔ Step 8: All foreign keys added.';
GO


-- ============================================================
-- STEP 9 — ADD CHECK CONSTRAINTS
-- Business rules enforced at the database level.
-- ============================================================

-- dim_date
ALTER TABLE dbo.dim_date ADD CONSTRAINT chk_date_month
    CHECK (month BETWEEN 1 AND 12);

ALTER TABLE dbo.dim_date ADD CONSTRAINT chk_date_hour
    CHECK (hour_of_day BETWEEN 0 AND 23);

ALTER TABLE dbo.dim_date ADD CONSTRAINT chk_date_year
    CHECK (year BETWEEN 2000 AND 2100);

ALTER TABLE dbo.dim_date ADD CONSTRAINT chk_date_day_of_week
    CHECK (day_of_week IN ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday','Unknown'));

-- dim_demographics
ALTER TABLE dbo.dim_demographics ADD CONSTRAINT chk_demo_offender_age_masked
    CHECK (offender_age_masked IN ('Under 18','18-25','26-35','36-50','51-65','65+','Unknown'));

ALTER TABLE dbo.dim_demographics ADD CONSTRAINT chk_demo_victim_age_masked
    CHECK (victim_age_masked IN ('Under 18','18-25','26-35','36-50','51-65','65+','Unknown'));

ALTER TABLE dbo.dim_demographics ADD CONSTRAINT chk_demo_population
    CHECK (area_population_estimate >= 0);

-- dim_socioeconomic
ALTER TABLE dbo.dim_socioeconomic ADD CONSTRAINT chk_socio_unemployment
    CHECK (unemployment_rate BETWEEN 0 AND 100);

ALTER TABLE dbo.dim_socioeconomic ADD CONSTRAINT chk_socio_housing_density
    CHECK (housing_density >= 0);

ALTER TABLE dbo.dim_socioeconomic ADD CONSTRAINT chk_socio_income
    CHECK (median_income_estimate >= 0);

-- dim_police_response
ALTER TABLE dbo.dim_police_response ADD CONSTRAINT chk_police_response_time
    CHECK (response_time_minutes >= 0);

ALTER TABLE dbo.dim_police_response ADD CONSTRAINT chk_police_units
    CHECK (units_dispatched >= 0);

ALTER TABLE dbo.dim_police_response ADD CONSTRAINT chk_police_arrest_made
    CHECK (arrest_made IN ('Yes','No','Unknown'));

-- dim_risk
ALTER TABLE dbo.dim_risk ADD CONSTRAINT chk_risk_level
    CHECK (crime_risk_level IN ('Low','Medium','High'));

-- dim_weapon
ALTER TABLE dbo.dim_weapon ADD CONSTRAINT chk_weapon_used
    CHECK (weapon_used IN ('Firearm','Knife','Blunt Object','Other','Unknown','None'));

-- fact_incident
ALTER TABLE dbo.fact_incident ADD CONSTRAINT chk_fact_property_damage
    CHECK (property_damage_value >= 0);

ALTER TABLE dbo.fact_incident ADD CONSTRAINT chk_fact_violent_flag
    CHECK (violent_flag IN ('Yes','No','Unknown'));

-- dim_location
ALTER TABLE dbo.dim_location ADD CONSTRAINT chk_location_latitude
    CHECK (latitude BETWEEN 49 AND 60 OR latitude IS NULL);

ALTER TABLE dbo.dim_location ADD CONSTRAINT chk_location_longitude
    CHECK (longitude BETWEEN -120 AND -110 OR longitude IS NULL);

-- dim_crime_type
ALTER TABLE dbo.dim_crime_type ADD CONSTRAINT chk_crime_category
    CHECK (crime_category IN (
        'Assault','Sexual Offence','Robbery','Arson',
        'Break and Enter','Drug Offence','Theft',
        'Fraud','Mischief','Vandalism'
    ));

PRINT '✔ Step 9: All CHECK constraints added (20 total).';
GO


-- ============================================================
-- STEP 10 — ADD INDEXES
-- ============================================================

CREATE NONCLUSTERED INDEX ix_fact_location_id   ON dbo.fact_incident (location_id);
CREATE NONCLUSTERED INDEX ix_fact_risk_id        ON dbo.fact_incident (risk_id);
CREATE NONCLUSTERED INDEX ix_fact_crime_type_id  ON dbo.fact_incident (crime_type_id);
CREATE NONCLUSTERED INDEX ix_location_neighborhood ON dbo.dim_location (neighborhood);
CREATE NONCLUSTERED INDEX ix_date_year_month     ON dbo.dim_date (year, month);

PRINT '✔ Step 10: All indexes created.';
GO


-- ============================================================
-- STEP 11 — CREATE VIEWS
-- Reference: Lab 2 Step 7 — Gold View for ML model
-- All 11 use cases served from vw_ml_features.
-- ============================================================

-- 11a. vw_ml_features — flat join for ML model training
CREATE VIEW dbo.vw_ml_features AS
SELECT
    f.incident_id,
    m.municipality,
    l.neighborhood,
    l.postal_code,
    l.latitude,
    l.longitude,
    d.incident_date,
    d.year,
    d.month,
    d.day_of_week,
    d.hour_of_day,
    ct.crime_category,
    ct.crime_subtype,
    w.weapon_used,
    f.violent_flag,
    f.property_damage_value,
    dm.offender_age_masked,
    dm.victim_age_masked,
    dm.area_population_estimate,
    se.median_income_estimate,
    se.unemployment_rate,
    se.housing_density,
    se.commercial_activity_index,
    pr.police_service,
    pr.response_time_minutes,
    pr.units_dispatched,
    pr.arrest_made,
    r.crime_risk_level
FROM       dbo.fact_incident       f
LEFT JOIN  dbo.dim_location        l   ON f.location_id        = l.location_id
LEFT JOIN  dbo.dim_municipality    m   ON l.municipality_id    = m.municipality_id
LEFT JOIN  dbo.dim_date            d   ON f.date_id            = d.date_id
LEFT JOIN  dbo.dim_crime_type      ct  ON f.crime_type_id      = ct.crime_type_id
LEFT JOIN  dbo.dim_weapon          w   ON f.weapon_id          = w.weapon_id
LEFT JOIN  dbo.dim_demographics    dm  ON f.demographic_id     = dm.demographic_id
LEFT JOIN  dbo.dim_socioeconomic   se  ON f.socioeconomic_id   = se.socioeconomic_id
LEFT JOIN  dbo.dim_police_response pr  ON f.police_response_id = pr.police_response_id
LEFT JOIN  dbo.dim_risk            r   ON f.risk_id            = r.risk_id;
GO
PRINT '✔ Step 11a: vw_ml_features created.';
GO

-- 11b. vw_high_risk_incidents — High risk only
CREATE VIEW dbo.vw_high_risk_incidents AS
SELECT
    f.incident_id,
    m.municipality,
    l.neighborhood,
    d.incident_date,
    d.hour_of_day,
    ct.crime_category,
    ct.crime_subtype,
    w.weapon_used,
    f.violent_flag,
    f.property_damage_value,
    pr.response_time_minutes,
    pr.units_dispatched,
    r.crime_risk_level
FROM       dbo.fact_incident       f
LEFT JOIN  dbo.dim_location        l   ON f.location_id        = l.location_id
LEFT JOIN  dbo.dim_municipality    m   ON l.municipality_id    = m.municipality_id
LEFT JOIN  dbo.dim_date            d   ON f.date_id            = d.date_id
LEFT JOIN  dbo.dim_crime_type      ct  ON f.crime_type_id      = ct.crime_type_id
LEFT JOIN  dbo.dim_weapon          w   ON f.weapon_id          = w.weapon_id
LEFT JOIN  dbo.dim_police_response pr  ON f.police_response_id = pr.police_response_id
LEFT JOIN  dbo.dim_risk            r   ON f.risk_id            = r.risk_id
WHERE r.crime_risk_level = 'High';
GO
PRINT '✔ Step 11b: vw_high_risk_incidents created.';
GO

-- 11c. vw_crime_by_neighborhood — aggregated by neighborhood
CREATE VIEW dbo.vw_crime_by_neighborhood AS
SELECT
    m.municipality,
    l.neighborhood,
    r.crime_risk_level,
    COUNT(f.incident_id)          AS total_incidents,
    AVG(f.property_damage_value)  AS avg_property_damage,
    AVG(pr.response_time_minutes) AS avg_response_time
FROM       dbo.fact_incident       f
LEFT JOIN  dbo.dim_location        l   ON f.location_id        = l.location_id
LEFT JOIN  dbo.dim_municipality    m   ON l.municipality_id    = m.municipality_id
LEFT JOIN  dbo.dim_police_response pr  ON f.police_response_id = pr.police_response_id
LEFT JOIN  dbo.dim_risk            r   ON f.risk_id            = r.risk_id
GROUP BY m.municipality, l.neighborhood, r.crime_risk_level;
GO
PRINT '✔ Step 11c: vw_crime_by_neighborhood created.';
GO

-- 11d. vw_police_response_summary — response stats by municipality
CREATE VIEW dbo.vw_police_response_summary AS
SELECT
    m.municipality,
    r.crime_risk_level,
    pr.police_service,
    COUNT(f.incident_id)                                        AS total_incidents,
    AVG(pr.response_time_minutes)                               AS avg_response_time,
    AVG(pr.units_dispatched)                                    AS avg_units_dispatched,
    SUM(CASE WHEN pr.arrest_made = 'Yes' THEN 1 ELSE 0 END)    AS total_arrests
FROM       dbo.fact_incident       f
LEFT JOIN  dbo.dim_location        l   ON f.location_id        = l.location_id
LEFT JOIN  dbo.dim_municipality    m   ON l.municipality_id    = m.municipality_id
LEFT JOIN  dbo.dim_police_response pr  ON f.police_response_id = pr.police_response_id
LEFT JOIN  dbo.dim_risk            r   ON f.risk_id            = r.risk_id
GROUP BY m.municipality, r.crime_risk_level, pr.police_service;
GO
PRINT '✔ Step 11d: vw_police_response_summary created.';
GO


-- ============================================================
-- STEP 12 — VERIFY
-- Run this again after ETL_Engine.py to confirm row counts.
-- A Data Engineer never trusts a Success message.
-- ============================================================

SELECT 'stg_crime_incidents'  AS table_name, COUNT(*) AS row_count FROM dbo.stg_crime_incidents  UNION ALL
SELECT 'dim_municipality',                    COUNT(*)             FROM dbo.dim_municipality      UNION ALL
SELECT 'dim_location',                        COUNT(*)             FROM dbo.dim_location          UNION ALL
SELECT 'dim_date',                            COUNT(*)             FROM dbo.dim_date              UNION ALL
SELECT 'dim_crime_type',                      COUNT(*)             FROM dbo.dim_crime_type        UNION ALL
SELECT 'dim_weapon',                          COUNT(*)             FROM dbo.dim_weapon            UNION ALL
SELECT 'dim_demographics',                    COUNT(*)             FROM dbo.dim_demographics      UNION ALL
SELECT 'dim_socioeconomic',                   COUNT(*)             FROM dbo.dim_socioeconomic     UNION ALL
SELECT 'dim_police_response',                 COUNT(*)             FROM dbo.dim_police_response   UNION ALL
SELECT 'dim_risk',                            COUNT(*)             FROM dbo.dim_risk              UNION ALL
SELECT 'fact_incident',                       COUNT(*)             FROM dbo.fact_incident
ORDER BY table_name;
GO

PRINT '============================================================';
PRINT '✔ Fortress_Build.sql complete.';
PRINT '   Next step: Run ETL_Engine.py to load data.';
PRINT '============================================================';

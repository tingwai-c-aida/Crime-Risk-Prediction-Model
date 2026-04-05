# ============================================================
# AIDA 1143 & 1145 | Group 4
# Alberta Crime Risk Prediction System
# ETL_Engine.py — Phase 2 | The Data Fortress
# ============================================================

import pandas as pd
import sqlalchemy as sa
import re
import logging
import numpy as np
from datetime import datetime
from pathlib import Path


# ============================================================
# --- CONFIGURATION ---
# ============================================================

SERVER_NAME   = r'LAPTOP-GDO01OH4'
DATABASE_NAME = 'FinalProject'
EXCEL_PATH    = r'C:\Users\nidhi\Documents\alberta_crime_dataset_10k_raw.xlsx'


# ============================================================
# --- REQ-07: LOGGING SETUP ---
# Writes to ETL_Engine.log in the same folder as this script.
# Every print() is mirrored to the log file automatically.
# Reference: Lab 2 — "A Data Engineer documents every run."
# ============================================================

LOG_PATH = Path(__file__).parent / "ETL_Engine.log"

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt  = "%Y-%m-%d %H:%M:%S",
    handlers = [
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("ETL_Engine")


# ============================================================
# --- HELPER: IDEMPOTENT UPSERT WITH TRANSACTION (REQ-11) ---
# Each table load is wrapped in its own BEGIN / COMMIT.
# On failure, only that table rolls back — prior tables are safe.
# Reference: Lab 2 Idempotency / Midnight Lab Sync pattern
# ============================================================

def upsert_dimension(engine, table_name, pk_col, df_new):
    with engine.connect() as conn:
        existing = pd.read_sql(f"SELECT {pk_col} FROM dbo.{table_name}", conn)
    existing_ids = set(existing[pk_col].tolist())
    df_insert    = df_new[~df_new[pk_col].isin(existing_ids)].copy()

    if len(df_insert) == 0:
        log.info(f"  SKIP   {table_name:<35} — no new rows")
        return 0

    # REQ-14: Audit timestamp on every new row
    df_insert["etl_created_at"] = datetime.now()

    try:
        with engine.begin() as conn:
            df_insert.to_sql(
                table_name, con=conn, schema="dbo",
                if_exists="append", index=False
                # REQ-16: No chunksize — fast_executemany on the engine handles
                # bulk batching at the ODBC driver level. chunksize overrides it.
            )
        log.info(f"  COMMIT {table_name:<35} — {len(df_insert)} new rows inserted")
        return len(df_insert)

    except Exception as exc:
        log.error(f"  ROLLBACK {table_name} — {exc}")
        raise


# ============================================================
# --- MAIN PIPELINE ---
# ============================================================

def run_warehouse_pipeline():

    run_start = datetime.now()
    log.info("=" * 60)
    log.info("ETL_Engine.py — Run started")
    log.info("=" * 60)

    try:

        # --------------------------------------------------
        # STEP 1: CONNECT TO SQL SERVER
        # --------------------------------------------------
        connection_url = (
            f"mssql+pyodbc://@{SERVER_NAME}/{DATABASE_NAME}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
        # REQ-16: fast_executemany=True on the engine enables bulk
        # insert optimization for all to_sql calls automatically.
        engine = sa.create_engine(connection_url, fast_executemany=True)

        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        log.info(f"Connected to [{DATABASE_NAME}] on [{SERVER_NAME}]")


        # --------------------------------------------------
        # STEP 2: EXTRACT
        # --------------------------------------------------
        log.info("Loading Excel Workbook...")
        df = pd.read_excel(EXCEL_PATH, engine="openpyxl")
        log.info(f"Extracted {len(df)} rows, {len(df.columns)} columns from Excel")


        # --------------------------------------------------
        # STEP 3: TRANSFORM — The Cleaning Room
        # --------------------------------------------------
        log.info("--- TRANSFORM ---")

        # 3a. Remove exact duplicate rows
        before = len(df)
        df = df.drop_duplicates()
        log.info(f"Duplicates removed: {before - len(df)} | Remaining: {len(df)}")

        # 3b. Standardize column names
        df.columns = [
            re.sub(r'[^a-zA-Z0-9]', '_', str(c)).strip('_').lower()
            for c in df.columns
        ]

        # 3c. Standardize date columns
        DATE_FORMATS = [
            "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d",
            "%d-%m-%Y", "%d-%b-%Y", "%b-%d-%Y"
        ]

        def parse_date(val):
            if pd.isna(val) or val is None:
                return pd.NaT
            for fmt in DATE_FORMATS:
                try:
                    return datetime.strptime(str(val).strip(), fmt).date()
                except ValueError:
                    continue
            return pd.NaT

        df["incident_date"] = df["incident_date"].apply(parse_date)
        df["reported_date"] = df["reported_date"].apply(parse_date)
        log.info(f"Dates standardized | Unparseable incident_dates: {df['incident_date'].isna().sum()}")

        # FIX: Re-derive year, month from the cleaned incident_date so rows
        # with unparseable raw dates get consistent NaN instead of wrong values
        # from the original Excel. Raw Excel year/month are overwritten here.
        df["year"]  = pd.to_datetime(df["incident_date"], errors="coerce").dt.year
        df["month"] = pd.to_datetime(df["incident_date"], errors="coerce").dt.month
        log.info("year and month re-derived from cleaned incident_date — raw Excel values overwritten")

        # 3d. Standardize day_of_week
        DAY_MAP = {
            "mon": "Monday",    "monday": "Monday",
            "tue": "Tuesday",   "tuesday": "Tuesday",
            "wed": "Wednesday", "wednesday": "Wednesday",
            "thu": "Thursday",  "thur": "Thursday", "thursday": "Thursday",
            "fri": "Friday",    "friday": "Friday",
            "sat": "Saturday",  "saturday": "Saturday",
            "sun": "Sunday",    "sunday": "Sunday"
        }
        df["day_of_week"] = (
            df["day_of_week"].astype(str).str.strip().str.lower()
            .map(DAY_MAP).fillna("Unknown")
        )

        # 3e. Standardize neighborhood to Title Case
        df["neighborhood"] = df["neighborhood"].astype(str).str.strip().str.title()

        # 3f. Standardize boolean flags
        TRUE_VALS  = {"y", "yes", "1", "true"}
        FALSE_VALS = {"n", "no", "0", "false"}

        def standardize_flag(val):
            if pd.isna(val): return "Unknown"
            s = str(val).strip().lower()
            if s in TRUE_VALS:  return "Yes"
            if s in FALSE_VALS: return "No"
            return "Unknown"

        df["violent_flag"] = df["violent_flag"].apply(standardize_flag)
        df["arrest_made"]  = df["arrest_made"].apply(standardize_flag)
        log.info("violent_flag and arrest_made standardized to Yes / No / Unknown")

        # 3g. Normalize weapon_used
        WEAPON_MAP = {
            "firearm": "Firearm",    "gun": "Firearm",
            "handgun": "Firearm",    "hand gun": "Firearm",
            "knife": "Knife",
            "blunt": "Blunt Object", "blunt object": "Blunt Object",
            "none": "None",          "no weapon": "None", "n/a": "None",
            "unknown": "Unknown",    "unknown/other": "Unknown",
            "other": "Other"
        }
        df["weapon_used"] = (
            df["weapon_used"].fillna("Unknown")
            .astype(str).str.strip().str.lower()
            .map(WEAPON_MAP).fillna("Unknown")
        )
        log.info("weapon_used normalized to 7 standard categories")

        # 3h. Cap outliers
        df["property_damage_value"] = pd.to_numeric(df["property_damage_value"], errors="coerce")
        df["response_time_minutes"] = pd.to_numeric(df["response_time_minutes"], errors="coerce")
        df["hour_of_day"]           = pd.to_numeric(df["hour_of_day"],           errors="coerce")
        df["units_dispatched"]      = pd.to_numeric(df["units_dispatched"],      errors="coerce")

        df["property_damage_value"] = df["property_damage_value"].clip(
            lower=0, upper=df["property_damage_value"].quantile(0.99)
        )
        df["response_time_minutes"] = df["response_time_minutes"].clip(
            lower=0, upper=df["response_time_minutes"].quantile(0.99)
        )
        df.loc[~df["hour_of_day"].between(0, 23), "hour_of_day"] = np.nan
        df["units_dispatched"] = df["units_dispatched"].clip(lower=0)
        log.info("Outliers capped — property_damage, response_time, hour_of_day, units_dispatched")

        # 3i. Null imputation
        df["police_service"]            = df["police_service"].fillna("Unknown")
        df["crime_subtype"]             = df["crime_subtype"].fillna("Unspecified")
        df["area_population_estimate"]  = df["area_population_estimate"].fillna(df["area_population_estimate"].median())
        df["median_income_estimate"]    = df["median_income_estimate"].fillna(df["median_income_estimate"].median())
        df["unemployment_rate"]         = df["unemployment_rate"].fillna(df["unemployment_rate"].median())
        df["housing_density"]           = df["housing_density"].fillna(df["housing_density"].median())
        df["commercial_activity_index"] = df["commercial_activity_index"].fillna(df["commercial_activity_index"].median())
        df["response_time_minutes"]     = df["response_time_minutes"].fillna(df["response_time_minutes"].median())
        df["units_dispatched"]          = df["units_dispatched"].fillna(df["units_dispatched"].median())
        df["property_damage_value"]     = df["property_damage_value"].fillna(0)
        log.info("Nulls imputed — median for numerics, Unknown for categoricals")

        # 3j. Validate Alberta coordinate bounds
        df.loc[~df["latitude"].between(49, 60),      "latitude"]  = np.nan
        df.loc[~df["longitude"].between(-120, -110), "longitude"] = np.nan
        log.info("Invalid Alberta coordinates set to NULL")

        # 3k. PII Age Masking
        def age_bin(age):
            try:
                a = int(age)
                if a < 18:  return "Under 18"
                if a <= 25: return "18-25"
                if a <= 35: return "26-35"
                if a <= 50: return "36-50"
                if a <= 65: return "51-65"
                return "65+"
            except (ValueError, TypeError):
                return "Unknown"

        df["offender_age_masked"] = df["offender_age"].apply(age_bin)
        df["victim_age_masked"]   = df["victim_age"].apply(age_bin)
        log.info("PII age masking applied — offender_age and victim_age binned")


        # --------------------------------------------------
        # STEP 4: FEATURE ENHANCEMENT — Derive crime_risk_level
        # --------------------------------------------------
        log.info("--- FEATURE ENHANCEMENT ---")

        df["_s_violent"] = df["violent_flag"].map({"Yes": 3, "No": 0, "Unknown": 1})

        HIGH_CAT   = {"Assault", "Sexual Offence", "Robbery", "Arson"}
        MEDIUM_CAT = {"Break and Enter", "Drug Offence", "Theft"}
        HIGH_SUB   = {
            "Aggravated Assault", "Sexual Assault", "Domestic Assault",
            "Carjacking", "Street Robbery", "Exploitation",
            "Structure Fire", "Wildland Fire", "Vehicle Fire"
        }
        MEDIUM_SUB = {
            "Residential B&E", "Commercial B&E", "Trafficking", "Distribution",
            "Theft Over $5000", "Vehicle B&E", "Commercial Robbery"
        }

        def crime_score(row):
            if row["crime_subtype"] in HIGH_SUB or row["crime_category"] in HIGH_CAT:
                return 4
            if row["crime_subtype"] in MEDIUM_SUB or row["crime_category"] in MEDIUM_CAT:
                return 2
            return 0

        df["_s_crime"]   = df.apply(crime_score, axis=1)
        df["_s_weapon"]  = df["weapon_used"].map(
            {"Firearm": 3, "Knife": 2, "Blunt Object": 2, "Other": 1, "Unknown": 0, "None": 0}
        ).fillna(0)
        df["_s_damage"]   = (df["property_damage_value"] > df["property_damage_value"].median()).astype(int)
        df["_s_response"] = (df["response_time_minutes"] > df["response_time_minutes"].median()).astype(int)
        df["_composite"]  = df[["_s_violent","_s_crime","_s_weapon","_s_damage","_s_response"]].sum(axis=1)

        def assign_risk(score):
            if score >= 9: return "High"
            if score >= 5: return "Medium"
            return "Low"

        df["crime_risk_level"] = df["_composite"].apply(assign_risk)
        df.drop(columns=[c for c in df.columns if c.startswith("_s") or c == "_composite"], inplace=True)
        risk_dist = df["crime_risk_level"].value_counts().to_dict()
        log.info(f"crime_risk_level derived | Distribution: {risk_dist}")


        # --------------------------------------------------
        # STEP 5: LOAD — Staging Table
        #
        # BUGFIX: STAGING_COLS previously included offender_age
        # and victim_age (raw integers). The staging schema expects
        # offender_age_masked and victim_age_masked (NVARCHAR text
        # bins) only. Raw integer columns removed from this list.
        #
        # REQ-16: chunksize removed. fast_executemany=True on the
        # engine handles bulk batching internally — chunksize
        # overrides and defeats the optimization.
        # --------------------------------------------------
        log.info("--- LOAD: Staging Table ---")

        STAGING_COLS = [
            "incident_id", "municipality", "postal_code", "latitude", "longitude",
            "neighborhood", "incident_date", "reported_date", "year", "month",
            "day_of_week", "hour_of_day", "crime_category", "crime_subtype",
            "weapon_used", "violent_flag", "property_damage_value",
            "police_service", "response_time_minutes", "units_dispatched",
            "arrest_made",
            "offender_age_masked", "victim_age_masked",
            "area_population_estimate", "median_income_estimate",
            "unemployment_rate", "housing_density", "commercial_activity_index",
            "crime_risk_level"
        ]

        # Pre-flight: validate all expected columns are present
        missing = [c for c in STAGING_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"Staging column mismatch — missing from DataFrame: {missing}")
        
    # REQ-19 — Automated backup before truncate
        with engine.begin() as conn:
            conn.execute(sa.text("""
                IF OBJECT_ID('dbo.stg_crime_incidents_backup', 'U') IS NOT NULL
                    DROP TABLE dbo.stg_crime_incidents_backup;
                SELECT * INTO dbo.stg_crime_incidents_backup
                FROM dbo.stg_crime_incidents;
            """))
        log.info("BACKUP stg_crime_incidents -> stg_crime_incidents_backup complete")

        try:
            with engine.begin() as conn:
                conn.execute(sa.text("TRUNCATE TABLE dbo.stg_crime_incidents"))
                df[STAGING_COLS].to_sql(
                    "stg_crime_incidents", con=conn, schema="dbo",
                    if_exists="append", index=False
            # NO chunksize — fast_executemany handles batching
                )
            log.info(f"COMMIT stg_crime_incidents — {len(df)} rows loaded")
        except Exception as exc:
            log.error(f"ROLLBACK stg_crime_incidents — {exc}")
            raise
        



        # --------------------------------------------------
        # STEP 6: LOAD — Dimension Tables (per-table transactions)
        # --------------------------------------------------
        log.info("--- LOAD: Dimension Tables ---")

        # FIX: municipality_id now uses a stable seed map instead of row index.
        # This ensures IDs are deterministic across re-runs regardless of sort
        # order in the source Excel. Unknown municipalities fall back to hash.
        KNOWN_MUNICIPALITIES = {
            "Calgary": 1, "Edmonton": 2, "Red Deer": 3, "Lethbridge": 4,
            "St. Albert": 5, "Medicine Hat": 6, "Grande Prairie": 7,
            "Airdrie": 8, "Spruce Grove": 9, "Fort McMurray": 10
        }
        df_muni = df[["municipality"]].drop_duplicates().reset_index(drop=True)
        df_muni.insert(0, "municipality_id",
            df_muni["municipality"].map(KNOWN_MUNICIPALITIES)
            .fillna(df_muni["municipality"].apply(
                lambda x: abs(hash(x)) % 9000 + 1000
            )).astype(int)
        )
        upsert_dimension(engine, "dim_municipality", "municipality_id", df_muni)

        df_loc = (
            df[["neighborhood","postal_code","latitude","longitude","municipality"]]
            .drop_duplicates(subset=["neighborhood","postal_code"])
            .reset_index(drop=True)
            .merge(df_muni, on="municipality", how="left")
        )
        df_loc.drop(columns=["municipality"], inplace=True)
        df_loc.insert(0, "location_id", range(1, len(df_loc)+1))
        upsert_dimension(engine, "dim_location", "location_id", df_loc)

        df_date = (
            df[["incident_date","year","month","day_of_week","hour_of_day"]]
            .drop_duplicates().dropna(subset=["incident_date"]).reset_index(drop=True)
        )
        df_date.insert(0, "date_id", range(1, len(df_date)+1))
        upsert_dimension(engine, "dim_date", "date_id", df_date)

        df_crime = df[["crime_category","crime_subtype"]].drop_duplicates().reset_index(drop=True)
        df_crime.insert(0, "crime_type_id", range(1, len(df_crime)+1))
        upsert_dimension(engine, "dim_crime_type", "crime_type_id", df_crime)

        df_weapon = df[["weapon_used"]].drop_duplicates().reset_index(drop=True)
        df_weapon.insert(0, "weapon_id", range(1, len(df_weapon)+1))
        upsert_dimension(engine, "dim_weapon", "weapon_id", df_weapon)

        df_demo = (
            df[["offender_age_masked","victim_age_masked","area_population_estimate"]]
            .drop_duplicates().reset_index(drop=True)
        )
        df_demo.insert(0, "demographic_id", range(1, len(df_demo)+1))
        upsert_dimension(engine, "dim_demographics", "demographic_id", df_demo)

        df_socio = (
            df[["median_income_estimate","unemployment_rate","housing_density","commercial_activity_index"]]
            .drop_duplicates().reset_index(drop=True)
        )
        df_socio.insert(0, "socioeconomic_id", range(1, len(df_socio)+1))
        upsert_dimension(engine, "dim_socioeconomic", "socioeconomic_id", df_socio)

        df_police = (
            df[["police_service","response_time_minutes","units_dispatched","arrest_made"]]
            .drop_duplicates().reset_index(drop=True)
        )
        df_police.insert(0, "police_response_id", range(1, len(df_police)+1))
        upsert_dimension(engine, "dim_police_response", "police_response_id", df_police)

        df_risk = pd.DataFrame({
            "risk_id":          [1, 2, 3],
            "crime_risk_level": ["Low", "Medium", "High"]
        })
        upsert_dimension(engine, "dim_risk", "risk_id", df_risk)


        # --------------------------------------------------
        # STEP 7: LOAD — Fact Table
        # --------------------------------------------------
        log.info("--- LOAD: Fact Table ---")

        with engine.connect() as conn:
            muni_map   = pd.read_sql("SELECT municipality_id, municipality FROM dbo.dim_municipality", conn)
            loc_map    = pd.read_sql("SELECT location_id, neighborhood, postal_code FROM dbo.dim_location", conn)
            date_map   = pd.read_sql("SELECT date_id, incident_date FROM dbo.dim_date", conn)
            crime_map  = pd.read_sql("SELECT crime_type_id, crime_category, crime_subtype FROM dbo.dim_crime_type", conn)
            weapon_map = pd.read_sql("SELECT weapon_id, weapon_used FROM dbo.dim_weapon", conn)
            demo_map   = pd.read_sql(
                "SELECT demographic_id, offender_age_masked, victim_age_masked, area_population_estimate "
                "FROM dbo.dim_demographics", conn
            )
            socio_map  = pd.read_sql(
                "SELECT socioeconomic_id, median_income_estimate, unemployment_rate, "
                "housing_density, commercial_activity_index FROM dbo.dim_socioeconomic", conn
            )
            police_map = pd.read_sql(
                "SELECT police_response_id, police_service, response_time_minutes, "
                "units_dispatched, arrest_made FROM dbo.dim_police_response", conn
            )
            risk_map   = pd.read_sql("SELECT risk_id, crime_risk_level FROM dbo.dim_risk", conn)

        df_fact = df.copy()
        df_fact["incident_date_str"] = df_fact["incident_date"].astype(str)
        date_map["incident_date"]    = date_map["incident_date"].astype(str)

        df_fact = df_fact.merge(muni_map,   on="municipality",                               how="left")
        df_fact = df_fact.merge(loc_map,    on=["neighborhood","postal_code"],                how="left")
        df_fact = df_fact.merge(date_map,   left_on="incident_date_str",
                                            right_on="incident_date",
                                            how="left", suffixes=("","_d"))
        df_fact = df_fact.merge(crime_map,  on=["crime_category","crime_subtype"],            how="left")
        df_fact = df_fact.merge(weapon_map, on="weapon_used",                                 how="left")
        df_fact = df_fact.merge(demo_map,   on=["offender_age_masked","victim_age_masked",
                                                  "area_population_estimate"],                how="left")
        df_fact = df_fact.merge(socio_map,  on=["median_income_estimate","unemployment_rate",
                                                  "housing_density","commercial_activity_index"], how="left")
        df_fact = df_fact.merge(police_map, on=["police_service","response_time_minutes",
                                                  "units_dispatched","arrest_made"],          how="left")
        df_fact = df_fact.merge(risk_map,   on="crime_risk_level",                            how="left")

        df_fact_final = df_fact[[
            "incident_id","location_id","date_id","crime_type_id",
            "weapon_id","demographic_id","socioeconomic_id",
            "police_response_id","risk_id",
            "violent_flag","property_damage_value","reported_date"
        ]].copy()

        before_dedup  = len(df_fact_final)
        df_fact_final = df_fact_final.drop_duplicates(subset=["incident_id"], keep="first")
        log.info(
            f"Fact deduplication: {before_dedup - len(df_fact_final)} removed "
            f"| {len(df_fact_final)} unique incidents"
        )

        with engine.connect() as conn:
            existing_ids = set(
                pd.read_sql("SELECT incident_id FROM dbo.fact_incident", conn)["incident_id"].tolist()
            )

        df_new_facts = df_fact_final[~df_fact_final["incident_id"].isin(existing_ids)].copy()

        if len(df_new_facts) > 0:
            df_new_facts["etl_created_at"] = datetime.now()   # REQ-14
            try:
                with engine.begin() as conn:
                    df_new_facts.to_sql(
                        "fact_incident", con=conn, schema="dbo",
                        if_exists="append", index=False
                    )
                log.info(f"COMMIT fact_incident — {len(df_new_facts)} new rows inserted")
            except Exception as exc:
                log.error(f"ROLLBACK fact_incident — {exc}")
                raise
        else:
            log.info("fact_incident — no new rows (idempotent re-run)")


        # --------------------------------------------------
        # STEP 8: DATA QUALITY AUDIT
        # --------------------------------------------------
        log.info("=" * 60)
        log.info("DATA QUALITY AUDIT")
        log.info("=" * 60)

        TABLES = [
            "stg_crime_incidents", "dim_municipality", "dim_location",
            "dim_date", "dim_crime_type", "dim_weapon",
            "dim_demographics", "dim_socioeconomic", "dim_police_response",
            "dim_risk", "fact_incident"
        ]

        with engine.connect() as conn:
            for table in TABLES:
                count = conn.execute(sa.text(f"SELECT COUNT(*) FROM dbo.{table}")).scalar()
                log.info(f"  COUNT dbo.{table:<35} = {count:>6} rows")

        elapsed = (datetime.now() - run_start).total_seconds()
        log.info("=" *90)
        log.info(f"WAREHOUSE COMPLETE — elapsed {elapsed:.1f}s | log: {LOG_PATH}")
        log.info("=" * 90)

    except Exception as e:
        log.error(f"PIPELINE FAILED: {str(e)}")
        raise


if __name__ == "__main__":
    run_warehouse_pipeline()

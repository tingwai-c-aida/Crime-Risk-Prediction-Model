# ============================================================
# AIDA 1143 & 1145 | Group 4
# Alberta Crime Risk Prediction System
# ETL_Engine.py — Phase 2 | The Data Fortress
# ============================================================

import pandas as pd
import sqlalchemy as sa
import logging
import numpy as np
import re
from datetime import datetime
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# ============================================================
#  CONFIGURATION 配置 
# ============================================================

SERVER_NAME   = r'SELENA-PC\MSSQLSERVER03'  # SQL Server 实例
DATABASE_NAME = 'FinalProject'               # 数据库名称
EXCEL_PATH = r"C:\Users\selen\OneDrive\Desktop\Evelyn\winter 2026\AIDA Final\alberta_crime_dataset_10k_raw.xlsx"
# ============================================================
#  LOGGING 设置日志 
# ============================================================
LOG_PATH = Path(__file__).parent / "ETL_Engine.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("ETL_Engine")

# =============================================
# DATABASE ENGINE 数据库连接 (REQ-06)
# =============================================
def get_engine():
    try:
        conn_str = (
            f"mssql+pyodbc://@{SERVER_NAME}/{DATABASE_NAME}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
        engine = sa.create_engine(conn_str, fast_executemany=True)
        log.info("Database connection successful")
        return engine  # ✅ 必须返回 engine
    except Exception as e:
        log.exception("FAILED connecting to database")
        raise

# ============================================================
# HELPER: UPSERT (REQ-08, REQ-13)
# 幂等 + 增量加载
# ============================================================
def upsert_dimension(engine, table_name, pk_col, df_new):
    with engine.connect() as conn:
        existing = pd.read_sql(f"SELECT {pk_col} FROM dbo.{table_name}", conn)

    existing_ids = set(existing[pk_col].tolist())
    df_insert = df_new[~df_new[pk_col].isin(existing_ids)].copy()

    if df_insert.empty:
        log.info(f"  SKIP   {table_name:<35} — no new rows")
        return 0

    df_insert["etl_created_at"] = datetime.now()

    try:
        with engine.begin() as conn:
            df_insert.to_sql(
                table_name,
                con=conn,
                schema="dbo",
                if_exists="append",
                index=False
                # fast_executemany handles batching (REQ-16)
            )

        log.info(f"  COMMIT {table_name:<35} — {len(df_insert)} new rows inserted")
        return len(df_insert)

    except Exception as exc:
        log.error(f"  ROLLBACK {table_name} — {exc}")
        raise


# ============================================================
# PYTHON VALIDATION LAYER (REQ-09)
# ============================================================
def validate_data(df):
    log.info("VALIDATION LAYER running...")

    # Example checks
    assert df["hour_of_day"].dropna().between(0,23).all(), "Invalid hour_of_day"
    assert (df["property_damage_value"] >= 0).all(), "Negative damage"
    assert df["latitude"].dropna().between(49,60).all(), "Invalid latitude"
    assert df["longitude"].dropna().between(-120,-110).all(), "Invalid longitude"

    log.info("Validation passed")

# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline():

    start_time = datetime.now()
    log.info("="*60)
    log.info("ETL STARTED")
    log.info("="*60)

    engine = get_engine()

    try:
        # ====================================================
        # STEP 1 — EXTRACT (REQ-05)
        # ====================================================
        log.info("Loading Excel Workbook...")
        try:
            df = pd.read_excel(EXCEL_PATH, engine="openpyxl")
            log.info(f"Extracted {len(df)} rows, {len(df.columns)} columns from Excel")
        except Exception as e:
            log.error(f"FAILED to load Excel: {e}")
            raise

        # ====================================================
        # STEP 2 — TRANSFORM — The Cleaning Room
        # 数据清洗核心层（Data Quality + Standardization）
        # ====================================================
        log.info("--- TRANSFORM ---")

        # --------------------------------------------------
        # 2a. Remove Duplicates
        # 去重（保证唯一性）
        # --------------------------------------------------
        before = len(df)
        df = df.drop_duplicates()
        log.info(f"Duplicates removed: {before - len(df)} | Remaining: {len(df)}")


        # --------------------------------------------------
        # 2b. Standardize Column Names
        # 列名标准化（符合SQL命名规范）
        # --------------------------------------------------
        def clean_column_names(cols):
            return [
                re.sub(r'_+', '_', re.sub(r'[^0-9a-zA-Z]', '_', str(c))).strip('_').lower()
                for c in cols
            ]
        df.columns = clean_column_names(df.columns)

        # --------------------------------------------------
        # 2c. Date Parsing (Hybrid Strategy)
        # 日期解析（高性能 + 高精度双策略）
        # --------------------------------------------------

        # Step 1: Fast parsing (vectorized)
        df["incident_date"] = pd.to_datetime(
            df["incident_date"],
            dayfirst=True,         # 加拿大格式优先
            errors="coerce"
        )

        df["reported_date"] = pd.to_datetime(
            df["reported_date"],
            dayfirst=True,
            errors="coerce"
        )

        # Step 2: Fallback for failed rows（精细解析）
        DATE_FORMATS = [
            "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d",
            "%d-%m-%Y", "%d-%b-%Y", "%b-%d-%Y"
        ]

        def fallback_parse(val):
            if pd.isna(val):
                return pd.NaT
            for fmt in DATE_FORMATS:
                try:
                    return datetime.strptime(str(val), fmt)
                except:
                    continue
            return pd.NaT

        mask = df["incident_date"].isna()
        df.loc[mask, "incident_date"] = df.loc[mask, "incident_date"].apply(fallback_parse)

        log.info(f"Date parsing complete | Failed: {df['incident_date'].isna().sum()}")


        # --------------------------------------------------
        # 2d. Time Feature Engineering 时间特征重建（保证一致性）
        # --------------------------------------------------
        dt = pd.to_datetime(df["incident_date"], errors="coerce")
        df["year"]  = dt.dt.year
        df["month"] = dt.dt.month

        df["hour_of_day"] = pd.to_numeric(df["hour_of_day"], errors="coerce")
        df.loc[~df["hour_of_day"].between(0, 23), "hour_of_day"] = np.nan

        # --------------------------------------------------
        # 2e. Standardize Categorical Fields 分类字段标准化
        # --------------------------------------------------

        # Day of week mapping
        DAY_MAP = {
            "mon": "Monday", "monday": "Monday",
            "tue": "Tuesday", "tuesday": "Tuesday",
            "wed": "Wednesday", "wednesday": "Wednesday",
            "thu": "Thursday", "thur": "Thursday", "thursday": "Thursday",
            "fri": "Friday", "friday": "Friday",
            "sat": "Saturday", "saturday": "Saturday",
            "sun": "Sunday", "sunday": "Sunday"
        }

        df["day_of_week"] = (
            df["day_of_week"].astype(str).str.strip().str.lower()
            .map(DAY_MAP).fillna("Unknown")
        )

        # Neighborhood 标准化
        df["neighborhood"] = df["neighborhood"].astype(str).str.strip().str.title()


        # --------------------------------------------------
        # 2f. Boolean Normalization 布尔字段标准化
        # --------------------------------------------------
        bool_map = {**dict.fromkeys(["y","yes","1","true"], "Yes"),
                    **dict.fromkeys(["n","no","0","false"], "No")}

        df["violent_flag"] = df["violent_flag"].str.lower().map(bool_map).fillna("Unknown")
        df["arrest_made"] = df["arrest_made"].str.lower().map(bool_map).fillna("Unknown")

        # --------------------------------------------------
        # 2g. Weapon Normalization 武器分类标准化
        # --------------------------------------------------
        WEAPON_MAP = {
            "gun": "Firearm", "firearm": "Firearm",
            "knife": "Knife",
            "blunt": "Blunt Object",
            "none": "None",
            "unknown": "Unknown"
        }

        df["weapon_used"] = (
            df["weapon_used"].astype(str).str.lower().str.strip()
            .map(WEAPON_MAP).fillna("Unknown")
        )

        # --------------------------------------------------
        # 2h. Numeric Cleaning + Outlier Handling 数值清洗 + 异常值处理
        # --------------------------------------------------
        df["property_damage_value"] = pd.to_numeric(df["property_damage_value"], errors="coerce")
        df["response_time_minutes"] = pd.to_numeric(df["response_time_minutes"], errors="coerce")

        df["property_damage_value"] = df["property_damage_value"].clip(lower=0)
        df["response_time_minutes"] = df["response_time_minutes"].clip(lower=0)

        # --------------------------------------------------
        # 2i. Null Imputation 缺失值填补
        # --------------------------------------------------
        df["police_service"] = df["police_service"].fillna("Unknown")
        df["crime_subtype"]  = df["crime_subtype"].fillna("Unspecified")

        df["response_time_minutes"] = df["response_time_minutes"].fillna(df["response_time_minutes"].median())
        df["property_damage_value"] = df["property_damage_value"].fillna(0)

        # --------------------------------------------------
        # 2j. Data Validation (REQ-9) 数据范围校验（Alberta范围）
        # --------------------------------------------------
        df.loc[~df["latitude"].between(49, 60), "latitude"] = np.nan
        df.loc[~df["longitude"].between(-120, -110), "longitude"] = np.nan

        # --------------------------------------------------
        # 2k. PII Masking (REQ-15) 隐私保护（年龄分桶）
        # --------------------------------------------------
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

         # ====================================================
        # STEP 3 — FEATURE ENHANCEMENT — Crime Risk Modeling 风险评分建模（Rule-based + Vectorized）
        # ====================================================
        log.info("--- FEATURE ENHANCEMENT ---")

        # --------------------------------------------------
        # 3a. Violent Score 暴力评分
        # --------------------------------------------------
        df["_s_violent"] = df["violent_flag"].map({
            "Yes": 3,
            "No": 0,
            "Unknown": 1
        }).fillna(1)

        # --------------------------------------------------
        # 3b. Crime Severity Score（Vectorized） 犯罪严重程度评分（向量化替代 apply）
        # --------------------------------------------------
        HIGH_CAT   = {"Assault", "Sexual Offence", "Robbery", "Arson"}
        MEDIUM_CAT = {"Break and Enter", "Drug Offence", "Theft"}

        HIGH_SUB = {
            "Aggravated Assault", "Sexual Assault", "Domestic Assault",
            "Carjacking", "Street Robbery", "Exploitation",
            "Structure Fire", "Wildland Fire", "Vehicle Fire"
        }

        MEDIUM_SUB = {
            "Residential B&E", "Commercial B&E", "Trafficking", "Distribution",
            "Theft Over $5000", "Vehicle B&E", "Commercial Robbery"
        }

        df["_s_crime"] = 0

        df.loc[
            df["crime_category"].isin(HIGH_CAT) |
            df["crime_subtype"].isin(HIGH_SUB),
            "_s_crime"
        ] = 4

        df.loc[
            df["crime_category"].isin(MEDIUM_CAT) |
            df["crime_subtype"].isin(MEDIUM_SUB),
            "_s_crime"
        ] = 2

        # --------------------------------------------------
        # 3c. Weapon Score 武器风险评分
        # --------------------------------------------------
        df["_s_weapon"] = df["weapon_used"].map({
            "Firearm": 3,
            "Knife": 2,
            "Blunt Object": 2,
            "Other": 1,
            "Unknown": 0,
            "None": 0
        }).fillna(0)

        # --------------------------------------------------
        # 4d. Damage Score（Relative）财产损失评分（相对中位数）
        # --------------------------------------------------
        median_damage = df["property_damage_value"].median()
        df["_s_damage"] = (df["property_damage_value"] > median_damage).astype(int)

        # --------------------------------------------------
        # 4e. Response Time Score 响应时间评分（慢 = 风险高）
        # --------------------------------------------------
        median_response = df["response_time_minutes"].median()
        df["_s_response"] = (df["response_time_minutes"] > median_response).astype(int)

        # --------------------------------------------------
        # 4f. Composite Risk Score 综合评分（多维度加权）
        # --------------------------------------------------
        df["_composite_score"] = df[
            ["_s_violent", "_s_crime", "_s_weapon", "_s_damage", "_s_response"]
        ].sum(axis=1)

        # --------------------------------------------------
        # 4g. Risk Level Classification 风险等级划分
        # --------------------------------------------------
        df["crime_risk_level"] = pd.cut(
            df["_composite_score"],
            bins=[-1, 5, 8, 100],
            labels=["Low", "Medium", "High"]
        )
        
        # --------------------------------------------------
        # 4h. Cleanup Temporary Columns 清理中间变量（保持表干净）
        # --------------------------------------------------
        df.drop(columns=[c for c in df.columns if c.startswith("_s") or c == "_composite_score"], inplace=True)

        # --------------------------------------------------
        # 4i. Logging Distribution 记录分布（用于数据验证）
        # --------------------------------------------------
        risk_dist = df["crime_risk_level"].value_counts().to_dict()
        log.info(f"crime_risk_level derived | Distribution: {risk_dist}")

        # ====================================================
        # STEP 5 — VALIDATION (REQ-09) 数据验证：防止空表或缺失列写入
        # ====================================================
        log.info("--- VALIDATION ---")
        if df.empty:
            raise ValueError("Dataset is empty — aborting load to protect staging table")

        STAGING_COLUMNS = [
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

        missing_cols = [c for c in STAGING_COLUMNS if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        log.info(f"Validation passed — {len(df)} rows ready for staging")

        # ====================================================
        # STEP 5 — BACKUP (REQ-19) 自动备份 staging 表，防止数据丢失
        # ====================================================
        log.info("--- BACKUP Staging Table ---")
        with engine.begin() as conn:
            conn.execute(sa.text("""
                IF OBJECT_ID('dbo.stg_crime_incidents_backup', 'U') IS NOT NULL
                    DROP TABLE dbo.stg_crime_incidents_backup;
                SELECT * INTO dbo.stg_crime_incidents_backup
                FROM dbo.stg_crime_incidents;
            """))
        log.info("Backup complete: stg_crime_incidents -> stg_crime_incidents_backup")

        # ====================================================
        # STEP 5 — STAGING LOAD (REQ-12) 将数据加载到 Staging 表
        # ====================================================
        log.info("--- LOAD: Staging Table ---")
        try:
            with engine.begin() as conn:
                # 清空表
                conn.execute(sa.text("TRUNCATE TABLE dbo.stg_crime_incidents"))

                # 写入 staging，fast_executemany 批量优化
                df[STAGING_COLUMNS].to_sql(
                    "stg_crime_incidents",
                    con=conn,
                    schema="dbo",
                    if_exists="append",
                    index=False
                )
            log.info(f"COMMIT stg_crime_incidents — {len(df)} rows loaded")
        except Exception as exc:
            log.error(f"ROLLBACK stg_crime_incidents — {exc}")
            raise

        # ====================================================
        # STEP 6 — LOAD Dimension Tables 按表事务逐个 upsert 维度表
        # ====================================================
        log.info("--- LOAD: Dimension Tables ---")

        # 1️⃣ Municipality
        KNOWN_MUNICIPALITIES = {
            "Calgary": 1, "Edmonton": 2, "Red Deer": 3, "Lethbridge": 4,
            "St. Albert": 5, "Medicine Hat": 6, "Grande Prairie": 7,
            "Airdrie": 8, "Spruce Grove": 9, "Fort McMurray": 10
        }
        df_muni = df[["municipality"]].drop_duplicates().reset_index(drop=True)
        df_muni.insert(0, "municipality_id",
            df_muni["municipality"].map(KNOWN_MUNICIPALITIES)
            .fillna(df_muni["municipality"].apply(lambda x: abs(hash(x)) % 9000 + 1000))
            .astype(int)
        )
        upsert_dimension(engine, "dim_municipality", "municipality_id", df_muni)

        # 2️⃣ Location
        df_loc = (
            df[["neighborhood","postal_code","latitude","longitude","municipality"]]
            .drop_duplicates(subset=["neighborhood","postal_code"])
            .reset_index(drop=True)
            .merge(df_muni, on="municipality", how="left")
        )
        df_loc.drop(columns=["municipality"], inplace=True)
        df_loc.insert(0, "location_id", range(1, len(df_loc)+1))
        upsert_dimension(engine, "dim_location", "location_id", df_loc)

        # 3️⃣ Date
        df_date = (
            df[["incident_date","year","month","day_of_week","hour_of_day"]]
            .dropna(subset=["incident_date"])
            .drop_duplicates(subset=["incident_date"])
            .reset_index(drop=True)
        )
        df_date.insert(0, "date_id", range(1, len(df_date)+1))
        upsert_dimension(engine, "dim_date", "date_id", df_date)

        # 4️⃣ Crime Type       
        df_crime = df[["crime_category","crime_subtype"]].drop_duplicates().reset_index(drop=True)
        df_crime.insert(0, "crime_type_id", range(1, len(df_crime)+1))
        upsert_dimension(engine, "dim_crime_type", "crime_type_id", df_crime)

        # 5️⃣ Weapon
        df_weapon = df[["weapon_used"]].drop_duplicates().reset_index(drop=True)
        df_weapon.insert(0, "weapon_id", range(1, len(df_weapon)+1))
        upsert_dimension(engine, "dim_weapon", "weapon_id", df_weapon)

        # 6️⃣ Demographics
        df_demo = (
            df[["offender_age_masked","victim_age_masked","area_population_estimate"]]
            .drop_duplicates().reset_index(drop=True)
        )
        df_demo.insert(0, "demographic_id", range(1, len(df_demo)+1))
        upsert_dimension(engine, "dim_demographics", "demographic_id", df_demo)

        # 7️⃣ Socioeconomic
        df_socio = (
            df[["median_income_estimate","unemployment_rate","housing_density","commercial_activity_index"]]
            .drop_duplicates().reset_index(drop=True)
        )
        df_socio.insert(0, "socioeconomic_id", range(1, len(df_socio)+1))
        upsert_dimension(engine, "dim_socioeconomic", "socioeconomic_id", df_socio)

        # 8️⃣ Police Response
        df_police = (
            df[["police_service","response_time_minutes","units_dispatched","arrest_made"]]
            .drop_duplicates().reset_index(drop=True)
        )
        df_police.insert(0, "police_response_id", range(1, len(df_police)+1))
        upsert_dimension(engine, "dim_police_response", "police_response_id", df_police)

        # 9️⃣ Risk Levels (固定 3 档)
        df_risk = pd.DataFrame({
            "risk_id": [1,2,3],
            "crime_risk_level": ["Low","Medium","High"]
        })
        upsert_dimension(engine, "dim_risk", "risk_id", df_risk)

        log.info("Dimension tables upsert complete ✅")

        # ====================================================
        # STEP 7 — LOAD Fact Table
        # ====================================================
        log.info("--- LOAD: Fact Table ---")

        
        # 读取所有维度映射表
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

            # -------------------------------
            # 合并维度
            # -------------------------------
            df_fact = df.copy()
            df_fact["incident_date_str"] = df_fact["incident_date"].astype(str)
            date_map["incident_date"] = date_map["incident_date"].astype(str)

            df_fact = (
                df_fact.merge(muni_map,   on="municipality", how="left")
                    .merge(loc_map,  on=["neighborhood","postal_code"], how="left")
                    .merge(date_map, left_on="incident_date_str", right_on="incident_date", how="left")
                    .merge(crime_map,  on=["crime_category","crime_subtype"], how="left")
                    .merge(weapon_map, on="weapon_used", how="left")
                    .merge(demo_map,   on=["offender_age_masked","victim_age_masked","area_population_estimate"], how="left")
                    .merge(socio_map,  on=["median_income_estimate","unemployment_rate","housing_density","commercial_activity_index"], how="left")
                    .merge(police_map, on=["police_service","response_time_minutes","units_dispatched","arrest_made"], how="left")
                    .merge(risk_map,   on="crime_risk_level", how="left")
            )

            # -------------------------------
            # 提取 Fact 核心列 + 去重
            # -------------------------------
            df_fact_final = df_fact[[
                "incident_id","location_id","date_id","crime_type_id",
                "weapon_id","demographic_id","socioeconomic_id",
                "police_response_id","risk_id",
                "violent_flag","property_damage_value","reported_date"
            ]].copy()

            before_dedup = len(df_fact_final)
            df_fact_final = df_fact_final.drop_duplicates(subset=["incident_id"])
            log.info(f"Fact deduplication: {before_dedup - len(df_fact_final)} removed | {len(df_fact_final)} unique incidents")

            # -------------------------------
            # 增量插入 Fact
            # -------------------------------
            with engine.connect() as conn:
                existing_ids = set(pd.read_sql("SELECT incident_id FROM dbo.fact_incident", conn)["incident_id"].tolist())

            df_new_facts = df_fact_final[~df_fact_final["incident_id"].isin(existing_ids)].copy()
            if len(df_new_facts) > 0:
                df_new_facts["etl_created_at"] = datetime.now()
                with engine.begin() as conn:
                    df_new_facts.to_sql("fact_incident", con=conn, schema="dbo", if_exists="append", index=False)
                log.info(f"COMMIT fact_incident — {len(df_new_facts)} new rows inserted")
            else:
                log.info("fact_incident — no new rows (idempotent re-run)")

            # ====================================================
            # STEP 8 — DATA QUALITY AUDIT
            # ====================================================
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

            elapsed = (datetime.now() - start_time).total_seconds()
            log.info("=" * 90)
            log.info(f"WAREHOUSE COMPLETE — elapsed {elapsed:.1f}s | log: {LOG_PATH}")
            log.info("=" * 90)

    except Exception as e:
        log.error(f"PIPELINE FAILED: {str(e)}")
        raise

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_pipeline()
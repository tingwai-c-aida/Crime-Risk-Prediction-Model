import pandas as pd
import random
from datetime import datetime, timedelta

# =====================================================
# LOAD REAL DATA (FOR MUNICIPALITY PROFILES)
# =====================================================
SOURCE_FILE = r"C:\Users\feelw\Desktop\evelyn\winter 2026\AIDA Final\alberta_crime_dataset_10k.xlsx"
source_df = pd.read_excel(SOURCE_FILE, engine="openpyxl")

# Columns bound to municipality
LINKED_COLS = [
    "neighborhood",
    "postal_code",
    "latitude",
    "longitude",
    "police_service",
    "area_population_estimate",
    "median_income_estimate",
    "unemployment_rate",
    "housing_density",
    "commercial_activity_index"
]

# =====================================================
# BUILD MUNICIPALITY PROFILES
# =====================================================
municipality_profiles = {}

for muni, group in source_df.groupby("municipality"):
    profile = {}

    # categorical pools
    profile["neighborhoods"] = (
        group["neighborhood"].dropna().unique().tolist()
    )
    profile["postal_codes"] = (
        group["postal_code"].dropna().unique().tolist()
    )
    profile["police_services"] = (
        group["police_service"].dropna().unique().tolist()
    )

    # numeric ranges
    def num_range(col):
        g = group[col].dropna()
        return (g.min(), g.max()) if not g.empty else (None, None)

    profile["lat_range"] = num_range("latitude")
    profile["lon_range"] = num_range("longitude")
    profile["population_range"] = num_range("area_population_estimate")
    profile["income_range"] = num_range("median_income_estimate")
    profile["unemployment_range"] = num_range("unemployment_rate")
    profile["housing_range"] = num_range("housing_density")
    profile["commercial_range"] = num_range("commercial_activity_index")

    municipality_profiles[muni] = profile

MUNICIPALITIES = list(municipality_profiles.keys())

# =====================================================
# GLOBAL CONFIG
# =====================================================
NUM_FILES = 5
ROWS_PER_FILE = 50
START_INCIDENT_ID = 900_000
NULL_PROB = 0.12

VALID_SUBTYPE_PROB = 0.80
CROSS_SUBTYPE_PROB = 0.10

DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
YES_NO_UNKNOWN = ["Yes","No","Unknown"]
WEAPONS = ["None","Knife","Firearm","Blunt Object","Unknown"]

# =====================================================
# CRIME CATEGORY → SUBTYPE
# =====================================================
CATEGORY_MAP = {
    "Assault": ["Domestic Assault","Aggravated Assault","Common Assault","Sexual Assault"],
    "Robbery": ["Street Robbery","Commercial Robbery","Bank Robbery","Armed Robbery"],
    "Theft": ["Petty Theft","Shoplifting","Vehicle Theft","Bicycle Theft"],
    "Arson": ["Structure Fire","Vehicle Fire","Wildland Fire"],
    "Fraud": ["Credit Card Fraud","Insurance Fraud","Wire Fraud","Identity Theft"],
    "Drug Offense": ["Possession","Trafficking","Distribution","Production"],
    "Mischief": ["Graffiti","Public Mischief","Criminal Mischief"]
}
ALL_SUBTYPES = [s for v in CATEGORY_MAP.values() for s in v]

# =====================================================
# HELPERS
# =====================================================
def maybe_null(val):
    return None if random.random() < NULL_PROB else val

def random_date():
    start = datetime(2018,1,1)
    end = datetime(2024,12,31)
    return start + timedelta(days=random.randint(0,(end-start).days))

def choose_subtype(cat):
    r = random.random()
    if r < VALID_SUBTYPE_PROB:
        return random.choice(CATEGORY_MAP[cat])
    elif r < VALID_SUBTYPE_PROB + CROSS_SUBTYPE_PROB:
        return random.choice(ALL_SUBTYPES)
    else:
        return None

def rand_from_range(rng):
    if rng[0] is None:
        return None
    return random.uniform(rng[0], rng[1])

# =====================================================
# ROW GENERATION
# =====================================================
def generate_rows(start_id, n):
    rows = []

    for i in range(n):
        incident_id = start_id + i
        incident_date = random_date()
        reported_date = incident_date + timedelta(days=random.randint(0,5))

        municipality = random.choice(MUNICIPALITIES)
        profile = municipality_profiles[municipality]
        category = random.choice(list(CATEGORY_MAP.keys()))

        rows.append({
            # HARD REQUIRED
            "incident_id": incident_id,
            "municipality": municipality,

            # MUNICIPALITY-LINKED
            "neighborhood": maybe_null(random.choice(profile["neighborhoods"])),
            "postal_code": maybe_null(random.choice(profile["postal_codes"])),
            "latitude": maybe_null(rand_from_range(profile["lat_range"])),
            "longitude": maybe_null(rand_from_range(profile["lon_range"])),
            "police_service": maybe_null(random.choice(profile["police_services"])),

            "area_population_estimate": maybe_null(rand_from_range(profile["population_range"])),
            "median_income_estimate": maybe_null(rand_from_range(profile["income_range"])),
            "unemployment_rate": maybe_null(rand_from_range(profile["unemployment_range"])),
            "housing_density": maybe_null(rand_from_range(profile["housing_range"])),
            "commercial_activity_index": maybe_null(rand_from_range(profile["commercial_range"])),

            # TEMPORAL
            "incident_date": maybe_null(incident_date.date()),
            "reported_date": maybe_null(reported_date.date()),
            "year": maybe_null(incident_date.year),
            "month": maybe_null(incident_date.month),
            "day_of_week": maybe_null(random.choice(DAYS)),
            "hour_of_day": maybe_null(random.randint(0,23)),

            # CRIME
            "crime_category": maybe_null(category),
            "crime_subtype": maybe_null(choose_subtype(category)),
            "weapon_used": maybe_null(random.choice(WEAPONS)),
            "violent_flag": maybe_null(random.choice(YES_NO_UNKNOWN)),
            "arrest_made": maybe_null(random.choice(YES_NO_UNKNOWN)),

            # DERIVED DOWNSTREAM
            "crime_risk_level": None
        })

    return pd.DataFrame(rows)

# =====================================================
# OUTPUT FILES
# =====================================================
for i in range(NUM_FILES):
    df = generate_rows(
        START_INCIDENT_ID + i * ROWS_PER_FILE,
        ROWS_PER_FILE
    )
    fname = f"synthetic_crime_batch_{i+1}.xlsx"
    df.to_excel(fname, index=False)
    print(f"✅ Generated {fname}")
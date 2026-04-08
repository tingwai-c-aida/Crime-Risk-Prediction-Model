import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path

# ======================================================
# GLOBAL CONFIG
# ======================================================
NUM_FILES = 5
ROWS_PER_FILE = 10000
START_INCIDENT_ID = 900_000

DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d-%b-%Y"]

YES_NO_UNKNOWN = ["Yes", "No", "Unknown"]

WEAPONS = [
    "Firearm",
    "Knife",
    "Blunt Object",
    "Other",
    "Unknown",
    "None"
]

CRIME_CATEGORIES = {
    "Assault": ["Common Assault", "Aggravated Assault", "Domestic Assault"],
    "Robbery": ["Street Robbery", "Commercial Robbery", "Carjacking"],
    "Theft": ["Shoplifting", "Vehicle Theft", "Theft Under $5000", "Theft Over $5000"],
    "Fraud": ["Credit Card Fraud", "Identity Theft", "Online Fraud"],
    "Drug Offence": ["Possession", "Trafficking", "Distribution", "Production"],
    "Arson": ["Structure Fire", "Vehicle Fire", "Wildland Fire"],
    "Mischief": ["Public Mischief", "Disturbing the Peace"],
    "Break and Enter": ["Residential B&E", "Commercial B&E", "Vehicle B&E"],
    "Sexual Offence": ["Sexual Assault", "Public Indecency", "Exploitation"]
}

POLICE_SERVICES = [
    "Edmonton Police Service",
    "Calgary Police Service",
    "Lethbridge Police Service",
    "Medicine Hat Police Service",
    "RCMP",
    "Municipal Police"
]

# ======================================================
# MUNICIPALITY PROFILES
# (Realistic and internally consistent)
# ======================================================
MUNICIPALITY_PROFILES = {
    "Edmonton": {
        "neighborhoods": ["Downtown", "Whyte Avenue", "Strathcona", "Oliver", "Garneau"],
        "population": (900_000, 1_100_000),
        "income": (65_000, 105_000),
        "unemployment": (4.5, 8.0),
        "density": (1500, 4000),
        "commercial": (6.0, 10.0),
        "lat": (53.52, 53.58),
        "lon": (-113.55, -113.45)
    },
    "Calgary": {
        "neighborhoods": ["Beltline", "Forest Lawn", "Bridgeland", "Sunridge", "Inglewood"],
        "population": (1_200_000, 1_500_000),
        "income": (70_000, 115_000),
        "unemployment": (4.0, 7.5),
        "density": (1400, 3800),
        "commercial": (6.5, 10.0),
        "lat": (51.03, 51.08),
        "lon": (-114.08, -114.00)
    },
    "Red Deer": {
        "neighborhoods": ["Downtown", "Clearview", "Normandeau", "Kentwood"],
        "population": (95_000, 120_000),
        "income": (60_000, 95_000),
        "unemployment": (4.8, 8.5),
        "density": (400, 1500),
        "commercial": (3.5, 7.0),
        "lat": (52.24, 52.30),
        "lon": (-113.83, -113.75)
    },
    "Grande Prairie": {
        "neighborhoods": ["Downtown", "Pinnacle", "Crystal Landing"],
        "population": (65_000, 80_000),
        "income": (60_000, 100_000),
        "unemployment": (4.5, 8.0),
        "density": (350, 1200),
        "commercial": (3.0, 6.5),
        "lat": (55.15, 55.21),
        "lon": (-118.80, -118.72)
    },
    "Lethbridge": {
        "neighborhoods": ["Downtown", "Henderson", "Bower", "Sundial"],
        "population": (95_000, 110_000),
        "income": (55_000, 95_000),
        "unemployment": (4.5, 9.0),
        "density": (400, 1600),
        "commercial": (3.5, 7.5),
        "lat": (49.69, 49.73),
        "lon": (-112.86, -112.80)
    },
    "Medicine Hat": {
        "neighborhoods": ["Downtown", "Crescent Heights", "River Flats", "Ross Glen"],
        "population": (60_000, 80_000),
        "income": (55_000, 90_000),
        "unemployment": (5.0, 9.5),
        "density": (300, 1200),
        "commercial": (2.5, 6.0),
        "lat": (50.00, 50.06),
        "lon": (-110.72, -110.66)
    },
    "St. Albert": {
        "neighborhoods": ["Downtown", "Erin Ridge", "Lacombe Park", "Braeside"],
        "population": (65_000, 75_000),
        "income": (75_000, 115_000),
        "unemployment": (3.5, 7.0),
        "density": (600, 1800),
        "commercial": (3.5, 6.5),
        "lat": (53.61, 53.67),
        "lon": (-113.65, -113.58)
    }
}

# ======================================================
# NULL PROBABILITIES
# (Derived from real missingness in original dataset)
# ======================================================
NULL_PROBS = {
    "weapon_used": 0.44,
    "arrest_made": 0.22,
    "response_time_minutes": 0.20,
    "police_service": 0.18,
    "violent_flag": 0.16,
    "housing_density": 0.13,
    "reported_date": 0.12,
    "unemployment_rate": 0.11,
    "units_dispatched": 0.10,
    "commercial_activity_index": 0.10,
    "area_population_estimate": 0.10,
    "postal_code": 0.09,
    "incident_date": 0.09,
    "year": 0.09,
    "month": 0.09,
    "median_income_estimate": 0.08,
    "neighborhood": 0.08,
    "victim_age": 0.07,
    "offender_age": 0.07,
    "day_of_week": 0.06,
    "latitude": 0.03,
    "longitude": 0.03
}

# ======================================================
# HELPERS
# ======================================================
def maybe_null(value, prob):
    return None if random.random() < prob else value

def random_date():
    start = datetime(2018, 1, 1)
    end = datetime(2024, 12, 31)
    d = start + timedelta(days=random.randint(0, (end - start).days))
    return d.strftime(random.choice(DATE_FORMATS))

# ======================================================
# ROW GENERATION
# ======================================================
def generate_rows(start_id, n):
    rows = []

    for i in range(n):
        municipality = random.choice(list(MUNICIPALITY_PROFILES.keys()))
        profile = MUNICIPALITY_PROFILES[municipality]
        category = random.choice(list(CRIME_CATEGORIES.keys()))

        incident_date = random_date()

        lat = round(random.uniform(*profile["lat"]), 6)
        lon = round(random.uniform(*profile["lon"]), 6)

        rows.append({
            "incident_id": start_id + i,
            "municipality": municipality,

            "neighborhood": maybe_null(
                random.choice(profile["neighborhoods"]),
                NULL_PROBS["neighborhood"]
            ),
            "postal_code": maybe_null(
                f"T{random.randint(0,9)}{random.randint(0,9)} "
                f"{random.randint(0,9)}{random.randint(0,9)}{random.randint(0,9)}",
                NULL_PROBS["postal_code"]
            ),

            "latitude": maybe_null(lat, NULL_PROBS["latitude"]),
            "longitude": maybe_null(lon, NULL_PROBS["longitude"]),

            "incident_date": maybe_null(incident_date, NULL_PROBS["incident_date"]),
            "reported_date": maybe_null(incident_date, NULL_PROBS["reported_date"]),

            "year": maybe_null(random.randint(2018, 2024), NULL_PROBS["year"]),
            "month": maybe_null(random.randint(1, 12), NULL_PROBS["month"]),
            "day_of_week": maybe_null(
                random.choice(["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]),
                NULL_PROBS["day_of_week"]
            ),
            "hour_of_day": random.randint(0, 23),

            "crime_category": category,
            "crime_subtype": random.choice(CRIME_CATEGORIES[category]),

            "weapon_used": maybe_null(random.choice(WEAPONS), NULL_PROBS["weapon_used"]),
            "violent_flag": maybe_null(random.choice(YES_NO_UNKNOWN), NULL_PROBS["violent_flag"]),

            "property_damage_value": round(random.uniform(0, 50_000), 2),

            "police_service": maybe_null(
                random.choice(POLICE_SERVICES),
                NULL_PROBS["police_service"]
            ),
            "response_time_minutes": maybe_null(
                round(random.uniform(1, 120), 2),
                NULL_PROBS["response_time_minutes"]
            ),
            "units_dispatched": maybe_null(
                random.randint(1, 6),
                NULL_PROBS["units_dispatched"]
            ),
            "arrest_made": maybe_null(
                random.choice(YES_NO_UNKNOWN),
                NULL_PROBS["arrest_made"]
            ),

            "offender_age": maybe_null(random.randint(14, 75), NULL_PROBS["offender_age"]),
            "victim_age": maybe_null(random.randint(14, 85), NULL_PROBS["victim_age"]),

            "area_population_estimate": maybe_null(
                random.randint(*profile["population"]),
                NULL_PROBS["area_population_estimate"]
            ),
            "median_income_estimate": maybe_null(
                random.randint(*profile["income"]),
                NULL_PROBS["median_income_estimate"]
            ),
            "unemployment_rate": maybe_null(
                round(random.uniform(*profile["unemployment"]), 2),
                NULL_PROBS["unemployment_rate"]
            ),
            "housing_density": maybe_null(
                round(random.uniform(*profile["density"]), 2),
                NULL_PROBS["housing_density"]
            ),
            "commercial_activity_index": maybe_null(
                round(random.uniform(*profile["commercial"]), 2),
                NULL_PROBS["commercial_activity_index"]
            ),
        })

    return pd.DataFrame(rows)

# ======================================================
# OUTPUT FILES
# ======================================================
output_dir = Path(
    r"C:\Users\selen\OneDrive\Desktop\Evelyn\winter 2026\AIDA Final\Simulated table"
)
output_dir.mkdir(exist_ok=True)

for i in range(NUM_FILES):
    df = generate_rows(
        START_INCIDENT_ID + i * ROWS_PER_FILE,
        ROWS_PER_FILE
    )
    out_path = output_dir / f"synthetic_batch_{i+1}.xlsx"
    df.to_excel(out_path, index=False)
    print(f"✅ Generated {out_path}")
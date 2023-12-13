import pandas as pd
import itertools

PERIODS = {
    "AM": [str(hour) for hour in range(7, 9)],
    "IP": [str(hour) for hour in range(9, 15)],
    "PM": [str(hour) for hour in range(15, 19)],
    "OP": [str(hour) for hour in itertools.chain(range(0, 7), range(19, 24))],
    "Daily": [str(hour) for hour in range(0, 24)],
}

PV_MODES = ["pv"]
PT_MODES = ["bus", "rail"]

TRIP_PURPOSES = ["home-based work", "home-based education", 
                "home-based other", "employment-based", "non home-based"]

DEMOGRAPHIC_TYPE = ["gender", "age_group", "income_group", 
                   "labour_force_status", "student_status", "car_availability"] 

BOUNDARIES = ["regional_council"]
#!!!
SCENARIOS = ["pt_variations/baseline", "pt_variations/flattened_2x"]
#!!!

COLUMNS = [
    "Group",
    "Boundaries",
    "Region",
    "Metric",
    "Time Period",
    "Mode",
    "Trip Purpose",
    "Demographic Type",
    "Demographic Group",
    "Value",
]
INDEX_COLS = [
    "Group",
    "Boundaries",
    "Region",
    "Metric",
    "Time Period",
    "Mode",
    "Trip Purpose",
    "Demographic Type",
    "Demographic Group",
]
ROAD_CAPS = [0.8, 1.0]


REGION_MAPPING = {
    1: "Northland Region",
    2: "Auckland Region",
    3: "Waikato Region",
    4: "Bay of Plenty Region",
    5: "Gisborne Region",
    6: "Hawke's Bay Region",
    7: "Taranaki Region",
    8: "Manawatu-Whanganui Region",
    9: "Wellington Region",
    10: "Marlborough Region",
    12: "West Coast Region",
    13: "Canterbury Region",
    14: "Otago Region",
    15: "Southland Region",
}

def fill_missing_columns(
    df, group, area, mode: None, period: None, 
    trip_purpose: None, demo_type: None, demo_group: None, 
    metric
) -> pd.DataFrame:
    df["Group"] = group
    df["Boundaries"] = area
    if "Region" not in df.columns:
        df["Region"] = df.index
    if "Time Period" not in df.columns:
        df["Time Period"] = period
    if "Mode" not in df.columns:
        df["Mode"] = mode
    if "Trip Purpose" not in df.columns:
        df["Trip Purpose"] = trip_purpose
    if "Demographic Type" not in df.columns:
        df["Demographic Type"] = demo_type
    if "Demographic Group" not in df.columns:
        df["Demographic Group"] = demo_group
    df["Metric"] = metric
    df = df.reset_index(drop=True)
    return df[COLUMNS]


def get_time_period(h):
    if (h >= 7) & (h < 9):
        return "AM"
    elif (h >= 9) & (h < 15):
        return "IP"
    elif (h >= 15) & (h < 18):
        return "PM"
    elif ((h >= 18) & (h < 24)) | ((h >= 0) & (h < 7)):
        return "OP"


def get_sec(time_str):
    """Get seconds from time."""
    h, m, s = time_str.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def fill_regions(df, all_regions) -> pd.DataFrame:
    regions = set(df.index)
    missing_regions = all_regions - regions
    for region in missing_regions:
        df.loc[region] = 0

    return df

def get_trip_purpose(od_activity):
    if 'home' in od_activity and 'work' in od_activity:
        trip_purpose = 'home-based work'
    elif 'home' in od_activity and 'education' in od_activity:
        trip_purpose = 'home-based education'
    elif 'home' in od_activity and 'shopping' in od_activity:
        trip_purpose = 'home-based shopping'
    elif 'home' in od_activity:
        trip_purpose = 'home-based other'
    elif 'work' in od_activity and 'business' in od_activity:
        trip_purpose = "employment-based"
    else:
        trip_purpose = 'non home-based'
    
    return trip_purpose

def get_age_group(age):
    if age < 15:
        age_group = "Under 15"
    elif 15 <= age < 40:
        age_group = "15-39"
    elif 40 <= age < 65:
        age_group = "40-64"
    else:
        age_group = "65+"
    return age_group
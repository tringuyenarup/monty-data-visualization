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

BOUNDARIES = ["regional_council", "sa2"]
SCENARIOS = ["base-ref", "future-ref"]

COLUMNS = [
    "Group",
    "Boundaries",
    "Region",
    "Metric",
    "Time Period",
    "Mode",
    "Value",
]
INDEX_COLS = [
    "Group",
    "Boundaries",
    "Region",
    "Metric",
    "Time Period",
    "Mode",
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
    df, group, area, mode: None, period: None, metric
) -> pd.DataFrame:
    df["Group"] = group
    df["Boundaries"] = area
    if "Region" not in df.columns:
        df["Region"] = df.index
    if "Time Period" not in df.columns:
        df["Time Period"] = period
    if "Mode" not in df.columns:
        df["Mode"] = mode
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

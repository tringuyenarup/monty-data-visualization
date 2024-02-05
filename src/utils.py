import pandas as pd
import itertools

PERIODS = {
    "AM": [str(hour) for hour in range(7, 9)],
    "IP": [str(hour) for hour in range(9, 15)],
    "PM": [str(hour) for hour in range(15, 19)],
    "OP": [str(hour) for hour in itertools.chain(range(0, 7), range(19, 24))],
    "Daily": [str(hour) for hour in range(0, 24)],
}

PV_MODES = ["pv"] #["pv", "freight"]
PT_MODES = ["bus", "rail", "ferry"]

TRIP_PURPOSES = ["home-based work", "home-based education", 
                "home-based other", "employment-based", "non home-based"]

DEMOGRAPHIC_TYPE = ["gender", "age_group", "income_group", 
                   "labour_force_status", "student_status", "car_availability"] 

BOUNDARIES = ["regional_council"]#["regional_council", "sa2"]#["regional_council", "sa2"]
#!!!
#SCENARIOS = ["baseline", "2x_frequency"]
#!!!
ASSIGNMENT_METRICS = {'vkt': 'Vehicle Kilometres Travelled',
                      'vht': 'Vehicle Hours Travelled',
                      'pkt': 'Passengers Kilometres Travelled',
                      'pht': 'Passengers Hours Travelled',
                      'length': 'Road Kilometres',
                      'lane_km': 'Lane Kilometres',
                      'boardings': "Boardings",
                      'alightings': "Alightings"
                      }

PT_ASSIGNMENT_METRICS = ['vkt', 'vht', 'pkt', 'pht']
BA_METRICS = ["boardings", "alightings"]
PV_ASSIGNMENT_METRICS = ['vkt', 'vht']
NETWORK_METRICS = ['length', 'lane_km']


INDEX_COLS = ['period', 'mode', 'trip_purpose', 'area_type', 'area', 'start_area', 
              'end_area', 'demographic_type', 'demographic_group']

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

PT_REGION_MAPPING = {
    "Auckland": "Auckland Region",
    "Wellington": "Wellington Region",
    "Christchurch": "Canterbury Region",
    "Waikato": "Waikato Region",
    "Bop": "Bay of Plenty Region",
    "Orc": "Otago Region",
    "Intercity": "Intercity"
    #Auckland, Wellington, Christchurch, Waikato, Intercity, BOP, and ORC
}

def get_time_period(h):
    if (h >= 7) & (h < 9):
        return "AM"
    elif (h >= 9) & (h < 15):
        return "IP"
    elif (h >= 15) & (h < 19):
        return "PM"
    elif ((h >= 19) & (h < 24)) | ((h >= 0) & (h < 7)):
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
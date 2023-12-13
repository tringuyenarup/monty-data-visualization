import pandas as pd
import os
from pathlib import Path
import logging
import warnings

from utils import *

warnings.filterwarnings("ignore")

MODE_MAPPING = {
    "car": "pv",
    "car_passenger": "pv",
    "bus": "pt",
    "rail": "pt",
    "ferry": "pt",
    "walk": "active",
    "bike": "active",
    "freight": "freight",
    "lightrail": "pt",
    "rail": "pt",
    "train": "pt",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)


def calculate_trip_metrics(scenario_path) -> pd.DataFrame:
    tmp_df = pd.DataFrame()
    logging.info("Read trips....")
    trip_df = read_trips(scenario_path)

    for mode in ["pv", "pt", "active"]:
        #logging.info(f"Calculate trip base for {mode}...")
        for area in BOUNDARIES:
            all_regions = set(trip_df[f"start_{area}"]).union(
                set(trip_df[f"end_{area}"])
            )
            for trip_purpose in TRIP_PURPOSES:
                for demo_type in DEMOGRAPHIC_TYPE:
                    logging.info(f"Calculate trip base for mode: {mode}, trip purpose: {trip_purpose}, demographic type: {demo_type}...")
                    if demo_type == "":
                        demo = "gender"
                    else:
                        demo_type = demo
                    for demo_group in trip_df[demo].unique():
                        for key, _ in PERIODS.items():
                            if key != "Daily":
                                for start_or_end in ["start", "end"]:
                                    tmp_trip_df = trip_df[
                                        (trip_df["longest_distance_mode"] == mode)
                                        & (trip_df["period"] == key)
                                        #& (trip_df["trip_purpose"] == trip_purpose)
                                    ]

                                    travel_distance = (
                                        tmp_trip_df.groupby([f"{start_or_end}_{area}"])
                                        .agg({"traveled_distance": "mean"})
                                        .rename(columns={"traveled_distance": "Value"})
                                    )
                                    travel_time = (
                                        tmp_trip_df.groupby([f"{start_or_end}_{area}"])
                                        .agg({"trav_time": "mean"})
                                        .rename(columns={"trav_time": "Value"})
                                    )

                                    trip_count = (
                                        tmp_trip_df.groupby([f"{start_or_end}_{area}"])
                                        .agg({"trip_id": "count"})
                                        .rename(columns={"trip_id": "Value"})
                                    )

                                    travel_distance = fill_regions(travel_distance, all_regions)
                                    travel_time = fill_regions(travel_time, all_regions)
                                    trip_count = fill_regions(trip_count, all_regions)

                                    travel_distance = fill_missing_columns(
                                        travel_distance,
                                        f"Average trip distance ({start_or_end}) in {area}",
                                        area,
                                        mode,
                                        key,
                                        trip_purpose,
                                        demo_type,
                                        demo_group,
                                        f"Average trip distance ({start_or_end}) in {area}",
                                    )
                                    travel_time = fill_missing_columns(
                                        travel_time,
                                        f"Average trip time ({start_or_end}) in {area}",
                                        area,
                                        mode,
                                        key,
                                        trip_purpose,
                                        demo_type,
                                        demo_group,
                                        f"Average trip time ({start_or_end}) in {area}",
                                    )
                                    trip_count = fill_missing_columns(
                                        trip_count,
                                        f"Trip counts for trip ({start_or_end}) in {area}",
                                        area,
                                        mode,
                                        key,
                                        trip_purpose,
                                        demo_type,
                                        demo_group,
                                        f"Trip counts for trip ({start_or_end}) in {area}",
                                    )
                                    if tmp_df.empty:
                                        tmp_df = pd.concat(
                                            [
                                                travel_distance[COLUMNS],
                                                travel_time[COLUMNS],
                                                trip_count[COLUMNS],
                                            ],
                                            ignore_index=True,
                                        )
                                    else:
                                        tmp_df = pd.concat(
                                            [
                                                tmp_df,
                                                travel_distance[COLUMNS],
                                                travel_time[COLUMNS],
                                                trip_count[COLUMNS],
                                            ],
                                            ignore_index=True,
                                        )

    return tmp_df


def read_trips(scenario_path) -> pd.DataFrame:
    df = pd.read_csv(os.path.join(scenario_path, "trips.csv"), low_memory=False)
    persons = pd.read_csv(os.path.join(scenario_path, "synthetic_persons.csv"), low_memory=False)
    persons = persons[['person_id', 'household_id', 'home_region', 'sa2', 'age', 'gender', 
                       'labour_force_status', 'student_status', 'hh_size', 'car_availability', 
                       'is_working_from_home', 'income_group']] #select important columns
    persons['age_group'] = persons["age"].apply(get_age_group)
    persons = persons.rename(columns={'sa2': 'home_sa2'})
    
    df['person'] = df['person'].astype(str)
    persons['person_id'] = persons['person_id'].astype(str)

    df = df.merge(persons, left_on='person', right_on = 'person_id', how='left')
    distance_bins = range(0, 40, 1)

    df["dep_time"] = (df["dep_time"].apply(get_sec)) / 3600
    df["trav_time"] = (df["trav_time"].apply(get_sec)) / 3600
    df["period"] = df["dep_time"].apply(get_time_period)
    df["traveled_distance"] = (df["traveled_distance"].map(int)) / 1000  # Down to km

    df["traveled_distance_bin"] = pd.cut(df["traveled_distance"] / 1000, distance_bins)
    df.loc[df["person"].str.contains("hgv"), "longest_distance_mode"] = "freight"
    df["longest_distance_mode"] = df["longest_distance_mode"].map(MODE_MAPPING)

    df["od_activities"] = df["start_activity_type"].str.split("_").str[0] + "-" + df["end_activity_type"].str.split("_").str[0]
    df['trip_purpose'] = df['od_activities'].apply(get_trip_purpose)

    return df


def main():
    logging.info("Start trips metrics calculation ...")
    out_df = pd.DataFrame()
    for scenario in SCENARIOS:
        logging.info(f"Calculate trip metrics for scenario {scenario}")
        if out_df.empty:
            out_df = calculate_trip_metrics(
                scenario_path=os.path.join(Path(os.path.abspath("")), scenario)
            ).rename(columns={"Value": scenario})
        else:
            out_df = out_df.merge(
                out_df,
                calculate_trip_metrics(
                    scenario_path=os.path.join(Path(os.path.abspath("")), scenario)
                ).rename(columns={"Value": scenario}),
                left_on=INDEX_COLS,
                right_on=INDEX_COLS,
            )

    logging.info("Finish trips metrics calculation...")
    out_df = out_df.astype(str)

    out_df.set_index(INDEX_COLS).to_csv("test.csv")


def run_trip_metrics(scenario) -> pd.DataFrame():
    logging.info(f"Calculate trip metrics for scenario {scenario}")

    out_df = calculate_trip_metrics(
        scenario_path=os.path.join(Path(os.path.abspath("")), scenario)
    ).rename(columns={"Value": scenario})

    logging.info("Finish trips metrics calculation...")
    return out_df

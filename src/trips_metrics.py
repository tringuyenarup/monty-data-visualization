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
        logging.info(f"Calculate trip base for {mode}...")
        for area in BOUNDARIES:
            all_regions = set(trip_df[f"start_{area}"]).union(
                set(trip_df[f"end_{area}"])
            )
            for activity in ["work", "education"]:
                for key, _ in PERIODS.items():
                    if key != "Daily":
                        for start_or_end in ["start", "end"]:
                            tmp_trip_df = trip_df[
                                (trip_df["longest_distance_mode"] == mode)
                                & (trip_df["period"] == key)
                                & (trip_df["end_activity_type"] == activity)
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

                            mode_split = (
                                tmp_trip_df.groupby([f"{start_or_end}_{area}"])
                                .agg({"trip_id": "count"})
                                .rename(columns={"trip_id": "Value"})
                            )

                            travel_distance = fill_regions(travel_distance, all_regions)
                            travel_time = fill_regions(travel_time, all_regions)
                            mode_split = fill_regions(mode_split, all_regions)

                            travel_distance = fill_missing_columns(
                                travel_distance,
                                f"Average trips distance ({start_or_end}) in {area}",
                                area,
                                mode,
                                key,
                                f"{activity}",
                            )
                            travel_time = fill_missing_columns(
                                travel_time,
                                f"Average trips time ({start_or_end}) in {area}",
                                area,
                                mode,
                                key,
                                f"{activity}",
                            )
                            mode_split = fill_missing_columns(
                                mode_split,
                                f"Mode by trips counts for trip ({start_or_end}) in {area}",
                                area,
                                mode,
                                key,
                                f"{activity}",
                            )
                            if tmp_df.empty:
                                tmp_df = pd.concat(
                                    [
                                        travel_distance[COLUMNS],
                                        travel_time[COLUMNS],
                                        mode_split[COLUMNS],
                                    ],
                                    ignore_index=True,
                                )
                            else:
                                tmp_df = pd.concat(
                                    [
                                        tmp_df,
                                        travel_distance[COLUMNS],
                                        travel_time[COLUMNS],
                                        mode_split[COLUMNS],
                                    ],
                                    ignore_index=True,
                                )

    return tmp_df


def read_trips(scenario_path) -> pd.DataFrame:
    df = pd.read_csv(os.path.join(scenario_path, "joined_trips.csv"), low_memory=False)
    distance_bins = range(0, 40, 1)

    df["dep_time"] = (df["dep_time"].apply(get_sec)) / 3600
    df["trav_time"] = (df["trav_time"].apply(get_sec)) / 3600
    df["period"] = df["dep_time"].apply(get_time_period)
    df["traveled_distance"] = (df["traveled_distance"].map(int)) / 1000  # Down to km

    df["traveled_distance_bin"] = pd.cut(df["traveled_distance"] / 1000, distance_bins)
    df.loc[df["person"].str.contains("hgv"), "longest_distance_mode"] = "freight"
    df["longest_distance_mode"] = df["longest_distance_mode"].map(MODE_MAPPING)

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

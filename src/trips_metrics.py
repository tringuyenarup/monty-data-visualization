import pandas as pd
import os
from pathlib import Path
import logging
import warnings

from .utils import *

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
    logging.info("Read trips....")
    trip_df = read_trips(scenario_path)

    groups = ['period', 'longest_distance_mode', 'trip_purpose', 'od', 'demo_type']
    trips_metrics_dfs = []

    for L in range(len(groups) + 1):
        for subset in itertools.combinations(groups, L):
            subset = [col for col in subset]
            logging.info(f"Calculate trip base for these subsets: {str(subset)}")
            if not subset:
                #break
                trips_metrics_dfs.append(
                    pd.DataFrame([[trip_df['trip_id'].count(), trip_df['trav_time'].mean(), trip_df['traveled_distance'].mean()]],
                                 columns = ['trip_id', 'trav_time', 'traveled_distance']))
            elif ('demo_type' in subset) and ('od' in subset):
                for demo_type in DEMOGRAPHIC_TYPE:
                    temp_subset_cols = subset.copy()
                    temp_subset_cols[temp_subset_cols.index('demo_type')] = demo_type
                    for od in ['start', 'end']:
                        for area_type in BOUNDARIES:
                            subset_cols = temp_subset_cols.copy()
                            subset_cols[subset_cols.index('od')] = od + '_' + area_type

                            trips_metrics_dfs.append(metrics_groupby(trip_df, subset_cols))

            elif ('demo_type' in subset):
                for demo_type in DEMOGRAPHIC_TYPE:
                    subset_cols = subset.copy()
                    subset_cols[subset_cols.index('demo_type')] = demo_type

                    trips_metrics_dfs.append(metrics_groupby(trip_df, subset_cols))

            elif ('od' in subset):
                for od in ['start', 'end']:
                    for area_type in BOUNDARIES:
                        subset_cols = subset.copy()
                        subset_cols[subset_cols.index('od')] = od + '_' + area_type

                        trips_metrics_dfs.append(metrics_groupby(trip_df, subset_cols))
                    
            else:
                subset_cols = subset.copy()
                trips_metrics_dfs.append(metrics_groupby(trip_df, subset_cols))
        
    trips_metrics_summary= pd.concat(trips_metrics_dfs, axis = 0, join='outer'
                                ).rename(columns={'trip_id': 'trip_count',
                                                'longest_distance_mode': 'mode',
                                                'trav_time': 'travel_time'})

    trips_metrics_summary = trips_metrics_summary.fillna("All")
    trips_metrics_summary['period'] = trips_metrics_summary['period'].fillna("Daily")
    trips_metrics_summary['Group'] = 'Trips'
    trips_index_cols = ['Group'] + [col for col in INDEX_COLS if col in trips_metrics_summary.columns]
    trips_metrics_summary = trips_metrics_summary.melt(id_vars=trips_index_cols, var_name="Metric", value_name="Value")

    return trips_metrics_summary


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

def metrics_groupby(trip_df, subset_cols):
    #logging.info(f"Grouping metrics for {str(subset_cols)}")
    trip_metrics_df = trip_df.groupby(subset_cols).agg(
        {'trip_id':'count', 'trav_time': 'mean', 'traveled_distance': 'mean'}).reset_index()
    
    demo_type = [i for i in subset_cols if i in DEMOGRAPHIC_TYPE]
    area_type = [i for i in subset_cols if any(j in i for j in BOUNDARIES)]

    if len(demo_type) > 0:
        trip_metrics_df['demographic_type'] = demo_type[0]
        trip_metrics_df = trip_metrics_df.rename(columns={demo_type[0]: 'demographic_group'})
    if len(area_type) > 0:
        trip_metrics_df['area_type'] = area_type[0].replace(area_type[0].split("_")[0]+"_", "")
        trip_metrics_df = trip_metrics_df.rename(columns={area_type[0]: area_type[0].split("_")[0]+"_area"})

    return trip_metrics_df

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


def run_trip_metrics(output_path, scenario) -> pd.DataFrame():
    logging.info(f"Calculate trip metrics for scenario {scenario}")

    out_df = calculate_trip_metrics(
        scenario_path=(output_path + scenario)
    ).rename(columns={"Value": scenario})

    logging.info("Finish trips metrics calculation...")
    return out_df

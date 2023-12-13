import pandas as pd
import os
from pathlib import Path
import logging
import warnings

warnings.filterwarnings("ignore")

from .utils import *


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)

def calculate_assignment_metrics(scenario_path) -> pd.DataFrame:
    tmp_assignment_dfs = []
    logging.info("Read network....")
    network_df = read_network(os.path.join(scenario_path, "link_table.csv"))

    # Calculate for PV
    for mode in PV_MODES:
        logging.info(f"Read volume data for {mode}")
        
        volume_df = read_volume(os.path.join(scenario_path, f"volumes_{mode}.csv"))
        time_df = read_volume(os.path.join(scenario_path, f"times_{mode}.csv"))

        logging.info(f"Calculate pv metrics for {mode}")
        tmp_pv_metrics = calculate_pv_metrics(network_df, volume_df, time_df, mode)

        tmp_assignment_dfs.append(tmp_pv_metrics)

    # Calculate for PT
    for mode in PT_MODES:
        logging.info(f"Read passenger data for {mode}")
        if mode == 'rail':
            if not os.path.exists(os.path.join(scenario_path, f"volumes_{mode}.csv")):
                mode = 'train'
        volume_df = read_volume(os.path.join(scenario_path, f"volumes_{mode}.csv"))
        time_df = read_volume(os.path.join(scenario_path, f"times_{mode}.csv"))
        passenger_df = read_volume(
            os.path.join(scenario_path, f"passengers_{mode}.csv")
        )

        logging.info(f"Calculate pt metrics for {mode}")
        tmp_pt_metrics = calculate_pt_metrics(
            network_df, volume_df, time_df, passenger_df, mode
        )

        tmp_assignment_dfs.append(tmp_pt_metrics)

    assignment_summary = pd.concat(tmp_assignment_dfs, ignore_index=True)
    assignment_summary['period'] = assignment_summary['period'].fillna('-')
    assignment_summary = pd.concat([assignment_summary, assignment_summary.groupby(['Group','Metric', 'area_type', 'mode','period']).agg({'Value':'sum'}).reset_index()])
    assignment_summary['area'] = assignment_summary['area'].fillna('All')

    return assignment_summary

def calculate_pt_metrics(network_df, volume_df, time_df, passenger_df, mode) -> pd.DataFrame:
    tmp_df = pd.DataFrame()
    df = network_df.join(volume_df, on="id", how="inner")
    df = df.join(time_df, on="id", rsuffix="_time")
    df = df.join(passenger_df, on="id", rsuffix="_passenger")
    pt_assignment_metrics_dfs = []

    # Add addtional columns
    for i in range(0, 24):
        # Vehicle travel information
        df[f"{i}_vkt"] = df[f"{i}"] * (df["length"] / 1000)
        df[f"{i}_vht"] = df[f"{i}"] * (df[f"{i}_time"] / 3600)
        # Passenger travel information
        df[f"{i}_pkt"] = df[f"{i}_passenger"] * (df["length"] / 1000)
        df[f"{i}_pht"] = df[f"{i}_passenger"] * (df[f"{i}_time"] / 3600)
        # Volume to ratio
    
    for area in BOUNDARIES:
        for period_key, period in PERIODS.items():
            for metric in PT_ASSIGNMENT_METRICS:
                pt_assignment_metrics_dfs.append(assignment_hourly_groupby(df, 'Assignment', metric, area, period_key, period, mode))

    pt_assignment_metrics_summary = pd.concat(pt_assignment_metrics_dfs)

    return pt_assignment_metrics_summary

def calculate_pv_metrics(network_df, volume_df, time_df, mode) -> pd.DataFrame:
    tmp_df = pd.DataFrame()

    df = network_df.join(volume_df, on="id", how="inner")
    df = df.join(time_df, on="id", rsuffix="_time")

    # Add addtional columns
    for i in range(0, 24):
        df[f"{i}_vkt"] = df[f"{i}"] * (df["length"] / 1000)
        df[f"{i}_vht"] = df[f"{i}"] * (df[f"{i}_time"] / 3600)
        df[f"{i}_vol_to_cap_ratio"] = df[f"{i}"] / df["capacity"]

    df["lane_km"] = df["length"] * df["lanes"]

    pv_assignment_metrics_dfs = []

    # Calculate VHT, VKT
    for area in BOUNDARIES:
        for metric in NETWORK_METRICS:
             pv_assignment_metrics_dfs.append(assignment_distance_groupby(df, 'Network', metric, area, mode))
        for period_key, period in PERIODS.items():
            for metric in PV_ASSIGNMENT_METRICS:
                pv_assignment_metrics_dfs.append(assignment_hourly_groupby(df, 'Assignment', metric, area, period_key, period, mode))

    pv_assignment_metrics_summary = pd.concat(pv_assignment_metrics_dfs)
    
    return pv_assignment_metrics_summary 

def read_network(network_path):
    return pd.read_csv(network_path, low_memory=False, index_col="id")


def read_volume(volume_path):
    return (
        pd.read_csv(volume_path, low_memory=False)
        .rename(columns={"link_id": "id"})
        .set_index("id")
    )

def assignment_hourly_groupby(df, group, metric, area, period_key, period, mode):
    tmp_df = df[[area, *[f"{i}_{metric}" for i in period]]].groupby([area])[[f"{i}_{metric}" for i in period]].apply(sum).sum(axis=1).reset_index().rename(columns={0: "Value"})
    tmp_df['period'] = period_key
    tmp_df['mode'] = mode
    tmp_df['area_type'] = area
    tmp_df['Group'] = group
    tmp_df['Metric'] = f"{ASSIGNMENT_METRICS[metric]}"

    tmp_df = tmp_df.rename(columns={area: 'area'})

    return tmp_df

def assignment_distance_groupby(df, group, metric, area, mode, division_factor = 1000):
    tmp_df = df.groupby([area]).agg({metric: sum}).rename(columns={metric: "Value"})/division_factor
    tmp_df = tmp_df.reset_index()
    tmp_df['mode'] = mode
    tmp_df['area_type'] = area
    tmp_df['Group'] = group
    tmp_df['Metric'] = f"{ASSIGNMENT_METRICS[metric]}"

    tmp_df = tmp_df.rename(columns={area: 'area'})

    return tmp_df


# final entry point
def calculate_all_metrics(scenario_path):
    out_df = pd.DataFrame()

    pv_metrics = calculate_pv_metrics(scenario_path)

    out_df = pd.concat([pv_metrics], ignore_index=True)

    return out_df


def run_assignment_metrics(output_path, scenario) -> pd.DataFrame:
    logging.info(f"Calculate assignment for scenario {scenario}")

    out_df = calculate_assignment_metrics(
        scenario_path=(output_path + scenario)
    ).rename(columns={"Value": scenario})

    logging.info("Finish assignment metrics calculation...")
    return out_df

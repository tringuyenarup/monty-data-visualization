import pandas as pd
import os
from pathlib import Path
import logging
import warnings

warnings.filterwarnings("ignore")

from utils import *


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)


def calculate_assignment_metrics(scenario_path) -> pd.DataFrame:
    tmp_assignment_df = pd.DataFrame()
    logging.info("Read network....")
    network_df = read_network(os.path.join(scenario_path, "link_table.csv"))

    # Calculate for PV
    for mode in PV_MODES:
        logging.info(f"Read volume data for {mode}")
        volume_df = read_volume(os.path.join(scenario_path, f"volumes_{mode}.csv"))
        time_df = read_volume(os.path.join(scenario_path, f"times_{mode}.csv"))

        logging.info(f"Calculate pv metrics for {mode}")
        tmp_pv_metrics = calculate_pv_metrics(network_df, volume_df, time_df, mode)

        if tmp_assignment_df.empty:
            tmp_assignment_df = tmp_pv_metrics
        else:
            tmp_assignment_df = pd.concat(
                [tmp_assignment_df, tmp_pv_metrics], ignore_index=True
            )

    # Calculate for PT
    for mode in PT_MODES:
        logging.info(f"Read passenger data for {mode}")
        volume_df = read_volume(os.path.join(scenario_path, f"volumes_{mode}.csv"))
        time_df = read_volume(os.path.join(scenario_path, f"times_{mode}.csv"))
        passenger_df = read_volume(
            os.path.join(scenario_path, f"passengers_{mode}.csv")
        )

        logging.info(f"Calculate pt metrics for {mode}")
        tmp_pt_metrics = calculate_pt_metrics(
            network_df, volume_df, time_df, passenger_df, mode
        )

        if tmp_assignment_df.empty:
            tmp_assignment_df = tmp_pt_metrics
        else:
            tmp_assignment_df = pd.concat(
                [tmp_assignment_df, tmp_pt_metrics], ignore_index=True
            )

    return tmp_assignment_df


def calculate_pt_metrics(
    network_df, volume_df, time_df, passenger_df, mode
) -> pd.DataFrame:
    tmp_df = pd.DataFrame()
    df = network_df.join(volume_df, on="id", how="inner")
    df = df.join(time_df, on="id", rsuffix="_time")
    df = df.join(passenger_df, on="id", rsuffix="_passenger")

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
        all_regions = set(network_df[area])
        for key, value in PERIODS.items():
            vkt = pd.DataFrame(
                df[[area, *[f"{i}_vkt" for i in value]]]
                .groupby([area], group_keys=False)[[f"{i}_vkt" for i in value]]
                .apply(sum)
                .sum(axis=1)
            ).rename(columns={0: "Value"})

            vht = pd.DataFrame(
                df[[area, *[f"{i}_vht" for i in value]]]
                .groupby([area], group_keys=False)[[f"{i}_vht" for i in value]]
                .apply(sum)
                .sum(axis=1)
            ).rename(columns={0: "Value"})

            pkt = pd.DataFrame(
                df[[area, *[f"{i}_pkt" for i in value]]]
                .groupby([area], group_keys=False)[[f"{i}_pkt" for i in value]]
                .apply(sum)
                .sum(axis=1)
            ).rename(columns={0: "Value"})

            pht = pd.DataFrame(
                df[[area, *[f"{i}_pht" for i in value]]]
                .groupby([area], group_keys=False)[[f"{i}_pht" for i in value]]
                .apply(sum)
                .sum(axis=1)
            ).rename(columns={0: "Value"})
            # Filling missing regions

            vkt = fill_regions(vkt, all_regions)
            vht = fill_regions(vht, all_regions)
            pkt = fill_regions(pkt, all_regions)
            pht = fill_regions(pht, all_regions)

            vkt = fill_missing_columns(
                vkt,
                f"Assignment ({mode})",
                area,
                mode,
                key,
                "Service Kilometers Travelled",
            )
            vht = fill_missing_columns(
                vht,
                f"Assignment ({mode})",
                area,
                mode,
                key,
                "Service Hours Travelled",
            )

            pkt = fill_missing_columns(
                pkt,
                f"Assignment ({mode})",
                area,
                mode,
                key,
                "Passengers Kilometers Travelled",
            )
            pht = fill_missing_columns(
                pht,
                f"Assignment ({mode})",
                area,
                mode,
                key,
                "Passengers Hours Travelled",
            )

            if tmp_df.empty:
                tmp_df = pd.concat([vkt, vht, pkt, pht], ignore_index=True)
            else:
                tmp_df = pd.concat([tmp_df, vkt, vht, pkt, pht], ignore_index=True)

    return tmp_df


def calculate_pv_metrics(network_df, volume_df, time_df, mode) -> pd.DataFrame:
    tmp_df = pd.DataFrame()

    df = network_df.join(volume_df, on="id", how="inner")
    df = df.join(time_df, on="id", rsuffix="_time")

    # Add addtional columns
    for i in range(0, 24):
        df[f"{i}_vkt"] = df[f"{i}"] * (df["length"] / 1000)
        df[f"{i}_vht"] = df[f"{i}"] * (df[f"{i}_time"] / 3600)
        df[f"{i}_vol_to_cap_ratio"] = df[f"{i}"] / df["capacity"]

    # Calculate VHT, VKT
    for area in BOUNDARIES:
        all_regions = set(network_df[area])
        for key, value in PERIODS.items():
            vkt = pd.DataFrame(
                df[[area, *[f"{i}_vkt" for i in value]]]
                .groupby([area], group_keys=False)[[f"{i}_vkt" for i in value]]
                .apply(sum)
                .sum(axis=1)
            ).rename(columns={0: "Value"})
            vht = pd.DataFrame(
                df[[area, *[f"{i}_vht" for i in value]]]
                .groupby([area], group_keys=False)[[f"{i}_vht" for i in value]]
                .apply(sum)
                .sum(axis=1)
            ).rename(columns={0: "Value"})

            # Fill all missing regions
            vkt = fill_regions(vkt, all_regions)
            vht = fill_regions(vht, all_regions)

            if mode == "freight":
                vkt = fill_missing_columns(
                    vkt,
                    "Assignment (Private Vehicle)",
                    area,
                    mode,
                    key,
                    "Freight Kilometers Travelled",
                )
                vht = fill_missing_columns(
                    vht,
                    "Assignment (Private Vehicle)",
                    area,
                    mode,
                    key,
                    "Freight Hours Travelled",
                )
            else:
                vkt = fill_missing_columns(
                    vkt,
                    "Assignment (Private Vehicle)",
                    area,
                    mode,
                    key,
                    "Vehicle Kilometers Travelled",
                )
                vht = fill_missing_columns(
                    vht,
                    "Assignment (Private Vehicle)",
                    area,
                    mode,
                    key,
                    "Vehicle Hours Travelled",
                )

            if tmp_df.empty:
                tmp_df = pd.concat([vkt, vht], ignore_index=True)
            else:
                tmp_df = pd.concat([tmp_df, vkt, vht], ignore_index=True)

    # Calculate road km and lane km
    df["lane_km"] = df["length"] * df["lanes"]
    for area in BOUNDARIES:
        all_regions = set(network_df[area])

        road_km_df_tmp = (
            df.groupby([area]).agg({"length": sum}).rename(columns={"length": "Value"})
        ) / 1000
        lane_km_df_tmp = (
            df.groupby([area])
            .agg({"lane_km": sum})
            .rename(columns={"lane_km": "Value"})
        ) / 1000

        # Fill missing regions
        road_km_df_tmp = fill_regions(road_km_df_tmp, all_regions)
        lane_km_df_tmp = fill_regions(lane_km_df_tmp, all_regions)

        road_km_df_tmp = fill_missing_columns(
            road_km_df_tmp,
            "Assignment (Private Vehicle)",
            area,
            None,
            None,
            "Road Kilometers",
        )
        lane_km_df_tmp = fill_missing_columns(
            lane_km_df_tmp,
            "Assignment (Private Vehicle)",
            area,
            None,
            None,
            "Lane Kilometers",
        )

        if tmp_df.empty:
            tmp_df = pd.concat([road_km_df_tmp, lane_km_df_tmp], ignore_index=True)
        else:
            tmp_df = pd.concat(
                [tmp_df, road_km_df_tmp, lane_km_df_tmp], ignore_index=True
            )

    # Calculate base on road cap
    for cap in ROAD_CAPS:
        for area in BOUNDARIES:
            all_regions = set(network_df[area])
            for key, value in PERIODS.items():
                vkt = pd.DataFrame(
                    df[[area, *[f"{i}_vkt" for i in value]]]
                    .groupby([area], group_keys=False)[[f"{i}_vkt" for i in value]]
                    .apply(sum)
                    .sum(axis=1)
                ).rename(columns={0: "Total VKT"})
                df[f"{key}_vol_to_cap_ratio_mean"] = df[
                    [f"{i}_vol_to_cap_ratio" for i in value]
                ].sum(axis=1) / len(value)

                vkt[f"VKT_{cap}"] = (
                    df[df[f"{key}_vol_to_cap_ratio_mean"] > cap][
                        [area, *[f"{i}_vkt" for i in value]]
                    ]
                    .groupby([area], group_keys=False)[[f"{i}_vkt" for i in value]]
                    .apply(sum)
                    .sum(axis=1)
                )
                vkt["Value"] = vkt[f"VKT_{cap}"] / vkt["Total VKT"]

                vht = pd.DataFrame(
                    df[[area, *[f"{i}_vht" for i in value]]]
                    .groupby([area], group_keys=False)[[f"{i}_vht" for i in value]]
                    .apply(sum)
                    .sum(axis=1)
                ).rename(columns={0: "Total VHT"})

                vht[f"VHT_{cap}"] = (
                    df[df[f"{key}_vol_to_cap_ratio_mean"] > cap][
                        [area, *[f"{i}_vht" for i in value]]
                    ]
                    .groupby([area], group_keys=False)[[f"{i}_vht" for i in value]]
                    .apply(sum)
                    .sum(axis=1)
                )
                vht["Value"] = vht[f"VHT_{cap}"] / vht["Total VHT"]

                # Road kilometer
                road_km_df_tmp = (
                    df.groupby([area])
                    .agg({"length": sum})
                    .rename(columns={"length": "Total length"})
                )
                road_km_df_tmp[f"Length_{cap}"] = (
                    df[df[f"{key}_vol_to_cap_ratio_mean"] > cap]
                    .groupby([area])
                    .agg({"length": sum})
                )

                road_km_df_tmp["Value"] = (
                    road_km_df_tmp[f"Length_{cap}"] / road_km_df_tmp["Total length"]
                )
                # filling missing regions
                vkt = fill_regions(vkt, all_regions)
                vht = fill_regions(vht, all_regions)
                road_km_df_tmp = fill_regions(road_km_df_tmp, all_regions)

                road_km_df_tmp = fill_missing_columns(
                    road_km_df_tmp,
                    "Assignment (Private Vehicle)",
                    area,
                    mode,
                    key,
                    f"% of Road Km. (> {cap*100}% cap).",
                )

                if mode == "freight":
                    vkt = fill_missing_columns(
                        vkt,
                        "Assignment (Private Vehicle)",
                        area,
                        mode,
                        key,
                        f"% of congested Freight Kilometers Travelled (> {cap*100}% cap).",
                    )
                    vht = fill_missing_columns(
                        vht,
                        "Assignment (Private Vehicle)",
                        area,
                        mode,
                        key,
                        f"% of congested Freight Hours Travelled (> {cap*100}% cap).",
                    )
                else:
                    vkt = fill_missing_columns(
                        vkt,
                        "Assignment (Private Vehicle)",
                        area,
                        mode,
                        key,
                        f"% of Vehicles Kilometers Travelled (> {cap*100}% cap).",
                    )
                    vht = fill_missing_columns(
                        vht,
                        "Assignment (Private Vehicle)",
                        area,
                        mode,
                        key,
                        f"% of Vehicles Hours Travelled (> {cap*100}% cap).",
                    )
                if tmp_df.empty:
                    tmp_df = pd.concat(
                        [vkt[COLUMNS], vht[COLUMNS], road_km_df_tmp[COLUMNS]],
                        ignore_index=True,
                    )
                else:
                    tmp_df = pd.concat(
                        [tmp_df, vkt[COLUMNS], vht[COLUMNS], road_km_df_tmp[COLUMNS]],
                        ignore_index=True,
                    )

    return tmp_df


def read_network(network_path):
    return pd.read_csv(network_path, low_memory=False, index_col="id")


def read_volume(volume_path):
    return (
        pd.read_csv(volume_path, low_memory=False)
        .rename(columns={"link_id": "id"})
        .set_index("id")
    )


# final entry point
def calculate_all_metrics(scenario_path):
    out_df = pd.DataFrame()

    pv_metrics = calculate_pv_metrics(scenario_path)

    out_df = pd.concat([pv_metrics], ignore_index=True)

    return out_df


def run_assignment_metrics(scenario) -> pd.DataFrame:
    logging.info(f"Calculate assignment for scenario {scenario}")

    out_df = calculate_assignment_metrics(
        scenario_path=os.path.join(Path(os.path.abspath("")), scenario)
    ).rename(columns={"Value": scenario})

    logging.info("Finish assignment metrics calculation...")
    return out_df

import logging
import os
import pandas as pd
import geopandas as gpd

from pathlib import Path
from .utils import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)


def calculate_demographics_metrics(scenario_path) -> pd.DataFrame:
    out_df = pd.DataFrame()

    logging.info("Load demographics data...")
    persons_df = pd.read_csv(
        os.path.join(scenario_path, "synthetic_persons.csv"), low_memory=False
    )
    persons_df["regional_council_simplified"] = (
        persons_df["regional_council_simplified"].map(int).map(REGION_MAPPING)
    )
    persons_df["person_id"] = range(1, len(persons_df) + 1)

    households_df = pd.read_csv(
        os.path.join(scenario_path, "synthetic_households.csv"), low_memory=False
    )
    households_df["regional_council_simplified"] = (
        households_df["regional_council_simplified"].map(int).map(REGION_MAPPING)
    )

    persons_df = persons_df.rename(
        columns={"regional_council_simplified": "regional_council"}
    )
    households_df = households_df.rename(
        columns={"regional_council_simplified": "regional_council"}
    )

    logging.info("Loading geo map....")
    geo_sa2 = gpd.read_file(
        os.path.join(
            Path(os.path.abspath("")),
            "geo/sa2-2018/statistical-area-2-2018-generalised.geojson",
        )
    )

    geo_sa2["SA22018_V1_00"] = geo_sa2["SA22018_V1_00"].astype(int)

    persons_df["sa2"] = (
        persons_df["sa2"]
        .map(int)
        .map(dict(zip(geo_sa2["SA22018_V1_00"], geo_sa2["SA22018_V1_NAME"])))
    )
    households_df["sa2"] = (
        households_df["sa2"]
        .map(int)
        .map(dict(zip(geo_sa2["SA22018_V1_00"], geo_sa2["SA22018_V1_NAME"])))
    )
    # for area in BOUNDARIES:
    #     persons_df[area] = persons_df[area].map(str)
    #     households_df[area] = households_df[area].map(str)

    """
    Calculate different metrics:
    - population
    - Households
    - Full time student
    - Unistudent
    - Employment
    """
    for area in BOUNDARIES:
        all_regions = set(persons_df[area])
        population = (
            persons_df.groupby([area])
            .agg({"person_id": "count"})
            .rename(columns={"person_id": "Value"})
        )
        household = (
            households_df.groupby([area])
            .agg({"household_id": "count"})
            .rename(columns={"household_id": "Value"})
        )

        full_time_enrolment = (
            persons_df[persons_df["student_status"] == "full_time_school"]
            .groupby([area])
            .agg({"person_id": "count"})
            .rename(columns={"person_id": "Value"})
        )
        uni_enrolment = (
            persons_df[
                persons_df["student_status"].isin(
                    ["full_time_tertiary", "part_time_tertiary"]
                )
            ]
            .groupby([area])
            .agg({"person_id": "count"})
            .rename(columns={"person_id": "Value"})
        )
        employment = (
            persons_df[
                persons_df["labour_force_status"].isin(["full_time", "part_time"])
            ]
            .groupby([area])
            .agg({"person_id": "count"})
            .rename(columns={"person_id": "Value"})
        )

        population = fill_regions(population, all_regions)
        household = fill_regions(household, all_regions)

        full_time_enrolment = fill_regions(full_time_enrolment, all_regions)
        uni_enrolment = fill_regions(uni_enrolment, all_regions)
        employment = fill_regions(employment, all_regions)

        # income_high = (
        #     households_df[households_df["income_band"] == "high"]
        #     .groupby([area])
        #     .agg({"household_id": "count"})
        #     .rename(columns={"household_id": "Value"})
        # )
        # income_medium = (
        #     households_df[households_df["income_band"] == "medium"]
        #     .groupby([area])
        #     .agg({"household_id": "count"})
        #     .rename(columns={"household_id": "Value"})
        # )
        # income_low = (
        #     households_df[households_df["income_band"] == "low"]
        #     .groupby([area])
        #     .agg({"household_id": "count"})
        #     .rename(columns={"household_id": "Value"})
        # )
        #   df, group, area, mode: None, period: None, metric
        population = fill_missing_columns(
            population, "Demographics", area, None, None, None, None, None, "Population"
        )
        household = fill_missing_columns(
            household, "Demographics", area, None, None, None, None, None, "Households"
        )
        full_time_enrolment = fill_missing_columns(
            full_time_enrolment, "Demographics", area, None, None, None, None, None, "School Enrolments"
        )
        uni_enrolment = fill_missing_columns(
            uni_enrolment, "Demographics", area, None, None, None, None, None, "University Enrolments"
        )
        employment = fill_missing_columns(
            employment, "Demographics", area, None, None, None, None, None, "Employments"
        )
        # income_high = fill_missing_columns(
        #     income_high, "Demographics", area, None, None, None, None, None,  "Income (High)"
        # )
        # income_medium = fill_missing_columns(
        #     income_medium, "Demographics", area, None, None, None, None, None,  "Income (Medium)"
        # )
        # income_low = fill_missing_columns(
        #     income_low, "Demographics", area, None, None, None, None, None, "Income (Low)"
        # )

        if out_df.empty:
            out_df = pd.concat(
                [
                    population,
                    household,
                    full_time_enrolment,
                    uni_enrolment,
                    employment,
                    # income_high,
                    # income_low,
                    # income_medium,
                ],
                ignore_index=True,
            )
        else:
            out_df = pd.concat(
                [
                    out_df,
                    population,
                    household,
                    full_time_enrolment,
                    uni_enrolment,
                    employment,
                    # income_high,
                    # income_low,
                    # income_medium,
                ],
                ignore_index=True,
            )

    return out_df


def run_demographics_metrics(scenario) -> pd.DataFrame:
    logging.info(f"Calculate demographics for scenario {scenario}")
    out_df = calculate_demographics_metrics(
        scenario_path=os.path.join(Path(os.path.abspath("")), scenario)
    ).rename(columns={"Value": scenario})

    return out_df

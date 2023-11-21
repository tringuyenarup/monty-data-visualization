import pandas as pd
import logging
from utils import *
import click

from assignment_metrics import run_assignment_metrics
from trips_metrics import run_trip_metrics
from demographics_metrics import run_demographics_metrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)


# @click.command()
# @click.option("--output_filename", "-n", required=False, default=r"test.csv")
def run(output_filename: str):
    out_df = pd.DataFrame()
    for scenario in SCENARIOS:
        assignment_metrics = run_assignment_metrics(scenario)
        trip_metrics = run_trip_metrics(scenario)
        demographics_metrics = run_demographics_metrics(scenario)

        df = pd.concat(
            [assignment_metrics, trip_metrics, demographics_metrics], ignore_index=True
        )
        df = df.set_index(INDEX_COLS)

        if out_df.empty:
            out_df = df
        else:
            out_df = out_df.merge(df, left_index=True, right_index=True)

    logging.info("Write csv output....")
    out_df.to_csv(output_filename)
    out_df.reset_index(inplace=True)
    for col in INDEX_COLS:
        out_df[col] = out_df[col].map(str)

    logging.info("Write parquet output....")
    out_df.set_index(INDEX_COLS).to_parquet("test.parquet")


if __name__ == "__main__":
    run("test.csv")

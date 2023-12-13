import pandas as pd
import logging
from .utils import *
import click
from functools import reduce

from .assignment_metrics import run_assignment_metrics
from .trips_metrics import run_trip_metrics
from .demographics_metrics import run_demographics_metrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)

def merge_scenarios(dfs):
    on_cols = rearrange_columns(dfs[0].columns)
    return reduce(lambda left,right: pd.merge(left,right,
                                              on = on_cols,
                                              how='outer'), dfs)

def rearrange_columns(columns):
    return ['Group'] + [col for col in INDEX_COLS if col in columns] + ['Metric']

# @click.command()
# @click.option("--output_filename", "-n", required=False, default=r"test.csv")
def run(output_path: str, output_filename: str, assignment: bool =True, trips: bool =True, demographics: bool = False):
    out_df = pd.DataFrame()

    metrics = []

    #ASSIGNMENT METRICS
    if assignment:
        assignment_dfs = []
        for scenario in SCENARIOS:
            assignment_dfs.append(run_assignment_metrics(output_path, scenario))
        out_assignment_df = merge_scenarios(assignment_dfs)

        logging.info("Write assignment metrics csv output....")
        index_cols = rearrange_columns(out_assignment_df.columns)
        val_cols = list(set(out_assignment_df.columns) - set(index_cols))
        out_assignment_df[index_cols+val_cols].to_csv(output_path + output_filename + "_assignment.csv", index=None)
        metrics.append(out_assignment_df[index_cols+val_cols])

    if trips:
        trips_dfs = []
        for scenario in SCENARIOS:
            trips_dfs.append(run_trip_metrics(output_path, scenario))
        out_trip_df = merge_scenarios(trips_dfs)

        logging.info("Write trips metrics csv output....")
        index_cols = rearrange_columns(out_trip_df.columns)
        val_cols = list(set(out_trip_df.columns) - set(index_cols))
        out_trip_df[index_cols+val_cols].to_csv(output_path + output_filename + "_trips.csv", index=None)
        metrics.append(out_trip_df[index_cols+val_cols])

    if demographics:
        demographics_dfs = []
        for scenario in SCENARIOS:
            demographics_dfs.append(run_demographics_metrics(output_path, scenario))
        out_demographics_df = merge_scenarios(trips_dfs)
        out_demographics_df.to_csv(output_path + output_filename + "_demographics.csv", index=None)
        metrics.append(out_demographics_df)

    out_df = pd.concat(metrics, ignore_index=True)
        #df = df.set_index(INDEX_COLS)

    logging.info("Write csv output....")
    #out_df.to_csv(output_filename + ".csv", index=None)
    #out_df.reset_index(inplace=True)
    #for col in INDEX_COLS:
    #    out_df[col] = out_df[col].map(str)

    logging.info("Write parquet output....")
    out_df.to_parquet(output_path+ output_filename + ".parquet")
    #out_df.set_index(INDEX_COLS).to_parquet(output_filename + ".parquet")

def main():
    output_path = '/Users/tszchun.chow/Documents/GitHub/monty-sim-analysis/pt_variations/analysis'
    if output_path[-1] != "/":
        output_path += '/'
    output_filename = 'metrics_tab'
    run(output_path, output_filename, assignment = True, trips=False)

if __name__ == "__main__":
    main()
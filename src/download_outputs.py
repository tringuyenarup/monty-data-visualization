import os
import pandas as pd
import subprocess
import configparser
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)

def download_simulation_results(output_dir, scenario_group, scenario):
    print(f"aws s3 cp --recursive s3://mot-non-production-simulations/applications/{scenario_group}/outputs_{scenario}/")
    os.system(f"aws s3 cp --recursive s3://mot-non-production-simulations/applications/{scenario_group}/outputs_{scenario}/  {output_dir} --exclude 'ITERS/*'")

def main():
    logging.info("Downloading raw simulation outputs from S3")
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    paths = config['paths']
    variables = config['variables']

    scenario_group = variables['scenario_group']
    results_dir_base = paths['raw_output_path']
    scenarios = [x.lstrip() for x in variables['scenarios'].split(',')]

    for scenario in scenarios:
        results_dir = results_dir_base + "/outputs_" + scenario
        if (not os.path.isdir(results_dir)) or (not any(os.scandir(results_dir))):
            print("Donwloading simulation results into " + results_dir) 
            download_simulation_results(results_dir, scenario_group, scenario)

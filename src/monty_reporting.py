import os
import pandas as pd
import subprocess
import configparser
import logging
import sys
import shutil

def monty_reporting_events(config_path, events_path, output_dir, sample_size = 0.1):
    """Process an events XML file into individual CSVs
    USAGE:
        monty-reporting events [OPTIONS] --config-path <CONFIG_PATH> --events-path <EVENTS_PATH> --sample-size <SAMPLE_SIZE> --output-dir <OUTPUT_DIR>
    OPTIONS:
        -c, --config-path <CONFIG_PATH>    Path to configuration TOML
        -e, --events-path <EVENTS_PATH>    Path to the events XML file to be processed
        -h, --help                         Print help information
        -o, --output-dir <OUTPUT_DIR>      Path to output directory for generated results
        -q, --quiet                        Suppress output to stdout
        -s, --sample-size <SAMPLE_SIZE>    Scenario sample size"""
    
    os.system(f"monty-reporting events -c {config_path} -e {events_path} -o {output_dir} -s {sample_size}")

def monty_reporting_network(network_path, output_path, region_join: list = None):
    """USAGE:
    monty-reporting network [OPTIONS] --network <NETWORK> --out <OUT>
    OPTIONS:
        -h, --help
                Print help information
        -j, --join [<JOIN>...]
                Files or glob patterns for region systems to override defaults
        -n, --network <NETWORK>
                Path to network XML file
        -o, --out <OUT>
                Path to generated output"""
    
    if region_join is not None:
        join = " --join"
        for file in region_join:
            join += (" " + file)

    os.system(f"monty-reporting network -n {network_path} -o {output_path}" + join)

def monty_reporting_trips(trips_path, output_path, region_join: list = None):
    """Add region columns to a trips CSV.
    A set of defaults are built into the tool but can be overried with the `--region_systems` flag.
    USAGE:
        monty-reporting trips [OPTIONS] --trips <TRIPS> --out <OUT>
    OPTIONS:
        -h, --help
                Print help information
        -j, --join <JOIN>...
                Files or glob patterns for region systems to override defaults
        -o, --out <OUT>
                Path to generated output
        -t, --trips <TRIPS>
                Path to trips CSV file"""
    
    if region_join is not None:
        join = " --join"
        for file in region_join:
            join += (" " + file)
    else:
        join = ""

    os.system(f"monty-reporting trips --trips {trips_path} --out {output_path}" + join)

def monty_reporting_to_s3(report_path, scenario_group, scenario):
    s3_uri = f"s3://mot-non-production-simulations/applications/{scenario_group}/analysis/{scenario}/"
    os.system(f"aws s3 cp {report_path} {s3_uri} --recursive")

def main():
    logging.info("Generate monty reporting outputs.")
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    paths = config['paths']
    variables = config['variables']

    cur_path = os.path.dirname(os.path.abspath(__file__))
    #print(cur_path)
    default_path = os.path.join(cur_path, 'default')
    scenario_group = variables['scenario_group']
    results_dir_base = paths['raw_output_path']
    analysis_dir_base = paths['report_path']
    if not os.path.isdir(analysis_dir_base):
        os.mkdir(analysis_dir_base)
    scenarios = [x.lstrip() for x in variables['scenarios'].split(',')]

    population_dir_base = paths['population_path']

    region_dir = paths['region_path']
    if not region_dir.endswith('/'):
        region_dir += '/'

    events_xml = 'output_events.xml.gz'
    config_toml = 'events_config.toml'
    network_xml = 'output_network.xml.gz'
    trips_csv = 'output_trips.csv.gz'
    households_csv = 'synthetic_households.csv'
    persons_csv = 'synthetic_persons.csv'

    default_config = os.path.join(default_path, config_toml)
    config_path = analysis_dir_base + "/" + config_toml
    if not os.path.isfile(config_path):
        logging.info(config_path + " not found, copying default events_config.toml")
        shutil.copy(default_config, config_path)

    for scenario in scenarios:
        results_dir = results_dir_base + "/outputs_" + scenario
        events_path = os.path.join(results_dir, events_xml)
        analysis_dir = analysis_dir_base + "/" + scenario

        if not os.path.isdir(analysis_dir):
            os.mkdir(analysis_dir)
        
        for file in [households_csv, persons_csv]:
            filepath = os.path.join(analysis_dir, file)
            if not os.path.isfile(filepath):
                logging.info(filepath + "not found, copying default " + file + " into " + analysis_dir)
                default_file = os.path.join(default_path, file)
                shutil.copy(default_file, filepath)

        regc = region_dir + 'regional_council.geojson'
        sa2 = region_dir + 'sa2.geojson'

        network_path = os.path.join(results_dir, network_xml)
        network_output = os.path.join(analysis_dir , "link_table.csv")
        trips_path = os.path.join(results_dir, trips_csv)
        trips_output = os.path.join(analysis_dir, "joined_trips.csv")

        monty_reporting_events(config_path, events_path, analysis_dir, sample_size = 0.1)
        monty_reporting_network(network_path, network_output, region_join = [regc, sa2])
        monty_reporting_trips(trips_path, trips_output, region_join = [regc, sa2])

        report_to_s3 = variables['report_to_s3']
        if report_to_s3:
            monty_reporting_to_s3(analysis_dir, scenario_group, scenario)


            
        

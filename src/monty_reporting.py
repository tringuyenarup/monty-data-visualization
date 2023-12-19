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

def main():
    logging.info("Generate monty reporting outputs.")
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    paths = config['paths']
    variables = config['variables']

    cur_path = os.path.dirname(os.path.abspath(__file__))
    default_path = os.path.join(cur_path, 'default')
    scenario_group = variables['scenario_group']
    results_dir_base = paths['raw_output_path']
    analysis_dir_base = paths['report_path']
    scenarios = [x.lstrip() for x in variables['scenarios'].split(',')]

    region_dir = paths['region_path']
    if not region_dir.endswith('/'):
        region_dir += '/'

    events_xml = 'output_events.xml.gz'
    config_toml = 'events_config.toml'
    network_xml = 'output_network.xml.gz'
    trips_csv = 'output_trips.csv.gz'

    for scenario in scenarios:
        results_dir = results_dir_base + "/outputs_" + scenario + "/"
        events_path = results_dir + events_xml
        analysis_dir = analysis_dir_base + "/" + scenario + "/"
        config_path = analysis_dir_base + "/" + config_toml
        if not os.path.isfile(config_path):
            logging.info("Events config not found, copying default events_config.toml")
            default_config = os.path.join(default_path, 'events_config.toml')
            shutil.copy(default_config, config_path)

        regc = region_dir + 'regional_council.geojson'
        sa2 = region_dir + 'sa2.geojson'

        network_path = results_dir + network_xml
        network_output = analysis_dir + "link_table.csv"
        trips_path = results_dir + trips_csv
        trips_output = analysis_dir + "joined_trips.csv"

        monty_reporting_events(config_path, events_path, analysis_dir, sample_size = 0.1)
        monty_reporting_network(network_path, network_output, region_join = [regc, sa2])
        monty_reporting_trips(trips_path, trips_output, region_join = [regc, sa2])

        households_file = analysis_dir + 'synthetic_households.csv'
        persons_file = analysis_dir + 'synthetic_persons.csv'
        default_households = os.path.join(default_path, 'synthetic_households.csv')
        default_persons = os.path.join(default_path, 'synthetic_households.csv')
        if not os.path.isfile(households_file):
            logging.info("[temporary measure only] Households csv not found in scenario report. Copying default file")
            shutil.copy(default_households, households_file)
        if not os.path.isfile(persons_file):
            logging.info("[temporary measure only] Persons csv not found in scenario report. Copying default file")
            shutil.copy(default_persons, persons_file)
            
        

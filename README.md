# Monty Data Visualization

## Introduction

The `monty-data-visualization` tool uses `monty-reporting` outputs (a set of data that has manipulated raw Monty simulation outputs into a summary of trips, network, and network assignments) to generate a comprehensive, standardized tabulation of metrics for easy visualization in the simulation output -> analysis pipeline. 
Currently, there are options to run the entire pipeline from downloading raw simulation outputs from S3 all the way to producing the metrics tabulation, but you have to make sure [`monty-reporting`](https://gitlab.com/mot-analytics/monty/analysis/monty-reporting) is installed in your computer. See under "Usage" in this README for more detail.

## Installation

Dependency installation: 
```
poetry install
```
Activate virtual environment to access dependencies to run the tool:
```
poetry shell
```
## Usage

(1) Download raw simulation outputs from S3:
```
poetry run download_outputs config.ini
```
(2) Run `monty-reporting` to - make sure you have the [tool installed](https://gitlab.com/mot-analytics/monty/analysis/monty-reporting):
```
poetry run monty_reporting config.ini
```
(3) Tabulate metrics by running the following:
```
poetry run tabulate_metrics config.ini
```
### Config
Key inputs of the tool can be edited in the config.ini file. The inputs include:
- `region_path`: Where region council and SA2 GIS files are located on your local drive
- `raw_output_path`: Raw output path (where raw simulation outputs from S3 are downloaded to on your local drive)
- `report_path`: Report path (where monty-reporting outputs are located on your local drive)
- `tabulation_filename`: File name of the tabulation (do not add in file format suffix)

- `scenario_group`: Scenario folder name where outputs are located on S3)
- `scenarios`: List of scenario names (delimited by comma) - please make sure they are located as separate folders with the report path
- `assignment`, `trips`, `demographics`: Whether you would like to include assignment-related, trips-related, and/or demographics-related metrics -> these are booleans

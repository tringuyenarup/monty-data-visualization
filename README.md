# Monty Data Visualization

## Introduction

The `monty-data-visualization` tool uses `monty-reporting` outputs (a set of data that has manipulated raw Monty simulation outputs into a summary of trips, network, and network assignments) to generate a comprehensive, standardized tabulation of metrics for easy visualization in the simulation output -> analysis pipeline. 

## Installation and Usage

Dependency installation: 
```
poetry install
```
Activate virtual environment to access dependencies to run the tool:
```
poetry shell
```
Run the tool by running the following:
```
poetry run tabulate_metrics config.ini
```
### Config
Key inputs of the tool can be edited in the config.ini file. The inputs include:
- Report path (where monty-reporting outputs are located)
- File name of the tabulation (do not add in file format suffix)
- List of scenario names (delimited by comma) - please make sure they are located as separate folders with the report path
- Whether you would like to include assignment-related, trips-related, and/or demographics-related metrics -> these are booleans

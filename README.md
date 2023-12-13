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
[Will go into more detail about how to change config]

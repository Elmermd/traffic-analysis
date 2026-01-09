# Traffic Analysis Project

This repository contains an end-to-end data engineering pipeline for traffic analysis.

## Data directories

The `data/` directory follows a layered structure commonly used in data engineering projects:

- `data/raw/`  
  This directory is intentionally empty. It only contains the folder structure of the project architecture.
  Raw data is expected to be generated dynamically by the extraction scripts when the pipeline is executed.  
  Data in this layer comes directly from external APIs and public data sources and is not committed to the repository.

- `data/processed/`  
  Contains cleaned and standardized datasets generated from the raw layer.

- `data/curated/`  
  Contains fully integrated, analytics-ready datasets used for reporting and visualization.

The full pipeline, including data extraction, transformation, and integration, can be executed using the provided Python scripts or orchestrated via Apache Airflow.

## Data Generation Notes

The `data/raw/` directories are intentionally empty in this repository.

Raw datasets are **not versioned** or uploaded to GitHub due to their size and because they can be **fully reproduced** by executing the data extraction scripts included in this project. These scripts are responsible for retrieving the original data directly from the corresponding public APIs and data sources.

To populate the `data/raw/` layer, run the extraction stage of the pipeline, either:
- by executing the extraction scripts manually, or
- by triggering the full pipeline using the Airflow DAG.

Once executed, the extraction scripts will automatically download the data and store it in the appropriate `data/raw/` subdirectories, preserving the expected folder structure for downstream processing.

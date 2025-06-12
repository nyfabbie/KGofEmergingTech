# TEMPORARY CODE: Theses scripts is used to download startups from a crunchbase screenshot


## Alternative 1: YCOMBINATOR DATASETS FALL 2024 AND SPRING 2024
# import kagglehub

# # Download latest version
# yc-f2024-path = kagglehub.dataset_download("rummagelabs/y-combinator-yc-fall-2024-batch-companies")
# yc-s2024-path = kagglehub.dataset_download("rummagelabs/yc-s2024-batch")

# print("Path to dataset files:", yc-f2024-path)
# print("Path to dataset files:", yc-s2024-path)


## Alternative 2: YCOMBINATOR DATASETS FROM 2005 TO 2024
import kagglehub
import os
from kagglehub import KaggleDatasetAdapter
import pandas as pd


def fetch_crunchbase(tech_names):
    """
    Fetches startup investment data from Crunchbase.

    Parameters:
        tech_names (list of str): List of emerging technology names.

    Returns:
        pd.DataFrame: DataFrame with startup investment data.
    """
    # Load the latest version
    df = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "supremesun/complete-ycombinator-dataset-from-2005-2024",
    "yc_companies.csv",
    pandas_kwargs={"usecols": ["active_founders", "founded", "industry", "long_description", "name", "region", "short_description", "website", "tags"]}
    )

    print("First 5 records:", df.head())

    return df


## Alternative 3: CRUNCHBASE 2014 Snapshot
# import kagglehub
# from kagglehub import KaggleDatasetAdapter

# # Set the path to the file you'd like to load
# file_path = ""

# # Load the latest version
# df = kagglehub.load_dataset(
#   KaggleDatasetAdapter.PANDAS,
#   "arindam235/startup-investments-crunchbase",
#   file_path,
#   # Provide any additional arguments like 
#   # sql_query or pandas_kwargs. See the 
#   # documenation for more information:
#   # https://github.com/Kaggle/kagglehub/blob/main/README.md#kaggledatasetadapterpandas
# )

# print("First 5 records:", df.head())
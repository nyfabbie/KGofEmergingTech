# TEMPORARY CODE: Theses scripts are used to download startups from a stratup database screenshot

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
    # Alternative 1 - YCOMBINATOR DATASETS FROM 2005 TO 2024: 10.00 usability score
    df = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "supremesun/complete-ycombinator-dataset-from-2005-2024",
    "yc_companies.csv",
    pandas_kwargs={"usecols": ["active_founders", "founded", "industry", "long_description", "name", "region", "short_description", "website", "tags"]}
    )

    # Alternative 2 - CRUNCHBASE 2014 Snapshot: 8.82 usability score
    # df = kagglehub.load_dataset(
    # KaggleDatasetAdapter.PANDAS,
    # "arindam235/startup-investments-crunchbase",
    # "investments_VC.csv"
    # )

    print("First 5 records of startup dataset:", df.head())
    return df



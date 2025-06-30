# TEMPORARY CODE: Theses scripts are used to download startups from a stratup database screenshot

import kagglehub
import os
from kagglehub import KaggleDatasetAdapter
import pandas as pd

def fetch_crunchbase():
    """
    Fetches startup investment data from Crunchbase.

    Parameters:
        tech_names (list of str): List of emerging technology names.

    Returns:
        pd.DataFrame: DataFrame with startup investment data.
    """
    # Step 1 - YCOMBINATOR DATASETS FROM 2005 TO 2024: 10.00 usability score
    yc_df = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "supremesun/complete-ycombinator-dataset-from-2005-2024",
    "yc_companies.csv",
    pandas_kwargs={"usecols": ["active_founders", "founded", "industry", "long_description", "name", "region", "short_description", "website", "tags"]}
    )
    

    # Step 2, enrichment - CRUNCHBASE 2014 Snapshot: 8.82 usability score
    crunchbase_df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "arindam235/startup-investments-crunchbase",
        "investments_VC.csv",
        pandas_kwargs={"encoding": "ISO-8859-1"}
    )

    yc_df.columns = yc_df.columns.str.strip() 
    crunchbase_df.columns = crunchbase_df.columns.str.strip()


    # Fetch the third dataset from GitHub
    github_url = "https://raw.githubusercontent.com/luminati-io/Crunchbase-dataset-samples/main/crunchbase-companies-information.csv"
    brightapi_df = pd.read_csv(github_url, low_memory=False)


    print("First 5 records of YCOMBINATOR dataset:", yc_df.head())
    print("First 5 records of Crunchbase dataset:", crunchbase_df.head())
    return yc_df, crunchbase_df, brightapi_df



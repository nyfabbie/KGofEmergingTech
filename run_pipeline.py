# Top level orchestration script for running the pipeline


import pandas as pd
import os
import time
from neo4j import GraphDatabase

from src.get_arxiv import fetch_arxiv, parse_et
from src.get_crunchbase import fetch_crunchbase
from src.get_wikidata import fetch_wikidata
from src.clean_data import match_papers_to_tech, match_startups_to_techs, clean_arxiv
from src.load_to_neo4j import load_graph

wikidata_csv_path = "data/wikidata_techs_res.csv"
crunchbase_csv_path = "data/crunchbase_startups_res.csv"
yc_csv_path = "data/ycombinator_startups_res.csv"
arxiv_csv_path = "data/arxiv_papers_res.csv"

tech_startup_csv_path = "data/matches_tech_startup.csv"
tech_paper_csv_path = "data/matches_tech_paper.csv"
emerging_technologies_file = "data/emerging_techs.csv"

def wait_for_neo4j(uri, user, pwd, max_retries=10, delay=2):
    for i in range(max_retries):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, pwd))
            with driver.session() as session:
                session.run("RETURN 1")
            print("✓ Neo4j is ready!")
            return
        except Exception as e:
            print(f"Waiting for Neo4j to be ready... ({i+1}/{max_retries})")
            time.sleep(delay)
    raise RuntimeError("Neo4j did not become ready in time.")


# Check all cached data files exist or fail with error
def check_cache_files():
    required_files = [
        wikidata_csv_path,
        crunchbase_csv_path,
        yc_csv_path,
        arxiv_csv_path,
        tech_startup_csv_path
    ]
    
    for file in required_files:
        if not os.path.exists(file):
            raise FileNotFoundError(f"Required cache file '{file}' does not exist. Set USE_CACHE to False to fetch fresh data.")

def normalize_name(name):
    import re
    if pd.isnull(name):
        return ""
    return re.sub(r'[^a-z0-9]', '', name.lower())


USE_CACHE = True  # Set to False for production

# gets a list from csv
emerging_technologies = pd.read_csv(emerging_technologies_file, header=None)[0].tolist()
if USE_CACHE:
    check_cache_files()
    techs_df = pd.read_csv(wikidata_csv_path)
    startups_yc = pd.read_csv(yc_csv_path)
    startups_crunchbase = pd.read_csv(crunchbase_csv_path)
    arxiv_df = pd.read_csv(arxiv_csv_path)

    matches_df = pd.read_csv(tech_startup_csv_path)
    edge_df = pd.read_csv(tech_paper_csv_path)

    paper_df = clean_arxiv(arxiv_df)
else:
    # Wikidate
    techs = fetch_wikidata(emerging_technologies)
    techs_df = pd.DataFrame(techs).drop_duplicates(subset="name").sort_values("name").reset_index(drop=True)
    techs_df.to_csv(wikidata_csv_path, index=False)
    # Crunchbase enrichment and YCombinator data
    startups_yc, startups_crunchbase = fetch_crunchbase()
    startups_yc.to_csv(yc_csv_path, index=False)
    startups_crunchbase.to_csv(crunchbase_csv_path, index=False)
    print(f"Saved Crunchbase startups to {crunchbase_csv_path } and YCombinator startups to {yc_csv_path}")
    # Arxiv

    
    papers = fetch_arxiv(emerging_technologies)
    if os.path.exists(arxiv_csv_path):
        os.remove(arxiv_csv_path)
    for paper in papers:
        if paper["response"]:
            df = parse_et(paper["response"], paper["query"])
            df.to_csv(arxiv_csv_path, index=False, mode='a', header=not os.path.exists(arxiv_csv_path))
    # arxiv_df = pd.read_csv(arxiv_csv_path) # Uncomment in case arxiv refused to fetch data

    # Tech to tech matches ???
    print("Saved to data/wikidata_techs_res.csv with", len(techs_df), "entries.")

    # Tech to paper matches
    papers_raw = pd.read_csv(arxiv_csv_path)
    edge_df = match_papers_to_tech(papers_raw, techs_df)
    edge_df.to_csv(tech_paper_csv_path, index=False)
    paper_df = clean_arxiv(papers_raw)

    # Tech to startup matches 
    matches_df = match_startups_to_techs(startups_yc, techs_df)
    matches_df.to_csv(tech_startup_csv_path, index=False)

# YCOMBiNATOR enrichment (aka combine incomplete YC startups with Crunchbase data)
startups_yc["norm_name"] = startups_yc["name"].apply(normalize_name)
startups_crunchbase["norm_name"] = startups_crunchbase["name"].apply(normalize_name)

# Merge YC-labeled startups with Crunchbase data by normalized name
startups_df = startups_yc.merge(
    startups_crunchbase[
        ['norm_name', 'homepage_url', 'category_list', 'funding_total_usd', 'status', 'region']
    ],
    on="norm_name", how="left", suffixes=('', '_crunchbase')
)
# Remove all commas and convert to int/float
startups_df['funding_total_usd'] = (
    startups_df['funding_total_usd']
    .replace({',': '', r'\s*-\s*': ''}, regex=True)  # Remove commas and lone dashes
    .replace('', pd.NA)  # Replace empty strings with NA
)
startups_df['funding_total_usd'] = pd.to_numeric(startups_df['funding_total_usd'], errors='coerce')

# Ensures 'by' columns are not null when dropping duplicates
startups_df = startups_df.sort_values(
    by=['funding_total_usd'],
    ascending=[False],
    na_position='last'
)
startups_df = startups_df.drop_duplicates(subset=['norm_name'], keep='first')

print(len(startups_df), "expanded startup nodes", )
print(len(startups_yc), "startup nodes from ycombinator", )
print(len(startups_crunchbase), "startup nodes from crunchbase", )
print(len(matches_df), "startup to tech edges", )
print(len(techs_df), "tech nodes")
print(len(paper_df), "paper nodes")
print(len(edge_df), "paper to tech edges")

wait_for_neo4j(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASSWORD", "password")
)

load_graph(techs_df, paper_df, edge_df, startups_df, matches_df)
print("✓ Data loaded into Neo4j")

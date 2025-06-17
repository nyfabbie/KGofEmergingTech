# Top level orchestration script for running the pipeline


import pandas as pd
import os
import time
import json
from neo4j import GraphDatabase
from dotenv import load_dotenv

from src.get_arxiv import fetch_arxiv, parse_et
from src.get_crunchbase import fetch_crunchbase
from src.get_wikidata import fetch_wikidata
from src.clean_data import match_papers_to_tech, match_startups_to_techs, clean_arxiv, enrich_and_merge_startups, clean_startups
from src.load_to_neo4j import load_graph

load_dotenv()

wikidata_csv_path = "data/wikidata_techs_res.csv"
crunchbase_csv_path = "data/crunchbase_startups_res.csv"
yc_csv_path = "data/ycombinator_startups_res.csv"
arxiv_csv_path = "data/arxiv_papers_res.csv"
brightdata_path = "data/crunchbase-companies-information.csv"


tech_startup_csv_path = "data/matches_tech_startup.csv"
tech_paper_csv_path = "data/matches_tech_paper.csv"
emerging_technologies_file = os.getenv("EMERGING_TECHS", "data/emerging_techs.json")

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


USE_CACHE = True  # Set to False for production

# gets a list from json
with open(emerging_technologies_file, "r", encoding="utf-8") as f:
    emerging_technologies_json = json.load(f)
emerging_technologies = list(emerging_technologies_json.keys())


if USE_CACHE:
    print("   NOTICE: Using cached data files. Set USE_CACHE to False to fetch fresh data.")
    check_cache_files()
    techs_df = pd.read_csv(wikidata_csv_path)
    startups_yc = pd.read_csv(yc_csv_path)
    startups_crunchbase = pd.read_csv(crunchbase_csv_path)
    arxiv_df = pd.read_csv(arxiv_csv_path)
    cb_info_df = pd.read_csv(brightdata_path, low_memory=False, keep_default_na=False)


    # matches_df = pd.read_csv(tech_startup_csv_path)
    # edge_df = pd.read_csv(tech_paper_csv_path)
else:
    print("   NOTICE: Fetching fresh data...")
    # Wikidate
    techs = fetch_wikidata(emerging_technologies)
    techs_df = pd.DataFrame(techs).drop_duplicates(subset="name").sort_values("name").reset_index(drop=True)
    techs_df.to_csv(wikidata_csv_path, index=False)
    # Crunchbase enrichment and YCombinator data
    startups_yc, startups_crunchbase = fetch_crunchbase()
    startups_yc.to_csv(yc_csv_path, index=False)
    startups_crunchbase.to_csv(crunchbase_csv_path, index=False)
    cb_info_df = pd.read_csv(brightdata_path, low_memory=False)
    print(f"Saved Crunchbase startups to {crunchbase_csv_path }, YCombinator startups to {yc_csv_path} and Brightdata info to {brightdata_path}")
    # Arxiv
    papers = fetch_arxiv(emerging_technologies)
    if os.path.exists(arxiv_csv_path):
        os.remove(arxiv_csv_path)
    for paper in papers:
        if paper["response"]:
            df = parse_et(paper["response"], paper["query"])
            df.to_csv(arxiv_csv_path, index=False, mode='a', header=not os.path.exists(arxiv_csv_path))

# MATCHING
# Tech to tech matches ???
print("Saved to data/wikidata_techs_res.csv with", len(techs_df), "entries.")

# Tech to paper matches
papers_raw = pd.read_csv(arxiv_csv_path)
edge_df = match_papers_to_tech(papers_raw, techs_df)
edge_df.to_csv(tech_paper_csv_path, index=False)
paper_df = clean_arxiv(papers_raw)

# Tech to startup matches 
if 'region' in startups_yc.columns:
    startups_yc = startups_yc.rename(columns={'region': 'location'})
matches_df = match_startups_to_techs(startups_yc, techs_df)
matches_df.to_csv(tech_startup_csv_path, index=False)
cb_info_matches_df = match_startups_to_techs(cb_info_df, techs_df, ["about","industries","full_description"])
cb_info_matches_df.to_csv("data/matches_tech_cbinfo.csv", index=False)

# Merge the two matches DataFrames
all_matches_df = pd.concat([matches_df, cb_info_matches_df], ignore_index=True)
all_matches_df = all_matches_df.sort_values("score", ascending=False).drop_duplicates(subset=["startup_name", "technology"], keep="first")

startups_df_filtered = enrich_and_merge_startups(startups_yc, startups_crunchbase, cb_info_df)
startups_df_filtered, cb_info_df = clean_startups(startups_df_filtered, cb_info_df)

print(len(startups_df_filtered), "filtered startup nodes", )
print(len(startups_yc), "startup nodes from ycombinator", )
print(len(startups_crunchbase), "startup nodes from crunchbase", )
print(len(cb_info_df), "startup nodes from brightdata", )
print(len(cb_info_matches_df), "startups from brightdata api to tech edges", )
print(len(matches_df), "startups from yc+crunchbase to tech edges", )
print(len(techs_df), "tech nodes")
print(len(paper_df), "paper nodes")
print(len(edge_df), "paper to tech edges")

wait_for_neo4j(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASSWORD", "password")
)

load_graph(techs_df, paper_df, edge_df, startups_df_filtered, all_matches_df, cb_info_df)
print("✓ Data loaded into Neo4j")

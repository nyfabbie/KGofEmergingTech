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
        arxiv_csv_path,
        tech_startup_csv_path
    ]
    
    for file in required_files:
        if not os.path.exists(file):
            raise FileNotFoundError(f"Required cache file '{file}' does not exist. Set USE_CACHE to False to fetch fresh data.")


USE_CACHE = True  # Set to False for production



# gets a list from csv
emerging_technologies = pd.read_csv(emerging_technologies_file, header=None)[0].tolist()
if USE_CACHE:
    check_cache_files()
    techs_df = pd.read_csv(wikidata_csv_path)
    startups = pd.read_csv(crunchbase_csv_path)
    arxiv_df = pd.read_csv(arxiv_csv_path)

    matches_df = pd.read_csv(tech_startup_csv_path)
    edge_df = pd.read_csv(tech_paper_csv_path)

    paper_df = clean_arxiv(arxiv_df)
else:
    # Wikidate
    techs = fetch_wikidata(emerging_technologies)
    techs_df = pd.DataFrame(techs).drop_duplicates(subset="name").sort_values("name").reset_index(drop=True)
    techs_df.to_csv(wikidata_csv_path, index=False)
    # Crunchbase
    startups = fetch_crunchbase(emerging_technologies)
    startups.to_csv(crunchbase_csv_path, index=False)
    print(f"Saved Crunchbase startups to {crunchbase_csv_path}")
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
    matches_df = match_startups_to_techs(startups, techs_df)
    matches_df.to_csv(tech_startup_csv_path, index=False)



print(len(startups), "startup nodes", )
print(len(matches_df), "startup to tech edges", )
print(len(techs_df), "tech nodes")
print(len(paper_df), "paper nodes")
print(len(edge_df), "paper to tech edges")

# Before load_graph(...)
wait_for_neo4j(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASSWORD", "password")
)

load_graph(techs_df, paper_df, edge_df, startups, matches_df)
print("✓ Data loaded into Neo4j")

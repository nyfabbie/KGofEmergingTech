# Top level orchestration script for running the pipeline
import pandas as pd
import os

from src.get_arxiv import fetch_arxiv, parse_et
from src.get_crunchbase import fetch_crunchbase
from src.get_wikidata import fetch_wikidata
from src.clean_data import clean_and_merge, match_startups_to_techs
from src.load_to_neo4j import load_graph

# gets a list from csv
emerging_technologies = pd.read_csv("data/emerging_techs.csv", header=None)[0].tolist()

USE_CACHE = True  # Set to False for production

if USE_CACHE:
    techs_df = pd.read_csv("data/wikidata_techs_res.csv")
    startups = pd.read_csv("data/crunchbase_startups_res.csv")
    papers = []  # You may want to load arxiv_papers_res.csv as a DataFrame and adapt your code
else:
    techs = fetch_wikidata(emerging_technologies)
    startups = fetch_crunchbase(emerging_technologies)
    papers = fetch_arxiv(emerging_technologies)
    # Save as usual

# Prototype: Intermediate step to clean & save the fetched data
# should be replaced with more robust cleaning and merging functions
# Wikidata
if not USE_CACHE:
    techs_df = pd.DataFrame(techs).drop_duplicates(subset="name").sort_values("name").reset_index(drop=True)
    techs_df.to_csv("data/wikidata_techs_res.csv", index=False)
    print("Saved to data/wikidata_techs_res.csv with", len(techs_df), "entries.")

# Arxiv: Remove the old file ONCE before parsing new results
arxiv_csv_path = "data/arxiv_papers_res.csv"
if not USE_CACHE and os.path.exists(arxiv_csv_path):
    os.remove(arxiv_csv_path)

if not USE_CACHE:
    for paper in papers:
        if paper["response"]:
            parse_et(paper["response"], paper["query"], output_path=arxiv_csv_path)

# Crunchbase
tech_names = techs_df["name"].tolist()
matches_df = match_startups_to_techs(startups, tech_names, 80, "data/matches_tech_startup.csv")
print("Startup to tech matches:", matches_df)

# Clean and merge just arxiv and Wikidata for now
if not USE_CACHE:
    tech_df, paper_df, edge_df = clean_and_merge(papers, techs)
else:
    tech_df, paper_df, edge_df = clean_and_merge([], techs_df)

print(len(tech_df), "tech nodes")
print(len(paper_df), "paper nodes")
print(len(edge_df), "edges")
load_graph(tech_df, paper_df, edge_df, startups, matches_df)
print("✓ Data loaded into Neo4j")

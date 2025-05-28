# Top level orchestration script for running the pipeline
import pandas as pd

from src.get_arxiv import fetch_arxiv, parse_et
# from src.get_crunchbase import fetch_crunchbase
from src.get_wikidata import fetch_wikidata
from src.clean_data import clean_and_merge
from src.load_to_neo4j import load_graph

# gets a list from csv
emerging_technologies = pd.read_csv("data/emerging_techs.csv", header=None)[0].tolist()

papers = fetch_arxiv(emerging_technologies)
# startups = fetch_crunchbase(emerging_technologies)
techs = fetch_wikidata(emerging_technologies)


# Prototype: Intermediate step to clean & save the fetched data
# should be replaced with more robust cleaning and merging functions
# Wikidata
df = pd.DataFrame(techs).drop_duplicates(subset="input_name").sort_values("input_name").reset_index(drop=True)
df.to_csv("data/wikidata_techs_res.csv", index=False)
print("Saved to data/wikidata_techs_res.csv with", len(df), "entries.")

# Arxiv
for paper in papers:
    if paper["response"]:
        parse_et(paper["response"], paper["query"])

# Crunchbase



# Clean and merge just arxiv and Wikidata for now
tech_df, paper_df, edge_df = clean_and_merge(papers, techs)
print(len(tech_df), "tech nodes")
print(len(paper_df), "paper nodes")
print(len(edge_df), "edges")
load_graph(tech_df, paper_df, edge_df)
print("✓ Data loaded into Neo4j")


# Uncomment the following lines to run the full pipeline
# nodes, relationships = clean_and_merge(papers, startups, techs)
# load_graph(nodes, relationships)

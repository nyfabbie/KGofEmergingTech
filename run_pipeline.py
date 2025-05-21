# Top level orchestration script for running the pipeline

# from get_arxiv import fetch_arxiv
# from get_crunchbase import fetch_crunchbase
from get_wikidata import fetch_wikidata
from clean_data import clean_and_merge
from load_to_neo4j import load_graph

emerging_technologies = ["quantum computing"]

# papers = fetch_arxiv(emerging_technologies)
# startups = fetch_crunchbase(emerging_technologies)
techs = fetch_wikidata(emerging_technologies)

nodes, relationships = clean_and_merge(papers, startups, techs)
load_graph(nodes, relationships)

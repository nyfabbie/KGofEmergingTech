# KGofEmergingTech
This project aims to construct a knowledge graph that maps emerging technologies and their relationships to academic papers and startups. By integrating data from arXiv, Crunchbase, and Wikidata, and utilizing Python for data processing, the goal is to create a graph database in Neo4j. This graph will represent entities such as technologies, papers, and startups, and their interconnections, providing insights into technological trends and innovation networks. The project employs Docker for environment management and GitHub for collaboration, facilitating a reproducible and collaborative workflow.

## The folder structure
```
├── run_pipeline.py ← Runs the whole thing in order
├── data/ ← (for raw/cleaned files if needed)
│
├── src/ ← Core pipeline logic lives here
│ ├── get_arxiv.py ← Pull papers related to a tech
│ ├── get_crunchbase.py ← Pull startups working on that tech
│ ├── get_wikidata.py ← Enrich tech concepts (e.g., synonyms, hierarchy)
│ ├── clean_data.py ← Standardize names, remove duplicates, normalize
│ └── load_to_neo4j.py ← Transform into nodes/edges and push to Neo4j
```


## The ETL pipeline
1. get_arxiv.py

    Input: a target keyword like "quantum computing"
    Fetches paper metadata using arXiv's API (title, abstract, authors, date)
    Outputs: papers_df (a pandas.DataFrame)


2. get_wikidata.py

    Input: the technology name (e.g., "quantum computing")
    Uses SPARQL queries to enrich tech data (e.g., alternative labels, parent/related techs)
    Outputs: tech_df (tech name, Wikidata QID, related techs)

3. get_crunchbase.py (hardest)

    Input: same tech keyword
    Uses Crunchbase API to find startups related to the technology
    Outputs: startups_df (name, description, funding, sectors)

4. clean_data.py

    Input: all 3 DataFrames
    Merges, deduplicates, normalizes names, resolves entity overlaps
    Adds identifiers like tech_id, startup_id, paper_id
    Output: ready-to-load node/relationship dicts or tables

5. load_to_neo4j.py

    Input: cleaned node & relationship data
    Connects to Neo4j via Bolt (with py2neo or neo4j-driver)

    Inserts:
        (:Technology {name})
        (:Paper {title})-[:MENTIONS]->(:Technology)
        (:Startup {name})-[:WORKS_ON]->(:Technology)
        Optionally (:Technology)-[:RELATED_TO]->(:Technology)
"""
Super small loader using neo4j-driver.
Assumes Neo4j is reachable at bolt://localhost:7687
"""

from neo4j import GraphDatabase
import os
import pandas as pd
from src.clean_data import normalize_name

URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")   # default works in Docker network
USER = os.getenv("NEO4J_USER", "neo4j")
PWD  = os.getenv("NEO4J_PASSWORD", "password")


def load_graph(tech_df, paper_df, edge_df, startups_df, matches_df, cb_info_df, tech_synonyms=None):
    # print(tech_df.head())
    # print(tech_df.columns)
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))

    def _tx_load(tx):
        # Tech nodes
        for _, r in tech_df.iterrows(): 
            synonyms = None
            if tech_synonyms is not None:
                # Try label, then name
                synonyms = tech_synonyms.get(r['label']) or tech_synonyms.get(r['name'])
            tx.run("""
                MERGE (t:Technology {tech_id:$qid})
                SET t.tech_name=$label,
                    t.name=$name,
                    t.description=$desc,
                    t.synonyms=$synonyms
            """, qid=r.qid, label=r.label, name=r.name, desc=r.description, synonyms=synonyms)

        # Paper nodes
        for _, r in paper_df.iterrows():
            tx.run("""
                MERGE (p:Paper {paper_id:$pid})
                SET p.arxiv_url=$url,
                    p.title=$title,
                    p.summary=$summary,
                    p.published=date($pub)
            """, pid=r.paper_id, url=r.id, title=r.title,
                 summary=r.summary, pub=r.published.date().isoformat())

        # Paper to Technology edges
        for _, r in edge_df.iterrows():
            tx.run("""
                MATCH (p:Paper {paper_id:$pid})
                MATCH (t:Technology {tech_id:$qid})
                MERGE (p)-[:MENTIONS]->(t)
            """, pid=r.paper_id, qid=r.qid)

        # Only one loop for all startups (merged DataFrame)
        for _, startup in startups_df.iterrows():
            startup_id = startup.get('startup_id', startup['name'].strip())
            tx.run("""
                MERGE (s:Startup {startup_id: $startup_id})
                SET s.name = $name,
                    s.description = 
                        CASE 
                            WHEN s.description IS NULL OR s.description = '' THEN $desc
                            WHEN $desc IS NULL OR $desc = '' THEN s.description
                            WHEN s.description CONTAINS $desc THEN s.description
                            ELSE s.description + ' | ' + $desc
                        END,
                    s.homepage = $homepage,
                    s.category = $category,
                    s.funding = $funding,
                    s.status = $status,
                    s.location = 
                        CASE
                            WHEN s.location IS NULL OR s.location = '' THEN $location
                            WHEN $location IS NULL OR $location = '' THEN s.location
                            WHEN s.location CONTAINS $location THEN s.location
                            ELSE s.location + ' | ' + $location
                        END,
                    s.region = 
                        CASE
                            WHEN s.region IS NULL OR s.region = '' THEN $region
                            WHEN $region IS NULL OR $region = '' THEN s.region
                            WHEN s.region CONTAINS $region THEN s.region
                            ELSE s.region + ' | ' + $region
                        END,
                    s.industries = $industries,
                    s.website = $website,
                    s.founded_date = $founded_date,
                    s.num_employees = $num_employees,
                    s.funding_currency = $funding_currency,
                    s.operating_status = $operating_status,
                    s.company_type = $company_type
            """, startup_id=startup_id, name=startup['name'].strip(),
                desc=startup.get('long_description', startup.get('about', '')),
                homepage=startup.get('homepage_url', ''),
                category=startup.get('category_list', ''),
                funding=startup.get('funding_total_usd', startup.get('funding_total', 0)) if pd.notnull(startup.get('funding_total_usd', startup.get('funding_total', None))) else None,
                status=startup.get('status', ''),
                location=startup.get('location', startup.get('location_extracted', '')),
                region=startup.get('region', ''),
                industries=startup.get('industries', ''),
                website=startup.get('website', ''),
                founded_date=startup.get('founded_date', ''),
                num_employees=startup.get('num_employees', ''),
                funding_currency=startup.get('funding_currency', ''),
                operating_status=startup.get('operating_status', ''),
                company_type=startup.get('company_type', '')
                )

        # Startup to Technology edges
        for _, match in matches_df.iterrows():
            startup_id = match.get('startup_id')
            if not startup_id:
                # fallback to normalized name if not present
                startup_id = normalize_name(match['startup_name'])
            tx.run("""
                MATCH (s:Startup {startup_id: $startup_id})
                MATCH (t:Technology {tech_id: $qid})
                MERGE (s)-[:USES]->(t)
            """, startup_id=startup_id, qid=match.qid)

    with driver.session() as sess:
        sess.execute_write(_tx_load)
    driver.close()

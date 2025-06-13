"""
Super small loader using neo4j-driver.
Assumes Neo4j is reachable at bolt://localhost:7687
"""

from neo4j import GraphDatabase
import os
import pandas as pd

URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")   # default works in Docker network
USER = os.getenv("NEO4J_USER", "neo4j")
PWD  = os.getenv("NEO4J_PASSWORD", "password")


def load_graph(tech_df, paper_df, edge_df, startups_df, matches_df):
    # print(tech_df.head())
    # print(tech_df.columns)
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))

    def _tx_load(tx):
        # Tech nodes
        for _, r in tech_df.iterrows(): 
            tx.run("""
                MERGE (t:Technology {tech_id:$qid})
                SET t.tech_name=$label,
                    t.name=$name,
                    t.description=$desc
            """, qid=r.qid, label=r.label, name=r.name, desc=r.description)

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

        # Startup nodes
        for _, startup in startups_df.iterrows():
            name = startup['name'].strip()
            tx.run("""
                MERGE (s:Startup {name: $name})
                SET s.description = $desc,
                    s.homepage = $homepage,
                    s.category = $category,
                    s.funding = $funding,
                    s.status = $status,
                    s.region = $region
            """, name=name,
                desc=startup.get('long_description', ''),
                homepage=startup.get('homepage_url', ''),
                category=startup.get('category_list', ''),
                funding=startup.get('funding_total_usd', ''),
                status=startup.get('status', ''),
                region=startup.get('region', ''))


        # Startup to Technology edges
        for _, match in matches_df.iterrows():
            startup_name = match['startup_name'].strip()
            tx.run("""
                MATCH (s:Startup {name: $startup_name})
                MATCH (t:Technology {tech_id: $qid})
                MERGE (s)-[:USES]->(t)
            """, startup_name=startup_name, qid=match.qid)

    with driver.session() as sess:
        sess.execute_write(_tx_load)
    driver.close()

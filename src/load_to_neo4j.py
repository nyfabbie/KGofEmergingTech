"""
Super small loader using neo4j-driver.
Assumes Neo4j is reachable at bolt://localhost:7687
"""

from neo4j import GraphDatabase
import os

URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")   # default works in Docker network
USER = os.getenv("NEO4J_USER", "neo4j")
PWD  = os.getenv("NEO4J_PASSWORD", "password")


def load_graph(tech_df, paper_df, edge_df):

    # print(tech_df.head())
    # print(tech_df.columns)
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))

    def _tx_load(tx):
        # Tech nodes
        for _, r in tech_df.iterrows():
            tx.run("""
                MERGE (t:Technology {qid:$qid})
                SET   t.name=$name,
                      t.description=$desc
            """, qid=r.qid, name=r.tech_key, desc=r.description)
            #""", qid=r.qid, name=r.name, desc=r.description)


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

        # Edges
        for _, r in edge_df.iterrows():
            tx.run("""
                MATCH (p:Paper {paper_id:$pid})
                MATCH (t:Technology {qid:$qid})
                MERGE (p)-[:MENTIONS]->(t)
            """, pid=r.paper_id, qid=r.qid)

    with driver.session() as sess:
        sess.execute_write(_tx_load)
    driver.close()

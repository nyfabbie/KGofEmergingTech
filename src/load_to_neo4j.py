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


def load_graph(tech_df, paper_df, edge_df, startups_df, matches_df, cb_info_df, startup_skills_df):
    # print(tech_df.head())
    # print(tech_df.columns)
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))

    def _tx_load(tx):
        # Create constraints
        # tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Technology) REQUIRE t.tech_id IS UNIQUE")
        # tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paper) REQUIRE p.paper_id IS UNIQUE")
        # tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:Startup) REQUIRE s.name IS UNIQUE")
        # tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (sk:Skill) REQUIRE sk.name IS UNIQUE")

        # Load Technologies
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

        # Startup nodes 1
        for _, row in cb_info_df.iterrows():
            name = row['name'].strip()
            tx.run("""
                MERGE (s:Startup {name: $name})
                SET s.description = $about,
                    s.industries = $industries,
                    s.region = $region,
                    s.website = $website,
                    s.founded_date = $founded_date,
                    s.num_employees = $num_employees,
                    s.funding = $funding_total,
                    s.funding_currency = $funding_currency,
                    s.operating_status = $operating_status,
                    s.company_type = $company_type,
                   s.location = $location
            """, name=name,
                about=row.get('about', ''),
                industries=row.get('industries', ''),
                region=row.get('region', ''),
                website=row.get('website', ''),
                founded_date=row.get('founded_date', ''),
                num_employees=row.get('num_employees', ''),
                funding_total=row.get('funding_total', 0) if pd.notnull(row.get('funding_total', None)) else None,
                currency=row.get('funding_currency', ''),
                funding_currency=row.get('funding_currency', ''),
                operating_status=row.get('operating_status', ''),
                company_type=row.get('company_type', ''),
                location=row.get('location_extracted', '')
                )

        # Startup nodes 2
        for _, startup in startups_df.iterrows():
            name = startup['name'].strip()  
            tx.run("""
                MERGE (s:Startup {name: $name})
                SET s.description = 
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
                        END
            """, name=name,
                desc=startup.get('long_description', ''),
                homepage=startup.get('homepage_url', ''),
                category=startup.get('category_list', ''),
                funding=startup.get('funding_total_usd', 0) if pd.notnull(startup.get('funding_total_usd', None)) else None,
                status=startup.get('status', ''),
                location=startup.get('location', ''),
                region=startup.get('region', '')
                )


        # Startup to Technology edges
        for _, match in matches_df.iterrows():
            startup_name = match['startup_name'].strip()
            tx.run("""
                MATCH (s:Startup {name: $startup_name})
                MATCH (t:Technology {tech_id: $qid})
                MERGE (s)-[:USES]->(t)
            """, startup_name=startup_name, qid=match.qid)

        # Load Skill nodes
        skills_query = """
        UNWIND $rows AS row
        MERGE (s:Skill {name: row.skill})
        """
        tx.run(skills_query, rows=startup_skills_df.to_dict('records'))
        print(f"   ✓ Loaded {len(startup_skills_df['skill'].unique())} Skill nodes")

        # Create Startup-HAS_SKILL-Skill relationships
        # This now works reliably because run_pipeline.py ensures the 'start_up' column
        # contains the canonical name that already exists in the graph.
        skill_edges_query = """
        UNWIND $rows AS row
        MATCH (st:Startup {name: row.start_up})
        MATCH (sk:Skill {name: row.skill})
        MERGE (st)-[:HAS_SKILL]->(sk)
        """
        tx.run(skill_edges_query, rows=startup_skills_df.to_dict('records'))
        print(f"   ✓ Created {len(startup_skills_df)} Startup-HAS_SKILL-Skill relationships")
        

    with driver.session() as sess:
        sess.execute_write(_tx_load)
    driver.close()

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


def load_graph(tech_df, paper_df, edge_df, startups_df, matches_df, startup_skills_df, LOAD_SKILLS=False):
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

        print(f"   ✓ Loaded {len(tech_df)} Technology nodes")

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

        print(f"   ✓ Loaded {len(paper_df)} Paper nodes")

        # Paper to Technology edges
        for _, r in edge_df.iterrows():
            tx.run("""
                MATCH (p:Paper {paper_id:$pid})
                MATCH (t:Technology {tech_id:$qid})
                MERGE (p)-[:MENTIONS]->(t)
            """, pid=r.paper_id, qid=r.qid)
        print(f"   ✓ Created {len(edge_df)} Paper-Technology relationships")

        # Startup nodes
        for _, row in startups_df.iterrows():
            original_name = row['original_name_cb_info'] if pd.notnull(row.get('original_name_cb_info')) and row['original_name_cb_info'] else row.get('original_name_yc', '')
            name = row['name'].strip()
            tx.run("""
                MERGE (s:Startup {name: $name})
                SET s.original_name = $original_name, 
                   s.description = $about + '. ' + $desc,
                    s.industries = $industries,
                    s.region = $region,
                    s.website = $website,
                    s.homepage = $homepage,
                    s.founded_date = $founded_date,
                    s.num_employees = $num_employees,
                    s.funding_total = $funding_total,
                    s.funding_currency = $funding_currency,
                    s.operating_status = $operating_status,
                    s.company_type = $company_type,
                    s.location = $location,
                    s.status = $status,
                    s.category = $category
            """, name=name,
                original_name=original_name,
                desc=row.get('long_description', ''),
                about=row.get('about', ''),
                industries=row.get('industries', ''),
                region=row.get('region', ''),
                website=row.get('website', ''),
                homepage=row.get('homepage_url', ''),
                founded_date=row.get('founding_date_final', ''),
                num_employees=row.get('num_employees', ''),
                funding_total=row.get('funding_total_usd', 0) if pd.notnull(row.get('funding_total_usd', None)) else None,
                funding_currency=row.get('funding_currency', ''),
                operating_status=row.get('operating_status', ''),
                company_type=row.get('company_type', ''),
                location=row.get('location_extracted', ''),
                status=row.get('status', ''),
                category=row.get('category_list', '')
                )
        print(f"   ✓ Loaded {len(startups_df)} Startup nodes from ALL startups")


        # Startup to Technology edges
        for _, match in matches_df.iterrows():
            startup_name = match['startup_name'].strip()
            tx.run("""
                MATCH (s:Startup {name: $startup_name})
                MATCH (t:Technology {tech_id: $qid})
                MERGE (s)-[:USES]->(t)
            """, startup_name=startup_name, qid=match.qid)
        print(f"   ✓ Created {len(matches_df)} Startup-Technology relationships")


        if LOAD_SKILLS:
            # Load Skill nodes
            skills_query = """
            UNWIND $rows AS row
            MERGE (s:Skill {name: row.skill_clean})
            """
            tx.run(skills_query, rows=startup_skills_df.to_dict('records'))
            print(f"   ✓ Loaded {len(startup_skills_df['skill_clean'].unique())} Skill nodes")

            # Create Startup-HAS_SKILL-Skill relationships
            skill_edges_query = """
            UNWIND $rows AS row
            MATCH (st:Startup {name: row.start_up})
            MATCH (sk:Skill {name: row.skill_clean})
            MERGE (st)-[:HAS_SKILL]->(sk)
            """
            tx.run(skill_edges_query, rows=startup_skills_df.to_dict('records'))
            print(f"   ✓ Created {len(startup_skills_df)} Startup-HAS_SKILL-Skill relationships")
        

    with driver.session() as sess:
        sess.execute_write(_tx_load)
    driver.close()

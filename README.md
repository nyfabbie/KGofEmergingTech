# KGofEmergingTech
This project aims to construct a knowledge graph that maps emerging technologies and their relationships to academic papers and startups. By integrating data from arXiv, Crunchbase, and Wikidata, and utilizing Python for data processing, the goal is to create a graph database in Neo4j. This graph will represent entities such as technologies, papers, and startups, and their interconnections, providing insights into technological trends and innovation networks. The project employs Docker for environment management and GitHub for collaboration, facilitating a reproducible and collaborative workflow.

## The ETL pipeline

The ETL (Extract, Transform, Load) pipeline orchestrates the process of building the knowledge graph:

1. **Extract**:  
   - Fetches data from multiple sources: arXiv (papers), Crunchbase and Y Combinator (startups), Wikidata (technologies), job boards (staff and skills), and Kaggle (job skills).
   - Supports both fresh data fetching and cached data loading for reproducibility and speed.

2. **Transform**:  
   - Cleans and normalizes data, including deduplication of startups and skills, unification of date and funding fields, and multilingual skill handling.
   - Matches technologies to startups and papers using fuzzy matching and context-aware logic.
   - Enriches startup and staff data with additional attributes and skills.

3. **Load**:  
   - Loads the processed entities and relationships into a Neo4j graph database.
   - Ensures data integrity and uniqueness via constraints and careful merging.
   - Supports optional loading of skill relationships for deeper analytics.

The pipeline is designed to be robust, cache-aware, and configurable via environment variables or interactive prompts. It can be run locally or in Docker, and provides clear logging and error messages to aid debugging and exploration.

---

## The folder structure

```
KGofEmergingTech/
├── compose.yaml
├── Dockerfile
├── requirements.txt
├── run_pipeline.py
├── README.md
├── data/
└── src/
    ├── clean_data.py
    ├── get_arxiv.py
    ├── get_crunchbase.py
    ├── get_jobboard.py
    ├── get_wikidata.py
    └──  load_to_neo4j.py
```

- **compose.yaml / Dockerfile**: Docker and Compose configuration for reproducible environments.
- **requirements.txt**: Python dependencies.
- **run_pipeline.py**: Main orchestration script for the ETL pipeline.
- **data/**: Cached and processed datasets, intermediate files, and configuration JSONs.
- **src/**: All ETL, cleaning, enrichment, and Neo4j loading scripts.

---


## Running the Knowledge Graph locally
1. Make sure Docker Desktop is downloaded (Download here: https://www.docker.com/products/docker-desktop/)
2. Download the repo. If zipped, unzip it. Copy the file path of the unzipped file
3. Go to folder path in terminal
4. Run
    ```bash
   docker build . && docker compose build && docker compose up
    ```
    Wait for it to finish execution. If fetching fresh data it will take a few minutes to fetch and load. 
     When it’s finally done, you will see the message 
     ```bash
    ✓ Data loaded into Neo4j
    ```
   Then go to http://localhost:7474/browser/ . Sign in with username “neo4j” and password as “password”.

5. Explore! Execute some interesting queries from our curated list (see below)

<br><br><br>


# Neo4j Exploration Queries

A curated list of interesting Cypher queries to explore our dataset around emerging technologies, startups, papers, and skills.



## Sample Queries
**Fetch a sample of 3 random emerging technologies and their connected nodes**
```
MATCH (t:Technology)
WITH t, rand() AS r
ORDER BY r
LIMIT 3
// Get connected skills, technologies, and papers
OPTIONAL MATCH (s:Startup)-[:USES]->(t)
OPTIONAL MATCH (s)-[:HAS_SKILL]->(sk:Skill)
```

## Temporal Analysis

**Technologies with papers OR startups before a given year**
```cypher
// Techs with papers before 2007
MATCH (t:Technology)<-[:MENTIONS]-(p:Paper)
WHERE p.published <= date("2007-01-01")
RETURN t, p, null AS s
UNION
// Techs with startups before 2007
MATCH (t:Technology)<-[:USES]-(s:Startup)
WHERE s.founded_date <= date("2007-01-01")
RETURN t, null AS p, s
```

---

## Startups Without Emerging Technologies

**Find startups that do not use any emerging technology**
```cypher
MATCH (s:Startup)
WHERE NOT (s)-[:USES]->(:Technology)
RETURN s.name, s.description
```

---

## Geography of Startups

**Startup count by region**
```cypher
MATCH (s:Startup)
RETURN s.region AS region, count(*) AS startup_count
ORDER BY startup_count DESC
```

**Technologies used by startups, grouped by region**
```cypher
MATCH (s:Startup)-[:USES]->(t:Technology)
RETURN s.region AS region, t.tech_name AS technology, count(*) AS startup_count
ORDER BY region, startup_count DESC
```

**High-funded non-NA startups using technologies**
```cypher
MATCH (s:Startup)-[:USES]->(t:Technology)
WHERE s.region <> 'NA' AND s.funding IS NOT NULL
RETURN s.name, s.region, s.funding, COLLECT(t.tech_name) AS technologies
ORDER BY s.funding DESC
```

---

## Technologies & Funding

**Startups using technologies and their funding**
```cypher
MATCH (s:Startup)-[:USES]->(t:Technology)
WHERE s.funding IS NOT NULL
RETURN s.name, s.funding, t.tech_name
ORDER BY s.funding DESC
```

**Top 10 funded startups using emerging technologies**
```cypher
MATCH (s:Startup)-[:USES]->(t:Technology)
WHERE s.funding IS NOT NULL
RETURN s.name, s.funding, COLLECT(t.tech_name) AS technologies
ORDER BY s.funding DESC
LIMIT 10
```

**Top 10 funded startups (whether or not they use technologies)**
```cypher
MATCH (s:Startup)
WHERE s.funding IS NOT NULL
OPTIONAL MATCH (s)-[:USES]->(t:Technology)
WITH s, COLLECT(t.tech_name) AS technologies
RETURN s.name, s.funding, technologies
ORDER BY s.funding DESC
LIMIT 10
```

---

## Technology Pairing & Clustering

**Technologies co-mentioned in papers**
```cypher
MATCH (t1:Technology)<-[:MENTIONS]-(p:Paper)-[:MENTIONS]->(t2:Technology)
WHERE t1 <> t2
RETURN t1.tech_name AS Tech1, t2.tech_name AS Tech2, COUNT(DISTINCT p) AS shared_papers
ORDER BY shared_papers DESC
```

**Technologies co-used by startups**
```cypher
MATCH (t1:Technology)<-[:USES]-(s:Startup)-[:USES]->(t2:Technology)
WHERE t1 <> t2
RETURN t1.tech_name AS Tech1, t2.tech_name AS Tech2, COUNT(DISTINCT s) AS shared_startups
ORDER BY shared_startups DESC
```

**Isolated technologies (no co-use/co-mention)**
```cypher
MATCH (t:Technology)
WHERE NOT (t)<-[:USES]-(:Startup)-[:USES]->(:Technology)
  AND NOT (t)<-[:MENTIONS]-(:Paper)-[:MENTIONS]->(:Technology)
RETURN t.tech_name
```

---

##  Technology Adoption

**Technologies only mentioned in papers, not used by any startup**
```cypher
MATCH (t:Technology)<-[:MENTIONS]-(:Paper)
WHERE NOT (t)<-[:USES]-(:Startup)
RETURN t
```

---

## Technology Usage Diversity

**Startups using more than one emerging technology**
```cypher
MATCH (s:Startup)-[:USES]->(t:Technology)
WITH s, COUNT(DISTINCT t) AS tech_count
WHERE tech_count > 1
RETURN s.name, tech_count
ORDER BY tech_count DESC
```

**Most popular technologies among startups**
```cypher
MATCH (t:Technology)<-[:USES]-(s:Startup)
RETURN t.tech_name, COUNT(s) AS num_startups
ORDER BY num_startups DESC
```

---

## Startup Skills

**All skills for a specific startup**
```cypher
MATCH (s:Startup {name: "dropbox"})-[:HAS_SKILL]->(sk:Skill)
RETURN sk, s
```

**Top 10 most common skills**
```cypher
MATCH (s:Startup)-[:HAS_SKILL]->(sk:Skill)
RETURN sk.name AS skill, count(*) AS startup_count
ORDER BY startup_count DESC
LIMIT 10
```

**Startups with the most unique skills**
```cypher
MATCH (s:Startup)-[:HAS_SKILL]->(sk:Skill)
RETURN s.name AS startup, count(DISTINCT sk) AS skill_count
ORDER BY skill_count DESC
LIMIT 10
```

**Skills used by only one startup**
```cypher
MATCH (s:Startup)-[:HAS_SKILL]->(sk:Skill)
WITH sk, count(s) AS startup_count
WHERE startup_count = 1
MATCH (s:Startup)-[:HAS_SKILL]->(sk)
RETURN sk.name AS unique_skill, s.name AS startup
```

---

## Skills + Technologies Overlap

**Shared skills & technologies between startup pairs (table view)**
```cypher
MATCH (s1:Startup)-[:HAS_SKILL]->(sk:Skill)<-[:HAS_SKILL]-(s2:Startup),
      (s1)-[:USES]->(t:Technology)<-[:USES]-(s2)
WHERE s1 <> s2
RETURN s1.name, s2.name, sk.name AS shared_skill, t.tech_name AS shared_technology
LIMIT 100
```

**Shared skills & technologies (graph view)**
```cypher
MATCH (s1:Startup)-[:HAS_SKILL]->(sk:Skill)<-[:HAS_SKILL]-(s2:Startup),
      (s1)-[:USES]->(t:Technology)<-[:USES]-(s2)
WHERE s1 <> s2
RETURN s1, s2, sk AS shared_skill, t AS shared_technology
LIMIT 300
```

---

## Skill Patterns

**Startups with a specific skill combo**
```cypher
MATCH (s:Startup)-[:HAS_SKILL]->(sk1:Skill {name: "Python"})
MATCH (s)-[:HAS_SKILL]->(sk2:Skill {name: "Leadership"})
RETURN s, sk1, sk2
```

**Co-occurring skill pairs**
```cypher
MATCH (s:Startup)-[:HAS_SKILL]->(s1:Skill),
      (s)-[:HAS_SKILL]->(s2:Skill)
WHERE id(s1) < id(s2)
RETURN s1.name AS skill1, s2.name AS skill2, count(*) AS co_occurrence
ORDER BY co_occurrence DESC
```

**Skills associated with specific technologies**
```cypher
MATCH (s:Startup)-[:USES]->(t:Technology),
      (s)-[:HAS_SKILL]->(sk:Skill)
RETURN t.tech_name AS technology, sk.name AS skill, count(*) AS count
ORDER BY count DESC
```


# Top level orchestration script for running the pipeline


import pandas as pd
import os
import time
import json
import random
from neo4j import GraphDatabase
from dotenv import load_dotenv

from src.get_arxiv import fetch_arxiv, parse_et
from src.get_crunchbase import fetch_crunchbase
from src.get_wikidata import fetch_wikidata
from src.clean_data import match_papers_to_tech, match_startups_to_techs, clean_arxiv, clean_merge_startups, extract_skills_from_roles, clean_skills, startup_name_normalization
from src.load_to_neo4j import load_graph


from src.get_linkedin import fetch_linkedin, fetch_kaggle


load_dotenv()

wikidata_csv_path = "data/wikidata_techs_res.csv"
crunchbase_csv_path = "data/crunchbase_startups_res.csv"
yc_csv_path = "data/ycombinator_startups_res.csv"
arxiv_csv_path = "data/arxiv_papers_res.csv"
brightdata_path = "data/crunchbase-companies-information.csv"
linkedin_staff_csv_path = "data/linkedin_staff.csv"
kaggle_jobs_csv_path = "data/kaggle_jobs_skills.csv"
startup_skills_csv_path = "data/startup_skills.csv"


tech_startup_csv_path = "data/matches_tech_startup.csv"
tech_paper_csv_path = "data/matches_tech_paper.csv"
emerging_technologies_file = os.getenv("EMERGING_TECHS", "data/emerging_techs.json")

def wait_for_neo4j(uri, user, pwd, max_retries=10, delay=2):
    for i in range(max_retries):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, pwd))
            with driver.session() as session:
                session.run("RETURN 1")
            print("✓ Neo4j is ready!")
            return
        except Exception as e:
            print(f"Waiting for Neo4j to be ready... ({i+1}/{max_retries})")
            time.sleep(delay)
    raise RuntimeError("Neo4j did not become ready in time.")


# Check all cached data files exist or fail with error
def check_cache_files():
    required_files = [
        wikidata_csv_path,
        crunchbase_csv_path,
        yc_csv_path,
        arxiv_csv_path,
        tech_startup_csv_path,
        linkedin_staff_csv_path,
        kaggle_jobs_csv_path,
        startup_skills_csv_path
    ]
    
    for file in required_files:
        if not os.path.exists(file):
            raise FileNotFoundError(f"Required cache file '{file}' does not exist. Set USE_CACHE to False to fetch fresh data.")


USE_CACHE = True  # Set to False for production
SCRAPE_LINKEDIN = False # Set to True to scrape LinkedIn data (long running)

# gets a list from json
with open(emerging_technologies_file, "r", encoding="utf-8") as f:
    emerging_technologies_json = json.load(f)
emerging_technologies = list(emerging_technologies_json.keys())


if USE_CACHE:
    print("   NOTICE: Using cached data files. Set USE_CACHE to False to fetch fresh data.")
    check_cache_files()
    techs_df = pd.read_csv(wikidata_csv_path)
    startups_yc = pd.read_csv(yc_csv_path)
    startups_crunchbase = pd.read_csv(crunchbase_csv_path)
    arxiv_df = pd.read_csv(arxiv_csv_path)
    cb_info_df = pd.read_csv(brightdata_path, low_memory=False, keep_default_na=False)
    final_linkedin_df = pd.read_csv(linkedin_staff_csv_path)
    kaggle_jobs_skills = pd.read_csv(kaggle_jobs_csv_path)
    startup_skills_df = pd.read_csv(startup_skills_csv_path)
    
    # matches_df = pd.read_csv(tech_startup_csv_path)
    # edge_df = pd.read_csv(tech_paper_csv_path)
else:
    print("   NOTICE: Fetching fresh data...")
    # Wikidate
    techs = fetch_wikidata(emerging_technologies)
    techs_df = pd.DataFrame(techs).drop_duplicates(subset="name").sort_values("name").reset_index(drop=True)
    techs_df.to_csv(wikidata_csv_path, index=False)
    # Crunchbase enrichment and YCombinator data
    startups_yc, startups_crunchbase, cb_info_df = fetch_crunchbase()
    startups_yc.to_csv(yc_csv_path, index=False)
    startups_crunchbase.to_csv(crunchbase_csv_path, index=False)
    print(f"Saved Crunchbase startups to {crunchbase_csv_path }, YCombinator startups to {yc_csv_path} and Brightdata info to {brightdata_path}")
    # Arxiv
    papers = fetch_arxiv(emerging_technologies)
    if os.path.exists(arxiv_csv_path):
        os.remove(arxiv_csv_path)
    for paper in papers:
        if paper["response"]:
            df = parse_et(paper["response"], paper["query"])
            df.to_csv(arxiv_csv_path, index=False, mode='a', header=not os.path.exists(arxiv_csv_path))



# ---------MATCHING ---------
startups_yc, startups_crunchbase, cb_info_df = startup_name_normalization(startups_yc, startups_crunchbase, cb_info_df)
# # Print columns in each dataframe for debugging
# print("Columns in startups_yc:", startups_yc.columns.tolist())
# print("Columns in startups_crunchbase:", startups_crunchbase.columns.tolist())
# print("Columns in cb_info_df:", cb_info_df.columns.tolist())


# Tech to tech matches ???
print("Saved to data/wikidata_techs_res.csv with", len(techs_df), "entries.")

# Tech to paper matches
papers_raw = pd.read_csv(arxiv_csv_path)
edge_df = match_papers_to_tech(papers_raw, techs_df)
edge_df.to_csv(tech_paper_csv_path, index=False)
paper_df = clean_arxiv(papers_raw)

# Tech to startup matches 
matches_df = match_startups_to_techs(startups_yc, techs_df)
matches_df.to_csv(tech_startup_csv_path, index=False)
cb_info_matches_df = match_startups_to_techs(cb_info_df, techs_df, ["about","industries","full_description"])
cb_info_matches_df.to_csv("data/matches_tech_cbinfo.csv", index=False)

all_matches_df = pd.concat([matches_df, cb_info_matches_df], ignore_index=True)
all_matches_df = all_matches_df.sort_values("score", ascending=False).drop_duplicates(subset=["startup_name", "technology"], keep="first")



all_startups = clean_merge_startups(startups_yc, startups_crunchbase, cb_info_df)
# print("Columns in all_startups:", all_startups.columns.tolist())
# print("FUNDING EXTRACTION: Number of startups with non-null fundings in all_startups:", all_startups['funding_total_usd'].notnull().sum())
# print("DATE EXTRACTION: Number of startups with non-null dates in all_startups_df:", all_startups['founding_date_final'].notnull().sum())


# --------- Fetch LinkedIn staff data ---------
if SCRAPE_LINKEDIN:
    # Fetch linkedin staff data from crunchbase companies
    all_linkedin_staff = []
    max_staff= 999
    unique_startups = pd.concat([all_startups['name']]).dropna().unique()
    print(f"\nFound {len(unique_startups)} unique startups to scrape from LinkedIn.")

    for i, startup_name in enumerate(unique_startups):
        print(f"Scraping LinkedIn for '{startup_name}' ({i+1}/{len(unique_startups)})...")
        try:
            # Fetch up to max_staff staff members for the current startup
            linkedin_staff = fetch_linkedin(startup_name, max_staff)
            if not linkedin_staff.empty:
                # Overwrite the scraped name with the canonical name from our list
                # to ensure data consistency.
                linkedin_staff['start_up'] = startup_name
                all_linkedin_staff.append(linkedin_staff)
            
            # Sleep with a random delay to mimic human behavior and avoid getting blocked
            sleep_time = random.uniform(5, 15) 
            print(f"   ... success. Sleeping for {sleep_time:.2f} seconds.")
            time.sleep(sleep_time)

        except Exception as e:
            print(f"   ... failed to scrape '{startup_name}': {e}")
            # Optional: sleep even on failure to be extra cautious
            time.sleep(5)

    # Combine all the collected staff data into a single DataFrame
    if all_linkedin_staff:
        final_linkedin_df = pd.concat(all_linkedin_staff, ignore_index=True)
        final_linkedin_df.to_csv(linkedin_staff_csv_path, index=False)
        print(f"\n✓ Successfully scraped and saved staff data for {len(final_linkedin_df['start_up'].unique())} startups.")
    else:
        print("\nNo LinkedIn staff data was collected.")
        final_linkedin_df = pd.DataFrame()
else:
    print("   NOTICE: Skipping LinkedIn scraping. Set SCRAPE_LINKEDIN to True to fetch fresh data.")
    if os.path.exists(linkedin_staff_csv_path):
        final_linkedin_df = pd.read_csv(linkedin_staff_csv_path)
    else:
        final_linkedin_df = pd.DataFrame()

if not USE_CACHE:
    # Fetch Kaggle job postings and skills
    kaggle_jobs_skills = fetch_kaggle()
    kaggle_jobs_skills.to_csv(kaggle_jobs_csv_path, index=False)
    # --------- Create Startup Skills ---------    
    print("\nMatching LinkedIn roles to Kaggle skills...")
    startup_skills_df = extract_skills_from_roles(final_linkedin_df, kaggle_jobs_skills)
# After loading or generating startup_skills_df, clean the skills
startup_skills_df = clean_skills(startup_skills_df)
startup_skills_df.to_csv(startup_skills_csv_path, index=False)
print(f"✓ Saved {len(startup_skills_df)} startup-skill relationships to {startup_skills_csv_path}")




print(len(all_startups), "ALL startup nodes", )
print(len(startups_yc), "startup nodes from ycombinator", )
print(len(startups_crunchbase), "startup nodes from crunchbase", )
print(len(cb_info_df), "startup nodes from brightdata", )
print(len(cb_info_matches_df), "startups from brightdata api to tech edges", )
print(len(matches_df), "startups from yc+crunchbase to tech edges", )
print(len(techs_df), "tech nodes")
print(len(paper_df), "paper nodes")
print(len(edge_df), "paper to tech edges")

wait_for_neo4j(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASSWORD", "password")
)


load_graph(techs_df, paper_df, edge_df, all_startups, all_matches_df, startup_skills_df, SCRAPE_LINKEDIN)
print("✓ Data loaded into Neo4j")

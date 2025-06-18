from staffspy import LinkedInAccount
import kagglehub
from kagglehub import KaggleDatasetAdapter

account = LinkedInAccount(
    # driver_type=DriverType( # if issues with webdriver, specify its exact location, download link in the FAQ
    #     browser_type=BrowserType.CHROME,
    #     executable_path="/Users/pc/chromedriver-mac-arm64/chromedriver"
    # ),
    session_file="session.pkl", # save login cookies to only log in once (lasts a week or so)
    log_level=1, # 0 for no logs
)

# search by company
def fetch_linkedin(startup: str):
    staff = account.scrape_staff(
        company_name="Cofactor Genomics",
        extra_profile_data=True,  # fetch all past experiences, schools, & skills
        max_results=1000,  # can go up to 1000
        # block=True # if you want to block the user after scraping, to exclude from future search results
        # connect=True # if you want to connect with the users until you hit your limit
    )
    staff = staff[['search_term', 'headline', 'current_position', 'skills']]
    staff = staff.rename(columns={'search_term': 'start_up'})
    return staff

def fetch_kaggle():
    jobs_skills = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS,
        "asaniczka/1-3m-linkedin-jobs-and-skills-2024?select=job_skills.csv",
        "job_skills.csv",
    )
    jobs_skills = jobs_skills.set_index('job_link')
    job_postings = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS,
        "asaniczka/1-3m-linkedin-jobs-and-skills-2024?select=linkedin_job_postings.csv",
        "linkedin_job_postings.csv",
        pandas_kwargs={"columns": ["job_link", "job_title"]}
    )
    job_postings = job_postings.set_index('job_link')
    job_postings_skills = jobs_skills.join(job_postings, how='inner')
    return job_postings_skills


fetch_linkedin("Cofactor Genomics").to_csv('C:/Users/abbie/PycharmProjects/KGofEmergingTech/data/linkedin_staff.csv', index=False)
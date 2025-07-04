from staffspy import LinkedInAccount, DriverType, BrowserType
import kagglehub
from kagglehub import KaggleDatasetAdapter
import staffspy.utils.utils as staffspy_utils
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import os
import getpass

# --- MONKEY-PATCH ---
# The original get_webdriver function in staffspy does not allow passing
# the necessary arguments for Chrome to run inside Docker.
# We are replacing it with our own version that adds these arguments.

def patched_get_webdriver(driver_type: DriverType):
    """Our patched version of get_webdriver that adds required Docker arguments."""
    options = Options()
    # These arguments are CRITICAL for running Chrome in a Docker container
    options.add_argument("--headless")
    options.add_argument("--no-sandbox") # Bypasses OS security model, a must for Docker.
    options.add_argument("--disable-dev-shm-usage") # Overcomes limited resource problems.
    options.add_argument("--disable-gpu") # Applicable to windows os only
    options.add_argument("--window-size=1920,1080") # Set a window size

    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Use the executable_path from the DriverType if provided
    service = Service(executable_path=driver_type.executable_path)
    
    # Initialize the driver with our custom options
    return webdriver.Chrome(service=service, options=options)

# Apply the patch by replacing the original function with our new one
staffspy_utils.get_webdriver = patched_get_webdriver

# --- END OF PATCH ---

# This global variable will hold our single, logged-in account instance.
_account = None

# search by company
def fetch_jobboard(startup: str, max=999):
    global _account

    # Lazy initialization: only create the account object if it doesn't exist yet.
    if _account is None:
        print("First-time Jobboard call, prompting for login...")
        _account = LinkedInAccount(
            driver_type=DriverType(
                browser_type=BrowserType.CHROME,
                executable_path="/usr/local/bin/chromedriver" # Explicitly provide the path
            ),
            username=input("Enter a LinkedIn email: "),
            password=getpass.getpass("Enter a LinkedIn password: "),
            log_level=1
        )

    staff = _account.scrape_staff(
        company_name=startup,
        extra_profile_data=True,  # fetch all past experiences, schools, & skills
        max_results=max,  # can go up to 1000
        # block=True # if you want to block the user after scraping, to exclude from future search results
        # connect=True # if you want to connect with the users until you hit your limit
    )
    staff = staff[['search_term', 'headline', 'current_position', 'skills']]
    staff = staff.rename(columns={'search_term': 'start_up'})
    return staff


# Job posting and skills scraped from a jobboard via Kaggle dataset in 2024
def fetch_kaggle():
    jobs_skills = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS,
        "asaniczka/1-3m-linkedin-jobs-and-skills-2024",
        "job_skills.csv",
    )
    jobs_skills = jobs_skills.set_index('job_link')
    job_postings = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS,
        "asaniczka/1-3m-linkedin-jobs-and-skills-2024",
        "linkedin_job_postings.csv",
        pandas_kwargs={"usecols": ["job_link", "job_title"]}
    )
    job_postings = job_postings.set_index('job_link')
    job_postings_skills = jobs_skills.join(job_postings, how='inner')
    return job_postings_skills
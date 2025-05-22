# Version: 0.1
# This script fetches all links related to the article of list of emerging technologies from Wikipedia and saves it to a CSV file.
# Eventually should become our "base" list of emerging technologies instead of those saved in "data/emerging_techs.csv"

import requests
from bs4 import BeautifulSoup
import pandas as pd

#url = "https://en.wikipedia.org/wiki/Emerging_technologies"
url = "https://en.wikipedia.org/wiki/List_of_emerging_technologies"

response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

content = soup.find("div", {"class": "mw-parser-output"})
all_links = content.find_all("a")

tech_entries = []
for link in all_links:
    title = link.get("title")
    href = link.get("href")
    if title and href and href.startswith("/wiki/") and ":" not in href:
        tech_entries.append({
            "name": title,
            "wikipedia_url": f"https://en.wikipedia.org{href}"
        })

df = pd.DataFrame(tech_entries).drop_duplicates(subset="name").sort_values("name").reset_index(drop=True)
df.to_csv("data/emerging_tech_links.csv", index=False)
print("Saved to data/emerging_tech_links.csv with", len(df), "entries.")

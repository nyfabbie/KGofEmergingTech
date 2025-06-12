import urllib, urllib.request
import os
import xml.etree.ElementTree as ET
import pandas as pd



# this function converts search query into arxivAPI url
def fetch_arxiv(queries, max_results=20):
    all_results = []

    for query in queries:
        # Build search query: "quantum computing" → "all:quantum+computing"
        words = query.strip().split()
        formatted_query = "all:" + "+".join(words)

        # Construct the full API URL
        url = f"http://export.arxiv.org/api/query?search_query={formatted_query}&start=0&max_results={max_results}"

        # Fetch the data
        try:
            data = urllib.request.urlopen(url).read().decode('utf-8')
            all_results.append({
                "query": query,
                "response": data
            })
        except Exception as e:
            print(f"Error fetching for '{query}': {e}")
            all_results.append({
                "query": query,
                "response": None,
                "error": str(e)
            })

    return all_results


def parse_et(response, query):
    root = ET.fromstring(response)
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'arxiv': 'http://arxiv.org/schemas/atom',
    }
    entries = []
    for entry in root.findall('atom:entry', ns):
        authors = [author.find('atom:name', ns).text
                   for author in entry.findall('atom:author', ns)]
        entry_data = {
            'id': entry.find('atom:id', ns).text,
            'technology': query,
            'published': entry.find('atom:published', ns).text,
            'updated': entry.find('atom:updated', ns).text,
            'title': entry.find('atom:title', ns).text,
            'summary': entry.find('atom:summary', ns).text,
            'authors': authors
        }
        entries.append(entry_data)

    df = pd.DataFrame(entries).drop_duplicates(subset='id').reset_index(drop=True)

    return df
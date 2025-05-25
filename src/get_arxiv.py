import urllib, urllib.request
import xml.etree.ElementTree as ET
import pandas as pd


# this function converts search query into arxivAPI url
def query_arxiv(query: str, max_results=20):
    words = query.split(" ")
    query = ""
    for word in words:
        if word == words[0]:
            query += "all:" + word
        else:
            query += "&all:"
    url = 'http://export.arxiv.org/api/query?search_query=' + query + '&start=0&max_results=' + str(max_results)
    data = urllib.request.urlopen(url).read().decode('utf-8')
    return data


def parse_et(data, query):
    open("/data/arxiv.csv")
    root = ET.fromstring(data)
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
    if not df.empty:
        df.to_csv("/data/arxiv.csv", index=False, mode='a')
        print("df saved to arxiv.csv")

'''url = 'http://export.arxiv.org/api/query?search_query=all:electron&start=0&max_results=1'''''

import urllib, urllib.request


#this function converts search query into arxivAPI url
def query_arxiv(query: str, max_results=200):
    words = query.split(" ")
    query = ""
    for word in words:
        if word == words[0]:
            query += "all:" + word
        else:
            query += "&all:"
    url = 'http://export.arxiv.org/api/query?search_query=' + query + '&start=0&max_results=' + str(max_results)
    data = urllib.request.urlopen(url)
    print(data.read().decode('utf-8'))


'''url = 'http://export.arxiv.org/api/query?search_query=all:electron&start=0&max_results=1'''''

import re
import requests

from nltk.sentiment.vader import SentimentIntensityAnalyzer 
from nltk import tokenize

from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def cleanData(tweet):
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

    tweet = tweet.replace("RT", "")
    tweet = re.sub(r'@\S*[\s, .]',r'',tweet)
    tweet = emoji_pattern.sub(r'', tweet)
    tweet = re.sub(r'[^a-zA-Z0-9 .]',r'',tweet)
    tweet = re.sub(r"http\S+", "", tweet)
    return tweet

data = cleanData('President Donal Trump approaches his first big test this week from a position of unusual weakness')
print(data)

url = 'https://maps.googleapis.com/maps/api/geocode/json'
params = {'sensor': 'true', 'address': 'United states'}
r = requests.get(url, params=params)
results = r.json()['results']

count = 0
for res in results:
    if count is 0 :
        print(res['geometry']['location'])
        break
        
#location = results[0]['geometry']['location']
#location['lat'], location['lng']


sid = SentimentIntensityAnalyzer()
ss = sid.polarity_scores(data)
print(ss)
s = [(k, ss[k]) for k in sorted(ss, key=ss.get, reverse=True)]
print('{0}: {1}, '.format(s[0][0], s[0][1]), end='')
print()


es.index(index='test-index', doc_type='test', id=1, body=params)

es.index(index='test-index', doc_type='test', id=2, body=params)


es.index(index='test-index', doc_type='test', id=3, body=params)


es.index(index='test-index', doc_type='test', id=4, body=params)


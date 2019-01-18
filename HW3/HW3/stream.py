import json 
import tweepy
import socket
import re
import requests

ACCESS_TOKEN = '311893922-jasoBxDymbqR4OxLBoDLw9Ab0pkPqUX2AVxFoYuy'
ACCESS_SECRET = 'wa87QvHapxNOh3AxDy5bPL1vh9GrfqDA5kKuasbn8unhl'
CONSUMER_KEY = 'UiDazJWIu3SvHSxltmO7mtN6b'
CONSUMER_SECRET = 'YDI2mHOyWXfjUupr1CpUNVpAD6oin6YQ2u03RpWUtPbFeaXn4H'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

hashtag = '#guncontrolnow'
TCP_IP = 'localhost'
TCP_PORT = 9002

url = 'https://maps.googleapis.com/maps/api/geocode/json'

def getLocation(location):
    if location is None:
        return { 'lat' : 0.0, 'lon' : 0.0}
    else:
        params = {'sensor': 'false', 'address': location}
        r = requests.get(url, params=params)
        results = r.json()['results']
        for res in results:
            print(res['geometry']['location'])
            location = { 'lat' :res['geometry']['location']['lat'],
                         'lon' : res['geometry']['location']['lng']}
            return location

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
    tweet = re.sub(r'[^a-zA-Z0-9 .#]',r'',tweet)
    tweet = re.sub(r"http\S+", "", tweet)
    return tweet

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print("____________tweet_____________")
        dict = {'text': cleanData(status.text), 
                'location': getLocation(status.user.location), 
                'timestamp_ms' : status.timestamp_ms }
        print(dict)
        conn.send((json.dumps(dict) + "\n").encode('utf-8'))
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag])

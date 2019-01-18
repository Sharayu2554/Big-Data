from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json 
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob
from nltk import tokenize
from elasticsearch import Elasticsearch

def getData(input):
	data = json.loads(input)
	sentimentTweet = get_tweet_sentiment(data['text'])
	data['sentiment'] = sentimentTweet
	print("_____data in getData______")	
	print(data)
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
	es.index(index='gun-control-now-test', doc_type='data',  body=json.dumps(data))
	return data

def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(tweet)
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return 'pos'
    elif analysis.sentiment.polarity == 0:
        return 'neu'
    else:
        return 'neg'

def getSentiment(data):
	ss = sid.polarity_scores(data)
	print(ss)
	s = [(k, ss[k]) for k in sorted(ss, key=ss.get, reverse=True)]
	print('{0}: {1}, '.format(s[0][0], s[0][1]), end='')
	return s[0][0]

#
TCP_IP = 'localhost'
TCP_PORT = 9002

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

######### your processing here ###################

print('Processing Data From Tweet :- ')
data = dataStream.map(lambda x : getData(x))
data.pprint()

#################################################

ssc.start()
ssc.awaitTermination()

################################################

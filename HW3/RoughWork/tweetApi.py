import json 
import tweepy
import socket


ACCESS_TOKEN = '311893922-jasoBxDymbqR4OxLBoDLw9Ab0pkPqUX2AVxFoYuy'
ACCESS_SECRET = 'wa87QvHapxNOh3AxDy5bPL1vh9GrfqDA5kKuasbn8unhl'
CONSUMER_KEY = 'UiDazJWIu3SvHSxltmO7mtN6b'
CONSUMER_SECRET = 'YDI2mHOyWXfjUupr1CpUNVpAD6oin6YQ2u03RpWUtPbFeaXn4H'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#Trump'

TCP_IP = 'localhost'
TCP_PORT = 9008


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))

s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()


class MyStreamListener(tweepy.StreamListener):

	def on_status(self, status):
		my_json_string = json.dumps({'text': status.text, 'geo': status.geo, 'lang' : status.lang, 'timestamp_ms' : status.timestamp_ms, 'place' : status.place,'coordinates' : status.coordinates})
		print("___________________status text________________")
		print(my_json_string)
		conn.send(status.text.encode('utf-8'))
    	#conn.send(status.timestamp_ms.encode('utf-8'))
	def on_error(self, status_code):
		if status_code == 420:
			return False
		else:
			print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])
#print(myStream)
#conn.close()
s.close()
import tweepy
import socket

ACCESS_TOKEN = '311893922-jasoBxDymbqR4OxLBoDLw9Ab0pkPqUX2AVxFoYuy'
ACCESS_SECRET = 'wa87QvHapxNOh3AxDy5bPL1vh9GrfqDA5kKuasbn8unhl'
CONSUMER_KEY = 'UiDazJWIu3SvHSxltmO7mtN6b'
CONSUMER_SECRET = 'YDI2mHOyWXfjUupr1CpUNVpAD6oin6YQ2u03RpWUtPbFeaXn4H'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

for i in range(9002, 9014):
    print("In for")
    TCP_IP = 'localhost'
    TCP_PORT = i
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("In s")
    s.bind((TCP_IP, TCP_PORT))
    print("In s binf")
    #s.listen(1)
    print("In s listen")
    #conn, addr = s.accept()
    print("In s accept")
    #conn.close()
    print("In conn close")
    s.close()
    print("Closed ", i)

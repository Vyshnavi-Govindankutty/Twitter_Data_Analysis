import tweepy as tw
from tweepy import Stream
#from tweepy.streaming import StreamListener
import socket
import json

from google.cloud import pubsub_v1

publisher=pubsub_v1.PublisherClient()
topic_path=publisher.topic_path('mytestproject-326317','twitter-data')

def pubsub_write(data):
	publisher.publish(data,topic_path)


class Listener(Stream):
	
    #def __init__(self):
    #    super(Listener, self).__init__()
    
    
    def data_proc(self,d):
        tweet={}
        try:
            if "retweeted" in d:
                try:
                    if (not d["retweeted"]) and ("extended_tweet" in d) and ('RT' not in d["extended_tweet"]["full_text"]):
                        tw=d["extended_tweet"]["full_text"]
                        retweet=d['retweeted']

                    elif (not d["retweeted"]) and ("text" in d) and ('RT' not in d["text"]):
                        tw=d['text']
                        retweet=d['retweeted']

                    else:
                        tw='Null'
                        retweet='Null'
                
                except KeyError:
                    tw='Null'
                    retweet='Null'
                    print(KeyError)
            else:
                tw='Null'
                retweet='Null'

        except:
            tw='Null'
            retweet='Null'
        try:
            if "created_at" in d:
                dt_tme=d['created_at']

            else:
                dt_tme='Null'
        except KeyError:
            print(KeyError)

        tweet['tw']=tw
        tweet['created_at']=dt_tme
        tweet['retweet']=str(retweet)
        t=("["+json.dumps(tweet)+"]\n").encode('utf-8')
        #t=bytes(t,'utf-8')
        if isinstance(t,bytes):
        	print(t)
        #print(tweet)
        #pubsub_write(t)
        publisher.publish(topic_path,t)
    

    def on_data(self,data):
        tweet=json.loads(data)
        #print(tweet)
        self.data_proc(tweet)
        return True        
        
        
    def on_error(self,status_code):
        print(status_code)
        return True
        
        

streaming_data = Listener(consumer_key,consumer_secret,access_token,access_token_secret)
streaming_data.filter(track=['$AAPL','$GOOGL','$GOOG','$MSFT','$AMZN','$FB','$TSLA','$BRK.B','$NVDA','$JPM','SP500','$SPY','$JNJ','$JPM','$UNH','$V','$PG','$HD','$PYPL','$DIS','$MA','$ADBE','$BAC','$PFE','$CMCSA','$CRM','$CSCO','$NFLX','$VZ','$ABT','$XOM','$KO','$PEP','$TMO','$LLY','$NTC','$NKE','$ACN','$ABBV',\
'$WMT','$DHR','$COST','$MRK','$T','$WFC','$AVGO','$CVX','$MCD','$MDT','$TXN','$NEE','$LIN','$ORCL','$QCOM','$HON','$PM','$BMY','$UNP','$INTU','$MS','$C','$LOW','$UPS','$SBUX','$GS','$AMT',\
'$AMGN','$BLK','$RTX','$AMD','$ISRG','$IBM','$TGT','$AMAT','$BA','$NOW','$MRNA','$MMM','$DE','$CAT','$CVS','$GE','$CHTR','$SCHW','$SPGI','$AXP','$PLD','$ZTS','$ANTM','$ADP','$GILD','$MDLZ',\
'$MO','$TJX','$LMT','$SYK','$CCI','$BKNG','$CB','$TMUS','$LRCX','$DUK','$FIS'],languages=['en'])
    








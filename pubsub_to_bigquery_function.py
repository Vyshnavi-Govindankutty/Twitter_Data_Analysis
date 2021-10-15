import base64
import json

from google.cloud import bigquery

def write_bigquery(tweets):
  client=bigquery.Client()
  table_id='mytestproject-326317.twitter_dataset.tweet_table'
  table=client.get_table(table_id)
  
  #rows=[{"tw": "$GWSO - crazy move today. The range is $4 and we are back to break even for the day. Let's get in positive territory. Shorts were working hard today. $TSLA #ElonMusk #BillGates $QS", "created_at": "Thu Oct 14 19:24:29 +0000 2021", "retweet": "false"}]

  print(tweets)
  dataset=client.insert_rows_json(table,json.loads(tweets))

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    write_bigquery(pubsub_message)
    #print(pubsub_message)

import os

from google.api_core import exceptions
from google.oauth2 import service_account
from google.cloud import pubsub_v1

from functions.pipelines import create_subscription, create_topic, topic_exists, publish_message, publish_data
#key to connect with my env in Google Cloud
json_path = "../key2.json"

# set up the credentials to be able to publish data in the pub/sub
# Load the credentials
credentials = service_account.Credentials.from_service_account_file(json_path)
publisher = pubsub_v1.PublisherClient(credentials=credentials)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

# Settings
PROJECT_ID = "neat-element-338511"

if __name__ == "__main__":
    #1. Create as many topics as files we have, better choice to store each file in different topics
    for filename in os.listdir("../data/"):
        if filename.endswith(".json"):
            name, _ = os.path.splitext(filename)
            print(name)
            if not topic_exists(PROJECT_ID, name, publisher):
                topic_path = create_topic(PROJECT_ID, name, publisher)
                subscription_id = name
                create_subscription(PROJECT_ID, name, subscriber,subscription_id)
                print(topic_path)
            publish_data(publisher,filename,PROJECT_ID,name)






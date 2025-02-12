import pandas
import os
import json

from google.api_core import exceptions
from google.oauth2 import service_account
from google.cloud import pubsub_v1

#key to connect with my env in Google Cloud
json_path = "../key.json"

# set up the credentials to be able to publish data in the pub/sub
# Load the credentials
credentials = service_account.Credentials.from_service_account_file(json_path)
publisher = pubsub_v1.PublisherClient(credentials=credentials)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

# Settings
PROJECT_ID = "neat-element-338511"

#topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def create_topic(project_id, topic_id,publisher):
    topic_path = publisher.topic_path(project_id, topic_id)

    # Create the topic
    try:
        topic = publisher.create_topic(request={"name": topic_path})
        print(f"Topic created: {topic.name}")
    except Exception as e:
        print(f"Error while creating the topic: {e}")
    return topic_path

def create_subscription(project_id, topic_id,subscriber,subscription_id):
    topic_path = f"projects/{project_id}/topics/{topic_id}"
    subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"

    # Create the subscription
    try:
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        print(f"Subscription created: {subscription.name}")
    except Exception as e:
        print(f"Error while creating the subscription: {e}")

def topic_exists(project_id, topic_id, publisher):
    topic_path = publisher.topic_path(project_id, topic_id)

    try:
        publisher.get_topic(request={"topic": topic_path})
        print(f"The topic {topic_id} already exists.")
        return True
    except exceptions.NotFound:
        print(f"The topic {topic_id} does not exists.")
        return False

def publish_message(publisher,data,project_id,topic_id):
    """Publish the message in the topic Pub/Sub."""
    try:
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        topic_path = publisher.topic_path(project_id, topic_id)
        future = publisher.publish(topic_path, data=message_bytes)
        print(f"Published message to {topic_path}: {future.result()}")

    except Exception as e:
        print(f"Error publishing message: {e}")

def publish_data(publisher,directory,project_id,topic_id):
    """Main function to read the data from the different files and publish it in the topic"""
    try:
        with open(os.path.join("../data/", directory), "r", encoding="utf-8") as f:
            #The json format is not the proper so i need to read line by line
            print(f)
            data_total = []
            for line in f:
                if line.strip():  # avoid empty lines
                    data = json.loads(line)
                    data_total.append(data)
        # publish each element from the list
        for element in data_total:
            publish_message(publisher,element,project_id, topic_id)

    except FileNotFoundError:
        print("Error: file not found.")
    except Exception as e:
        print(f"Error reading or processing data: {e}")

if __name__ == "__main__":
    #1. Create as many topics as files we have, better choice to store each file in different topics
    for filename in os.listdir("../data/"):
        if filename.endswith(".json"):
            name, _ = os.path.splitext(filename)
            print(name)
            if not topic_exists(PROJECT_ID, name, publisher):
                topic_path = create_topic(PROJECT_ID, name, publisher)
                subscription_id = "testing"
                create_subscription(PROJECT_ID, name, subscriber,subscription_id)
                print(topic_path)
            publish_data(publisher,filename,PROJECT_ID,name)






from google.api_core import exceptions
import json
import os

def create_topic(project_id, topic_id,publisher):
    """
    Create the topic

    :param project_id: name of the project
    :param topic_id: name of the topic
    :param publisher: credentials to publish data in the topic
    :return:
    """
    topic_path = publisher.topic_path(project_id, topic_id)

    # Create the topic
    try:
        topic = publisher.create_topic(request={"name": topic_path})
        print(f"Topic created: {topic.name}")
    except Exception as e:
        print(f"Error while creating the topic: {e}")
    return topic_path

def create_subscription(project_id, topic_id,subscriber,subscription_id):
    """
    Create the subscription to publish data inside the topic

    :param project_id: name of the project
    :param topic_id: name of the topic
    :param subscriber: credentials to the subscription
    :param subscription_id: name of the subscription
    :return:
    """
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
    """
    Function to verify if the topic exists

    :param project_id: name of the project
    :param topic_id: name of the topic
    :param publisher: credentials to publish data
    :return:
    """
    topic_path = publisher.topic_path(project_id, topic_id)

    try:
        publisher.get_topic(request={"topic": topic_path})
        print(f"The topic {topic_id} already exists.")
        return True
    except exceptions.NotFound:
        print(f"The topic {topic_id} does not exists.")
        return False

def publish_message(publisher,data,project_id,topic_id):
    """
    Function to publish message to topic

    :param publisher: credentials to publish data
    :param data: data to publish
    :param project_id: name of the project
    :param topic_id:  name of the topic
    :return:
    """
    try:
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        topic_path = publisher.topic_path(project_id, topic_id)
        future = publisher.publish(topic_path, data=message_bytes)
        print(f"Published message to {topic_path}: {future.result()}")

    except Exception as e:
        print(f"Error publishing message: {e}")

def publish_data(publisher,directory,project_id,topic_id):
    """
    Function to publish message to topic

    :param publisher: credentials to publish data
    :param directory: folder to read the data from
    :param project_id: name of the project
    :param topic_id: name of the topic
    :return:
    """
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

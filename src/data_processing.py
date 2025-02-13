from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.oauth2 import service_account
import json
from datetime import datetime,timezone

# Key to connect with my env in Google Cloud
json_path = "../key2.json"

# Set up the credentials
credentials = service_account.Credentials.from_service_account_file(json_path)
publisher = pubsub_v1.PublisherClient(credentials=credentials)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
bigquery_client = bigquery.Client(credentials=credentials)

# Settings
PROJECT_ID = "neat-element-338511"
TABLE_LOCATIONS = "neat-element-338511.dataset.locations"
TABLE_TRANSACTIONS = "neat-element-338511.dataset.transactions"
TABLE_SEGMENT = "neat-element-338511.dataset.transactions_segment"

def process_message_locations(message):
    """
    Process messages for the 'locations' subscription

    :param message:
    :return:
    """
    try:
        data = json.loads(message.data.decode("utf-8"))
        data['TimeStamp'] = datetime.now(timezone.utc).timestamp()
        errors = bigquery_client.insert_rows_json(TABLE_LOCATIONS, [data])
        if not errors:
            print("Location data successfully inserted in BigQuery")
            message.ack()
        else:
            print(f"Error inserting location data: {errors}")
            message.nack()
    except Exception as e:
        print(f"Error processing location message: {e}")
        message.nack()

def process_message_transactions(message):
    """
    Process messages for the 'transactions' subscription

    :param message:
    :return:
    """
    try:
        data = json.loads(message.data.decode("utf-8"))
        transaction_data = {
            "UniqueId": data.get("UniqueId"),
            "TransactionDateUTC": data.get("TransactionDateUTC"),
            "Itinerary": data.get("Itinerary"),
            "OriginAirportCode": data.get("OriginAirportCode"),
            "DestinationAirportCode": data.get("DestinationAirportCode"),
            "OneWayOrReturn": data.get("OneWayOrReturn"),
            "TimeStamp": datetime.now(timezone.utc).timestamp(),
        }
        # Insert transaction data
        errors = bigquery_client.insert_rows_json(TABLE_TRANSACTIONS, [transaction_data])
        if errors:
            print(f"Errors inserting transaction: {errors}")
            message.nack()
            return

        # Process segments
        segments = data.get("Segment", [])
        #print(segments)
        for segment in segments:
            segment_data = {
                "UniqueId": data.get("UniqueId"),
                "SegmentNumber": segment.get("SegmentNumber"),
                "LegNumber": segment.get("LegNumber"),
                "DepartureAirportCode": segment.get("DepartureAirportCode"),
                "ArrivalAirportCode": segment.get("ArrivalAirportCode"),
                "NumberOfPassengers": int(segment.get("NumberOfPassengers", 0))
            }

            # Insert segment data
            errors = bigquery_client.insert_rows_json(TABLE_SEGMENT, [segment_data])
            if errors:
                print(f"Errors inserting segment: {errors}")
                message.nack()
                return

        print("Transaction and segment data successfully inserted in BigQuery")
        message.ack()
    except Exception as e:
        print(f"Error processing transaction message: {e}")
        message.nack()

# Configure subscriptions
subscription_locations = subscriber.subscription_path(PROJECT_ID, "locations")
subscription_transactions = subscriber.subscription_path(PROJECT_ID, "transactions")

# Start listening to both subscriptions with separate callbacks
streaming_pull_future_locations = subscriber.subscribe(subscription_locations, callback=process_message_locations)
streaming_pull_future_transactions = subscriber.subscribe(subscription_transactions, callback=process_message_transactions)

print(f"Reading messages from {subscription_locations} and {subscription_transactions}")

# Keep the program running
try:
    streaming_pull_future_locations.result()
    streaming_pull_future_transactions.result()
except KeyboardInterrupt:
    streaming_pull_future_locations.cancel()
    streaming_pull_future_transactions.cancel()

# **TRAVIX - ALBANO GONZALEZ**

## **Purpose of the Assignment**

The aim of this test is to create a system for publishing and reading data in the Google cloud. To do so, we are going to create a pipeline that ingests data in the topics and at the same time reads them to infest them in our storage layer.

---



## **What the Code Does**

### **1. Configuration Management**
- The `key.json` file provides the credentials to connect our environment with Google Cloud. We need to create a service account and create the role to work with Pub/Sub and BigQuery.

---

### **2. Data Extraction from files and storing it into Pub/Sub**
This step involves reading the data from the JSON files and publishing it to the corresponding Google Cloud Pub/Sub topics.
Initial configuration:
- The google-cloud-pubsub and json libraries are used.
Google Cloud credentials are configured.
- File reading:
JSON files (locations.json and transactions.json) are read using the json library.
- Pub/Sub publishing:
A Pub/Sub client is created.
For each record in the JSON files:
The record is converted to byte format.
The message is published in the corresponding topic (‘locations’ or ‘transactions’).
- Docker: We create the dockerFile to run the pipeline and store the data in our Pub/Sub topics.
- Error handling:
An error handling system is implemented to ensure that all messages are published correctly.

### **3. Data Processing**
This step involves consuming the messages from the Pub/Sub topics, processing them and storing them in the corresponding BigQuery tables.
- Initial configuration:
The google-cloud-pubsub and google-cloud-bigquery libraries are used.
Google Cloud credentials are configured.
- Creation of subscriptions:
Subscriptions are created for the topics ‘locations’ and ‘transactions’.
- Message processing:
For each message received:
The Pub/Sub message is decoded.
The content is processed according to the type of message (location or transaction).
- Insertion in BigQuery:
For location messages:
They are inserted directly into the ‘locations’ table.
For transaction messages:
The main data is inserted into the ‘transactions’ table.
Segments are processed and inserted into the ‘transactions_segment’ table.
- Error handling and acknowledgement:
An error handling system is implemented.
Successfully processed messages are acknowledged (ack).
Messages that cannot be processed are rejected (nack) for later retry.
- Continuous execution:
The process runs continuously, listening for new messages on both subscriptions.

### **4. SQL Queries**

I have created 3 tables to store the data.
Locations: This table stores information about the location file.
Transactions: This table stores information about the transactions file.
Transactions_segment: This table stores information about the segments of each row in the transactions file. I also store the uniqueId to be able to join both tables.
1.  From which Country are most transactions originating? How many transactions is this?

I did not store all the transactions but with this run you should be able to get the country and how many transactions.

"SELECT l.CountryName, COUNT(t.UniqueId) AS total
FROM `neat-element-338511.dataset.transactions` t
JOIN `neat-element-338511.dataset.locations` l
ON t.OriginAirportCode = l.AirportCode
GROUP BY l.CountryName
ORDER BY total DESC;"

2. What's the split between domestic vs international transactions?


"SELECT
  CASE
    WHEN t.OriginAirportCode = t.DestinationAirportCode THEN 'Domestic'
    ELSE 'International'
  END AS flight,
  COUNT(*) AS transaction_count
FROM `neat-element-338511.dataset.transactions` t
GROUP BY flight;"

3. What's the distribution of number of segments included in transactions?

I dont understand the query here.

I have created a table which name is transactions_segment, this one is going to store the different segments withing the transactions.
You could be able to join this table with the transactions table joining by uniqueId and sum the numberOfPassengers up.



### **Setup and Execution**

1. **Set Up the Environment**:
   - I have used pycharm as IDE
   - Clone the repository:
   - install pyenv (if not already installed, to manage Python versions):
      - brew install pyenv
      - pyenv install 3.13
      - pyenv local 3.13
      - poetry env use $(pyenv which python)
   - In the repository folder run: "poetry install" to install the whole dependencies. If you have not already installed poetry you should do it before (curl -sSL https://install.python-poetry.org | python3)
   - poetry shell
   - set the interpreter in the IDE (the one you created, /.venv/bin/python/python3.13)
   


2. **Run the Script**:
   - Go to Google cloud - IAM and create a service account. Then assign the following roles: Administrator pub/sub and administrator BigQuery, after that go to keys - create new one - download as json. You should add it to your project to set up the configuration.
   - Execute the script: `python main.py` or crete the image in your docker to run it: docker build -t pubsub-publisher || docker run -it --rm pubsub-publisher
   - Go to Google cloud and check the topics.
   - Create the tables Locations, Transactions and Transaction_Segment within your BigQuery account to store the data in these 3 tables.
   - Run the data_processing.py to send the data from the topics to the storage layer (BigQuery).
   - Check the tables to see if there are being populated.
   - Run the queries to analyze the values.

---

### **Future Improvements**
1. **Testing**:
   - Create some test to validate the code.
2. **Create more functions to validate the topics and subscription, also to remove them**: 
3. **Orchestration**:
   - Finish the implementation with Airflow.
4. **Automate**:
   - Create a json to automate the process and create the topics and tables automatically
5. **Power BI**:
   - Create some Dashboards with PowerBi to show the insights.
---



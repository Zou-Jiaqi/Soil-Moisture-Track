from concurrent.futures import ThreadPoolExecutor
from google.cloud import pubsub_v1
from fastavro import schemaless_reader, parse_schema, writer
import json
import logging
import sys
import cygnss_ingest
import smap_ingest
import os
import io

project_id = os.getenv("PROJECT_ID")
subscription_id = os.getenv("SUBSCRIPTION_ID")
schema_id = os.getenv("SCHEMA_ID")
number_of_retries = int(os.getenv("NUMBER_OF_RETRIES"))
topic_id = os.getenv("TOPIC_ID")

logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,   # Important! Cloud Run reads from stdout
)


def pull_date():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    client = pubsub_v1.SchemaServiceClient()
    schema_path = client.schema_path(project_id, schema_id)
    schema = client.get_schema(request={"name": schema_path})
    schema_dict = json.loads(schema.definition)

    for i in range(number_of_retries):
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 1}
        )
        for msg in response.received_messages:
            data_bytes = msg.message.data
            bytes_io = io.BytesIO(data_bytes)
            decoded = schemaless_reader(bytes_io, schema_dict)
            if "DownloadDate" in decoded:
                subscriber.acknowledge(
                    request={"subscription": subscription_path, "ack_ids": [msg.ack_id]}
                )
                datestr = decoded["DownloadDate"]
                return datestr
    raise Exception(f"No download date received from pub/sub.")


def push_date():
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    client = pubsub_v1.SchemaServiceClient()
    schema_path = client.schema_path(project_id, schema_id)
    schema = client.get_schema(request={"name": schema_path})

    # Parse schema
    parsed_schema = parse_schema(schema)

    # Create a record
    record = {"DownloadDate": "2025-06-20"}

    # Serialize using fastavro
    buffer = io.BytesIO()
    writer(buffer, parsed_schema, [record])
    avro_bytes = buffer.getvalue()

    # Publish to Pub/Sub
    future = publisher.publish(topic_path, data=avro_bytes)
    print(f"Published message ID: {future.result()}")


def ingest_all():
    executor = ThreadPoolExecutor(2)
    push_date()
    datestr = pull_date()
    smap_exec = executor.submit(smap_ingest.ingest, datestr)
    cygnss_exec = executor.submit(cygnss_ingest.ingest, datestr)
    smap_exec.result()
    cygnss_exec.result()


if __name__ == '__main__':
    ingest_all()

from google.cloud import pubsub_v1
import csv
import json
import time

project_id = "burnished-web-484613-t0"
topic_id = "house_topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open("houseprice.csv") as file:

    reader = csv.DictReader(file)

    for row in reader:

        message = json.dumps(row).encode("utf-8")

        publisher.publish(topic_path, message)

        print("Published:", row)

        time.sleep(1)  # 1 row per second (streaming simulation)


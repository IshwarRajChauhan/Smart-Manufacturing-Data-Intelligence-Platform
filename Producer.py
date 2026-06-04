import boto3
import json
import random
import time
from datetime import datetime

kinesis = boto3.client("kinesis")

STREAM_NAME = "manufacturing-stream"

# Example of sending a bad record (missing fields, wrong data types, etc.)
# data = {
#     "machine_id": "M999",
#     "temperature": 100
# }

kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey=data["machine_id"]
    )

for _ in range(1):

    data = {
    "machine_id": f"M{random.randint(1,5):03}",
    "temperature": random.randint(60, 100),
    "vibration": round(random.uniform(1.0, 8.0), 2),
    "power_usage": random.randint(100, 500),
    "timestamp": datetime.utcnow().isoformat()
}

    if random.random() > 0.7:
        data["humidity"] = random.randint(30, 80)

    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey=data["machine_id"]
    )

    print(f"Sent: {data}")

    time.sleep(2)

print("Finished sending records.")

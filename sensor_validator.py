import json
import boto3
import base64
from datetime import datetime

s3 = boto3.client("s3")

BUCKET = "manufacturing-data-lake-ishwar"

REQUIRED_FIELDS = [
    "machine_id",
    "temperature",
    "timestamp"
]

def lambda_handler(event, context):

    for record in event["Records"]:

        try:
            # Decode Kinesis record
            payload = base64.b64decode(
                record["kinesis"]["data"]
            ).decode("utf-8")

            data = json.loads(payload)

            # Validate required fields
            is_valid = all(
                field in data and data[field] is not None
                for field in REQUIRED_FIELDS
            )

            current_time = datetime.utcnow()

            file_name = (
                current_time.strftime("%Y%m%d_%H%M%S_%f")
                + ".json"
            )

            if is_valid:

                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"raw/{file_name}",
                    Body=json.dumps(data)
                )

                print(f"VALID -> raw/{file_name}")

            else:

                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"quarantine/{file_name}",
                    Body=json.dumps(data)
                )

                print(f"INVALID -> quarantine/{file_name}")

        except Exception as e:

            error_record = {
                "error": str(e),
                "raw_record": str(record)
            }

            file_name = (
                datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
                + "_error.json"
            )

            s3.put_object(
                Bucket=BUCKET,
                Key=f"quarantine/{file_name}",
                Body=json.dumps(error_record)
            )

            print(f"ERROR -> {str(e)}")

    return {
        "statusCode": 200,
        "body": "Processing Complete"
    }
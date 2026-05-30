import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):

    response = glue.start_job_run(
        JobName="manufacturing-etl"
    )

    print("Glue Job Started")
    print(response)

    return {
        "statusCode": 200
    }
from time import sleep
from kafka import KafkaConsumer
import boto3
import json

botoclient = boto3.client('s3')

if __name__ == "__main__":
    print("starting batch consumer app")
    consumer = KafkaConsumer(
        'PinterestTopic',
        bootstrap_servers=["localhost:9092"],
        group_id="group1"
    )
    message_queue = []
    for message in consumer:
        while message_queue < 501:
            message_queue += message.value
        else:
            message_queue_json = json.dumps(message_queue)
            botoclient.put_object(
                body=json.dumps(message_queue_json),
                bucket='pinterestprojectpipeline',
                key='new_user_credentials.csv'
            )
            message_queue=[]
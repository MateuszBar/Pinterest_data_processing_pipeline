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
    counter = 1
    message_queue = []
    for message in consumer:
        decoded_message = json.loads(message.value.decode('utf-8'))
        message_queue.append(decoded_message)
        print(f"{len(message_queue)} messages in queue")
        if len(message_queue) > 299:
            print("sending messages to s3 bucket\n")
            botoclient.put_object(
                Body=json.dumps(message_queue),
                Bucket='pinterestprojectpipeline',
                Key=f'PinterestData{counter}.json'
            )
            message_queue.clear()
            print(f"counter = {counter}")
            counter += 1


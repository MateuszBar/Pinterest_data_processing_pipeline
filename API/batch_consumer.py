from time import sleep
from kafka import KafkaConsumer
import boto3
import json

#initialise s3 client
botoclient = boto3.client('s3')

if __name__ == "__main__":
    #initialise consumer
    print("starting batch consumer app")
    consumer = KafkaConsumer(
        'PinterestTopic',
        bootstrap_servers=["localhost:9092"],
        group_id="group1"
    )
    counter = 1
    #create empty message queue
    message_queue = []
    #for loop for each message in consumer
    for message in consumer:
        #decode message and add to queue
        decoded_message = json.loads(message.value.decode('utf-8'))
        message_queue.append(decoded_message)
        print(f"{len(message_queue)} messages in queue")
        #if queue is 300, send to s3 bucket
        if len(message_queue) > 299:
            print("sending messages to s3 bucket\n")
            botoclient.put_object(
                Body=json.dumps(message_queue),
                Bucket='pinterestprojectpipeline',
                Key=f'PinterestData{counter}.json'
            )
            #reset queue and print counter
            message_queue.clear()
            print(f"counter = {counter}")
            counter += 1


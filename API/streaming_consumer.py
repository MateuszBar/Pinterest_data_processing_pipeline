from email.header import decode_header
from kafka import KafkaConsumer
from time import sleep

def process_message(message):
    print(message)    
    
if __name__ == "__main__":
    print("starting streaming consumer app")
    consumer = KafkaConsumer(
        'PinterestTopic',
        bootstrap_servers=["localhost:9092"],
        group_id="group1"
    )
    
    for message in consumer:
        decoded_message = message.value.decode('utf-8')
        process_message(decoded_message)
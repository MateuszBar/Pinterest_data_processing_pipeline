from time import sleep
from kafka import KafkaConsumer
import threading
from queue import Queue, Empty

message_queue = Queue()


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        
    def run(self):
        consumer = KafkaConsumer(
            "user_signups",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        
        for message in consumer:
            self.process_message(message.value)
            
        consumer.close()
        
    def process_message(self, message):
        # Insert the message into buffer queue
        message_queue.put(message)
        
        
def process_messages():
    print("processing message in queue buffer")
    temp_messages = []
    
    try:
        while True:
            temp_messages.append(message_queue.get_nowait())

    except Empty:
        pass
      
    # Combine all messages into 1 call
    print(temp_messages)
    
if __name__ == "__main__":
    print("starting batch consumer worker")
    
    consumer = Consumer()
    consumer.daemon = True
    consumer.start()
    
    while True:
        process_messages()
        sleep(5)
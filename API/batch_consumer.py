from kafka import KafkaConsumer

batch_consumer = KafkaConsumer('PinterestTopic',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                )
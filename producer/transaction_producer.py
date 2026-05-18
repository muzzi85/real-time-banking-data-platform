from kafka import KafkaProducer

from faker import Faker

import json
import random
import time


fake = Faker()


producer = KafkaProducer(

    bootstrap_servers='localhost:9092',

    value_serializer=lambda v:
    json.dumps(v).encode('utf-8')
)


transaction_types = [

    "POS",
    "ATM",
    "ONLINE",
    "TRANSFER"
]

while True:

    transaction = {

        "customer_id":
            random.randint(1000, 5000),

        "customer_name":
            fake.name(),

        "transaction_type":
            random.choice(transaction_types),

        "amount":
            round(random.uniform(10, 5000), 2),

        "country":
            fake.country(),

        "timestamp":
            time.time()
    }

    producer.send(
        "banking-transactions",
        value=transaction
    )

    print(
        f"Produced: {transaction}"
    )

    time.sleep(2)
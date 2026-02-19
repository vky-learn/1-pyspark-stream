from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer = lambda v: json.dumps(v).encode('utf-8')
                         )
try:
    while True:
        
        msg = { "event_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            "id": random.randint(1, 100),
            "name": random.choice(['Alice', 'Bob', 'Charlie', 'David', 'Eve']),
            "location": random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
            "activity": random.choice(['login', 'logout', 'purchase', 'view'])
                }
        producer.send('events', value=msg)
        print(f"Sent: {msg}")
        time.sleep(5)
except KeyboardInterrupt:
    print("Stopping producer...")
finally:    
    producer.flush()
    producer.close()



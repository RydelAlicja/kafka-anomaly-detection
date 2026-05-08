from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    tx_id = f"TX{random.randint(1000, 9999)}"
    user_id = f"u{random.randint(1, 10):02d}" 
    return {
        "tx_id": tx_id,
        "user_id": user_id,
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(["Warszawa", "Kraków", "Gdańsk"]),
        "timestamp": datetime.now().isoformat()
    }


while True:
    tx = generate_transaction()
    producer.send('transactions', value=tx)
    producer.flush() 
    
    print(f"Wysłano: {tx['tx_id']} dla {tx['user_id']}")
    time.sleep(1) 

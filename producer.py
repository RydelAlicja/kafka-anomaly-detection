from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    tx_id = f"TX{random.randint(1000, 9999)}"
    user_id = f"u{random.randint(1, 10):02d}" # Mniejszy zakres (1-10) szybciej wywoła alert
    return {
        "tx_id": tx_id,
        "user_id": user_id,
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "store": random.choice(["Warszawa", "Kraków", "Gdańsk"]),
        "timestamp": datetime.now().isoformat()
    }

print("Producent ruszył... Wysyłam dane do Kafki.")

while True:
    tx = generate_transaction()
    # KLUCZOWE: wysyłamy do tematu 'transactions'
    producer.send('transactions', value=tx)
    # KLUCZOWE: wymuszamy wysłanie z bufora na serwer
    producer.flush() 
    
    print(f"Wysłano: {tx['tx_id']} dla {tx['user_id']}")
    time.sleep(0.2) # Przyspieszamy do 0.2s, żeby szybciej mieć alert na screenie

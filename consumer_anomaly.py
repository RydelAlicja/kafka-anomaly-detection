from kafka import KafkaConsumer
import json
from datetime import datetime
from collections import defaultdict


consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='total-reset-group-1', 
    auto_offset_reset='earliest',    
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_windows = defaultdict(list)

print("Anomalie:")

for message in consumer:
    tx = message.value
    u_id = tx['user_id']
    now = datetime.now()
    
    user_windows[u_id].append(now)
    
    user_windows[u_id] = [t for t in user_windows[u_id] if (now - t).total_seconds() <= 60]
    
    count = len(user_windows[u_id])
    
    print(f"User: {u_id} | Licznik (60s): {count}")
    

    if count > 3:
        print(f"Anomialia: ")
        print(f"Użytkownik {u_id} wykonał {count} transakcji w mniej niż 60 sekund")

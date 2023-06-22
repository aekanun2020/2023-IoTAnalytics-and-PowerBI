import csv
import json
import sys
from kafka import KafkaProducer

# รับค่า IP ที่ส่งมาจาก parameter
bootstrap_servers = sys.argv[1] + ':9092'

# กำหนดค่า Kafka topic
topic = 'quickstart-events'

# สร้าง Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         acks='all',
                         max_in_flight_requests_per_connection=1)

# อ่านข้อมูลจากไฟล์ CSV และเผยแพร่ลงใน Kafka topic
with open('2023-01-01-2023-04-30.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # ส่งข้อมูลลงใน Kafka topic
        future = producer.send(topic, value=row)
        try:
            record_metadata = future.get(timeout=30)
            print(f"ส่งข้อความสำเร็จ: topic = {record_metadata.topic}, partition = {record_metadata.partition}, offset = {record_metadata.offset}")
        except Exception as e:
            print(f"เกิดข้อผิดพลาดในการส่งข้อความ: {e}")

# ปิด Kafka producer
producer.close()


from kafka import KafkaConsumer
import json
import asyncio
from send_email import send_email
from generate_letter import generate_offer_letter
import os
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    "offer_topic",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

async def process(data):
    name = data["Name"]
    email = data["Email"]
    role = data["Role"]
    amount = data["Offer_amount"]
    start_date = data["Starting_date"]
    location = data["Location"]
    subject = "Your Offer Letter"
    message = f"Dear {name},\n\nWe are delighted to have you on board. Attached below is your offer letter.\n\nRegards,\nHR Team"


    if (data["has_passed"] == "yes"):
        doc_path = generate_offer_letter(name, role, amount, start_date, location)
        print("inside process")
        success = await send_email(email, subject, message, doc_path)
        if success:
            print(f"Email sent to {email}")
        else:
            print(f"Failed to send email to {email}")



print("Starting Kafka consumer...")
for msg in consumer:
    asyncio.run(process(msg.value))
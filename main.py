from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pandas as pd
import json
import os
import asyncio
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()



producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
'''
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",  
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
'''


def load_email_list():
    df = pd.read_excel("email_list.xlsx")
    return df.to_dict(orient="records")



async def send_email_message(person):
    personalized_message = f"""
Dear {person['Name']},

Congratulations!!

We are delighted to have you on board with us.

Role: {person['Role']}
Offer Amount: {person['Offer_amount']}
Start Date: {person['Starting_date']}
Location: {person['Location']}

Regards,
HR Team
"""

    payload = {
        "Email": person["Email"],
        "Name": person["Name"],
        "Role": person["Role"],
        "Offer_amount": person["Offer_amount"],
        "Starting_date": person["Starting_date"],
        "Location": person["Location"],
        "has_passed": person.get("has_passed", "no") ,
        "message": personalized_message
    }
    print("inside send email messsage")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: producer.send("offer_topic", value=payload))


@app.post("/send-emails")
async def send_bulk_emails():
    email_list = load_email_list()

    # Create tasks for all emails
    tasks = [send_email_message(person) for person in email_list]
    await asyncio.gather(*tasks)

    # Flush Kafka producer after sending all
    producer.flush()

    return JSONResponse({"status": "Emails queued successfully"})

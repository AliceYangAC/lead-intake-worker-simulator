from azure.servicebus import ServiceBusClient
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()
CONN = os.getenv("SERVICE_BUS_CONNECTION_STRING")
QUEUE = "leads"

client = ServiceBusClient.from_connection_string(CONN)

with client:
    receiver = client.get_queue_receiver(queue_name=QUEUE)
    with receiver:
        messages = receiver.peek_messages(max_message_count=10)
        for msg in messages:
            print("Message:", msg)

import time
import uuid
import random
from datetime import datetime
import psycopg2
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import os
from dotenv import load_dotenv

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "local",
    "host": "localhost",
    "port": 5432,
}

# Load environment variables from .env file
load_dotenv()


# For local development, we use the Azure Service Bus emulator connection string
SERVICE_BUS_CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STRING")
QUEUE_NAME = "leads"
INTERVAL_SECONDS = int(os.getenv("INTERVAL")) 


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def insert_lead(conn):
    lead_id = f"lead_{uuid.uuid4().hex[:6]}"
    fname = random.choice(["John", "Alice", "Maria", "David"])
    lname = random.choice(["Doe", "Smith", "Lee", "Patel"])
    email = f"{fname.lower()}.{lname.lower()}@example.com"
    phone = "123-456-7890"
    
    # Used AI for a variety of options; in production we might refer to vehicles with IDs and a vehicles table, but for testing we can keep it simple
    vehicle = random.choice([
        # Economy / Compact
        "2018 Honda Fit",
        "2020 Toyota Corolla",
        "2019 Hyundai Elantra",
        "2021 Kia Forte",
        "2017 Mazda3 Sport",

        # Sedans
        "2020 Honda Accord Touring",
        "2021 Toyota Camry XSE",
        "2019 Nissan Altima SV",
        "2022 Hyundai Sonata Hybrid",
        "2020 Volkswagen Jetta Highline",

        # SUVs / Crossovers
        "2021 Toyota RAV4 XLE",
        "2020 Honda CR-V EX-L",
        "2019 Mazda CX-5 Signature",
        "2022 Hyundai Tucson Preferred",
        "2021 Ford Escape Titanium",

        # Pickup Trucks
        "2020 Ford F-150 XLT",
        "2021 RAM 1500 Big Horn",
        "2019 Chevrolet Silverado LTZ",
        "2022 Toyota Tacoma TRD Sport",
        "2020 GMC Sierra Elevation",

        # EVs / Hybrids
        "2021 Tesla Model 3 Long Range",
        "2020 Nissan Leaf SV",
        "2022 Hyundai Ioniq 5 Preferred AWD",
        "2021 Toyota Prius Prime",
        "2022 Ford Mustang Mach-E Premium",

        # Luxury
        "2019 BMW 330i xDrive",
        "2020 Mercedes-Benz C300 4MATIC",
        "2021 Audi Q5 Technik",
        "2020 Lexus RX 350",
        "2018 Volvo XC90 Inscription",

        # Sports / Performance
        "2019 Ford Mustang GT",
        "2020 Subaru WRX STI",
        "2021 Chevrolet Camaro SS",
        "2018 Dodge Challenger R/T",
        "2022 Toyota GR86 Premium",

        # Budget / Older Vehicles
        "2012 Honda Civic LX",
        "2010 Toyota Camry LE",
        "2013 Ford Escape SE",
        "2011 Hyundai Santa Fe GLS",
        "2014 Mazda6 GS",

        # High-End / Aspirational
        "2021 Porsche Macan S",
        "2020 BMW X5 xDrive40i",
        "2019 Mercedes-Benz GLE 450",
        "2022 Range Rover Velar R-Dynamic",
        "2021 Tesla Model S Plaid",
    ])

    wants_email = random.choice([True, False])
    # Used AI to generate a variety of notes for context testing
    notes = random.choice([
        "Interested in availability",
        "Wants financing options",
        "Asked about trade-in",
        "Looking to schedule a test drive this week",
        "Wants to compare this model with similar vehicles",
        "Concerned about mileage and maintenance history",
        "Asking whether the vehicle has been in any accidents",
        "Wants to know total out-the-door pricing",
        "Checking if the vehicle is still under manufacturer warranty",
        "Interested in monthly payment estimates",
        "Wants to see interior photos or a video walkthrough",
        "Asking about delivery options for out-of-town buyers",
        "Wants to negotiate the listed price",
        "Inquiring about available color options",
        "Wants to confirm if the vehicle supports Apple CarPlay/Android Auto",
        "Asking whether winter tires are included",
        "Wants to know if the dealership accepts cryptocurrency",
        "Concerned about credit score and financing approval",
        "Asking if extended warranty packages are available",
        "Wants to reserve the vehicle with a deposit",
        "Inquiring about previous owner history",
        "Wants to know if the vehicle can tow a small trailer",
        "Asking about insurance cost estimates",
        "Wants to bring a mechanic to inspect the vehicle",
        "Looking for a family-friendly vehicle with good safety ratings",
        "Asking if the dealership offers student or military discounts",
        "Wants to upgrade from their current vehicle and needs recommendations",
        "Asking about hybrid vs. gas model differences",
        "Wants to confirm if the vehicle has remote start",
        "Looking for a quick purchase and wants paperwork prepared in advance",
    ])
    created_at = datetime.now()

    # with conn.cursor() as cur:
    #     cur.execute(
    #         """
    #         INSERT INTO leads (lead_id, fname, lname, email, phone, vehicle, wants_email, notes, created_at)
    #         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    #         """,
    #         (lead_id, fname, lname, email, phone, vehicle, wants_email, notes, created_at),
    #     )
    #     conn.commit()

    # print(f"[DB] Inserted lead: {lead_id}")
    return {
        "lead_id": lead_id,
        "fname": fname,
        "lname": lname,
        "email": email,
        "phone": phone,
        "vehicle": vehicle,
        "wants_email": wants_email,
        "notes": notes,
        "created_at": created_at.isoformat(),
    }


def create_conversation(conn, lead_id):
    conversation_id = f"conv_{uuid.uuid4().hex[:6]}"
    now = datetime.now()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO conversations (conversation_id, lead_id, status, created_at, last_updated)
            VALUES (%s,%s,%s,%s,%s)
            """,
            (conversation_id, lead_id, 1, now, now),
        )
        conn.commit()

    print(f"[DB] Created conversation: {conversation_id}")
    return conversation_id


def publish_to_service_bus(payload):
    client = ServiceBusClient.from_connection_string(conn_str=SERVICE_BUS_CONNECTION_STR)
    sender = client.get_queue_sender(queue_name=QUEUE_NAME)

    with sender:
        message = ServiceBusMessage(str(payload))
        sender.send_messages(message)

    print(f"[SB] Published message for lead: {payload['lead_id']}")


def simulate_worker():
    print("Starting lead simulation worker...")
    print(f"Interval: {INTERVAL_SECONDS} seconds\n")

    while True:
        try:
            conn = None
            # conn = get_db_connection()

            # Insert lead
            lead = insert_lead(conn)
            
            # For testing, we can generate a conversation ID even if we aren't inserting into the DB, since the Service Bus message will include it and the downstream consumer can handle it accordingly. In production, we would want to ensure the conversation is created in the DB first.
            conversation_id = f"conv_{uuid.uuid4().hex[:6]}"

            # Create conversation if needed
            # conversation_id = None
            # if lead["wants_email"]:
            #     conversation_id = create_conversation(conn, lead["lead_id"])

            # Publish to Service Bus
            publish_payload = {
                "lead_id": lead["lead_id"],
                "conversation_id": conversation_id,
                "fname": lead["fname"],
                "lname": lead["lname"],
                "email": lead["email"],
                "vehicle": lead["vehicle"],
                "notes": lead["notes"],
                "created_at": lead["created_at"],
            }

            publish_to_service_bus(publish_payload)

            #conn.close()

        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    simulate_worker()

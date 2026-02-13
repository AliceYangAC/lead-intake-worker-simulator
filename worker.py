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

# How often to generate a new lead (in seconds)
INTERVAL_SECONDS = int(os.getenv("INTERVAL")) 

# Ratio of notes that include PII (between 0 and 1)
PII_RATIO = float(os.getenv("NOTES_PII_RATIO", "0.5"))
PII_RATIO = max(0.0, min(1.0, PII_RATIO)) 


VEHICLE_OPTIONS = {
    "Economy / Compact": [
        ("Honda", "Fit", "LX"),
        ("Toyota", "Corolla", "LE"),
        ("Hyundai", "Elantra", "Preferred"),
        ("Kia", "Forte", "EX"),
        ("Mazda", "Mazda3", "Sport"),
        ("Volkswagen", "Golf", "Trendline"),
        ("Ford", "Focus", "SE"),
        ("Nissan", "Sentra", "SV"),
    ],

    "Sedans": [
        ("Honda", "Accord", "Touring"),
        ("Toyota", "Camry", "XSE"),
        ("Nissan", "Altima", "SV"),
        ("Hyundai", "Sonata", "Hybrid"),
        ("Volkswagen", "Jetta", "Highline"),
        ("Subaru", "Legacy", "Limited"),
        ("Mazda", "Mazda6", "GS"),
        ("Kia", "Stinger", "GT-Line"),
    ],

    "SUVs / Crossovers": [
        ("Toyota", "RAV4", "XLE"),
        ("Honda", "CR-V", "EX-L"),
        ("Mazda", "CX-5", "Signature"),
        ("Hyundai", "Tucson", "Preferred"),
        ("Ford", "Escape", "Titanium"),
        ("Subaru", "Forester", "Touring"),
        ("Nissan", "Rogue", "SL"),
        ("Chevrolet", "Equinox", "LT"),
    ],

    "Pickup Trucks": [
        ("Ford", "F-150", "XLT"),
        ("RAM", "1500", "Big Horn"),
        ("Chevrolet", "Silverado", "LTZ"),
        ("Toyota", "Tacoma", "TRD Sport"),
        ("GMC", "Sierra", "Elevation"),
        ("Nissan", "Frontier", "PRO-4X"),
        ("Ford", "Ranger", "Lariat"),
        ("Honda", "Ridgeline", "Black Edition"),
    ],

    "EVs / Hybrids": [
        ("Tesla", "Model 3", "Long Range"),
        ("Nissan", "Leaf", "SV"),
        ("Hyundai", "Ioniq 5", "Preferred AWD"),
        ("Toyota", "Prius Prime", "Upgrade"),
        ("Ford", "Mustang Mach-E", "Premium"),
        ("Kia", "EV6", "Wind AWD"),
        ("Volkswagen", "ID.4", "Pro"),
        ("Chevrolet", "Bolt", "Premier"),
    ],

    "Luxury": [
        ("BMW", "330i", "xDrive"),
        ("Mercedes-Benz", "C300", "4MATIC"),
        ("Audi", "Q5", "Technik"),
        ("Lexus", "RX 350", "Luxury"),
        ("Volvo", "XC90", "Inscription"),
        ("Acura", "RDX", "A-Spec"),
        ("Infiniti", "QX50", "Sensory"),
        ("Genesis", "GV70", "Advanced"),
    ],

    "Sports / Performance": [
        ("Ford", "Mustang", "GT"),
        ("Subaru", "WRX", "STI"),
        ("Chevrolet", "Camaro", "SS"),
        ("Dodge", "Challenger", "R/T"),
        ("Toyota", "GR86", "Premium"),
        ("Nissan", "370Z", "Sport"),
        ("BMW", "M2", "Competition"),
        ("Porsche", "718 Cayman", "Base"),
    ],

    "Budget / Older Vehicles": [
        ("Honda", "Civic", "LX"),
        ("Toyota", "Camry", "LE"),
        ("Ford", "Escape", "SE"),
        ("Hyundai", "Santa Fe", "GLS"),
        ("Mazda", "Mazda6", "GS"),
        ("Chevrolet", "Impala", "LT"),
        ("Nissan", "Altima", "S"),
        ("Kia", "Rio", "LX"),
    ],

    "High-End / Aspirational": [
        ("Porsche", "Macan", "S"),
        ("BMW", "X5", "xDrive40i"),
        ("Mercedes-Benz", "GLE", "450"),
        ("Land Rover", "Range Rover Velar", "R-Dynamic"),
        ("Tesla", "Model S", "Plaid"),
        ("Audi", "A7", "Prestige"),
        ("Lexus", "LS 500", "Executive"),
        ("Maserati", "Levante", "GranSport"),
    ],
}

DEALERSHIP_NAMES = [
    "AutoNation {city}",
    "{make} of {city}",
    "{city} Auto Mall",
    "{province} Motor Group",
    "{make} Centre {city}",
    "{city} Premium Autos",
    "{make} & More",
    "{city} Car House",
    "{province} Auto Plaza",
    "{make} Direct {city}",
]

CITIES = [
    "Toronto", "Ottawa", "Vancouver", "Calgary", "Edmonton",
    "Montreal", "Winnipeg", "Halifax", "Regina", "Saskatoon"
]

PROVINCES = [
    "ON", "BC", "AB", "QC", "MB", "NS", "SK"
]


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def generate_dealership(dealer_id, make):
    city = random.choice(CITIES)
    province = random.choice(PROVINCES)

    name_template = random.choice(DEALERSHIP_NAMES)
    dealership_name = name_template.format(make=make, city=city, province=province)

    return {
        "name": dealership_name,
        "email": f"contact@{dealership_name.replace(' ', '').lower()}.com",
        "phone": f"555-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "address1": f"{random.randint(10,999)} Main St",
        "address2": "",
        "city": city,
        "province": province,
        "postal_code": f"{random.choice('ABCEGHJ')}{random.randint(1,9)}{random.choice('ABCEGHJ')} {random.randint(1,9)}{random.choice('ABCEGHJ')}{random.randint(1,9)}"
    }

def generate_vehicle():
    # Pick a random category, then a random vehicle tuple
    category = random.choice(list(VEHICLE_OPTIONS.keys()))
    make, model, trim = random.choice(VEHICLE_OPTIONS[category])

    dealer_id = random.randint(1, 200)

    # Mock dealership lookup
    dealership = generate_dealership(dealer_id, make)
    
    year = random.randint(2000, 2026)

    return {
        "dealership": dealership,
        "status": random.choice([0, 1]),  # new/used
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "mileage": f"{random.randint(20_000, 250_000)} km",
        "transmission": random.choice(["Automatic", "Manual", "CVT"]),
        "comments": random.choice([
            "",
            "",
            "",
            "Clean CarFax, one owner.",
            "Dealer maintained.",
            "Low mileage for the year.",
            "Certified pre-owned.",
            "Excellent condition.",
            "Minor cosmetic wear.",
            "Fully loaded with premium package.",
            "Recently serviced and detailed.",
        ]),
        "category": category  
    }

def generate_notes(fname, lname, email, phone):
    normal_notes = [
        "Hi, I'm just checking if this vehicle is still available.",
        "Can you tell me what financing options are available for this car?",
        "I'm considering trading in my current vehicle — can you estimate its value toward this one?",
        "I'd like to schedule a test drive for this car sometime this week.",
        "I'm comparing this model with a few others — can you share more details?",
        "Can you provide the full maintenance history for this vehicle?",
        "Has this specific vehicle ever been in an accident?",
        "What would be the total out-the-door price for this car?",
        "Is the manufacturer warranty still active on this vehicle?",
        "What would monthly payments look like for this car?",
        "Do you have additional interior photos or a video walkthrough of this vehicle?",
        "Do you offer delivery for this vehicle? I'm not located nearby.",
        "Is the price on this listing negotiable?",
        "What color options are available for this model?",
        "Does this vehicle support Apple CarPlay or Android Auto?",
        "Are winter tires included with this car?",
        "Do you accept cryptocurrency for purchasing this vehicle?",
        "My credit isn't perfect — can I still get approved for this car?",
        "Do you offer extended warranty packages for this vehicle?",
        "Can I put down a deposit to hold this car?",
        "Do you know anything about the previous owner of this vehicle?",
        "Is this vehicle capable of towing a small trailer?",
        "Do you know what insurance might cost for this model?",
        "Can I bring my mechanic to inspect this vehicle?",
        "I'm looking for something safe for my family — how does this model rate?",
        "Do you offer student or military discounts on this vehicle?",
        "I'm upgrading from my current car — would this be a good fit?",
        "What's the difference between the hybrid and gas versions of this model?",
        "Does this vehicle come with remote start?",
        "I'm hoping to buy quickly — can we prepare the paperwork for this car ahead of time?",
    ]

    # Synthetic PII 
    synthetic_pii = [
        f"My other email is test.user{random.randint(10,99)}@example.com if you need to send more details about this car.",
        f"You can call me back at 555-{random.randint(100,999)}-{random.randint(1000,9999)} about this vehicle.",
        f"My temporary address is {random.randint(10,999)} Maple Street if you need it for the quote.",
        f"I might register the car under my partner's name, Alex Johnson — is that okay for this vehicle?",
        f"My trade-in VIN is 1HGCM82633A{random.randint(100000,999999)} — can you estimate its value toward this car?",
        f"My driver's license number is D{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(100,999)} if needed for the test drive.",
        f"My plate number is ABCD {random.randint(100,999)} if you need it for insurance on this vehicle.",
        f"Please send the paperwork for this car to temp.email{random.randint(1,9)}@mailinator.com.",
    ]

    # Lead-specific PII
    self_pii_notes = [
        f"Hi, it's {fname} {lname}. I'm following up about this vehicle.",
        f"You can email me at {email} with more details about this car.",
        f"My phone number is {phone} if you need to reach me about this vehicle.",
        f"Hey, this is {fname}. Can someone call me back about this car?",
        f"I prefer texts — {phone} is the best number if you have updates on this vehicle.",
        f"Please send the full quote for this car to {email}.",
        f"I'm {fname} {lname}, just checking on the status of my inquiry about this vehicle.",
        f"Can you confirm the appointment time for viewing this car? You can reach me at {phone}.",
        f"Feel free to send photos or documents for this vehicle to {email}.",
    ]

    pii_pool = synthetic_pii + self_pii_notes
    weighted_pool = []

    # Number of entries to generate for each category
    # Using len(normal_notes) keeps the distribution stable
    total = len(normal_notes)

    normal_count = int(total * (1 - PII_RATIO))
    pii_count = int(total * PII_RATIO)

    # Add normal-only notes
    weighted_pool.extend(random.sample(normal_notes, normal_count))

    # Add normal+PII notes
    for note in random.sample(normal_notes, pii_count):
        weighted_pool.append(note + " " + random.choice(pii_pool))

    # Fallback if ratio is weird (eg, 0 or 1)
    if not weighted_pool:
        weighted_pool.append(random.choice(normal_notes))

    return random.choice(weighted_pool)



def insert_lead(conn):
    lead_id = random.randint(1, 999)
    fname = random.choice(["John", "Alice", "Maria", "David"])
    lname = random.choice(["Doe", "Smith", "Lee", "Patel"])
    email = f"{fname.lower()}.{lname.lower()}@example.com"
    phone = "123-456-7890"
    notes = generate_notes(fname, lname, email, phone)
    # For testing, we can generate a vehicle object even if we aren't inserting into the DB, since the Service Bus message will include it and the downstream consumer can handle it accordingly. In production, we would want to ensure the vehicle is created in the DB first and we have a valid vehicle_id to reference.
    vehicle = generate_vehicle()
    
    status = 0 # New lead
    wants_email = random.choice([True, False])
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
        "status": status,
        "vehicle": vehicle,
        "wants_email": wants_email,
        "notes": notes,
        "created_at": created_at.isoformat(),
    }


def create_conversation(conn, lead_id):
    conversation_id = random.randint(1, 999)
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
            conversation_id = random.randint(1, 999) if lead["wants_email"] else None

            # Create conversation if needed
            # conversation_id = None
            # if lead["wants_email"]:
            #     conversation_id = create_conversation(conn, lead["lead_id"])

            # Publish to Service Bus if we have a conversation ID (indicating the lead wants emails and we want to process it)
            if conversation_id:
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

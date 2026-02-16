# Lead Intake Worker Simulator

## Example Message

```json
{
    "lead": {
        "id": "lead_c47f91a953",
        "fname": "Alice",
        "lname": "Smith",
        "email": "alice.smith@example.com",
        "phone": "555-307-8655",
        "status": 0,
        "wants_email": true,
        "notes": "My credit isn't perfect — can I still get approved for this car?",
        "timestamp": "2026-02-16T10:30:54.198014"
    },
    "vehicle": {
        "id": "vehicle_599cae2001",
        "status": 1,
        "year": 2020,
        "make": "Kia",
        "model": "Stinger",
        "trim": "GT-Line",
        "mileage": "146612 km",
        "transmission": "Manual",
        "comments": "Low mileage for the year."
    },
    "dealership": {
        "id": "dealer_82f7ad2124",
        "name": "Kia Direct Montreal",
        "email": "contact@kiadirectmontreal.com",
        "phone": "555-335-9346",
        "address1": "376 Main St",
        "address2": "",
        "city": "Montreal",
        "province": "NS",
        "postal_code": "B9B 8H3"
    },
    "conversationId": "conv_66e8be1f55"
}
```

## Set up `.venv`

```bash
python -m venv .venv
```

This creates a folder named `.venv` containing your isolated Python environment.

**macOS / Linux**

```bash
source .venv/bin/activate
```

**Windows PowerShell**

```powershell
.venv\Scripts\Activate
```

You should now see `(.venv)` at the start of your terminal prompt.
With the environment activated, install everything from `requirements.txt`:

```bash
pip install -r requirements.txt
```

Your environment is now ready to run:

- `python worker.py`  
- `python peek.py`  
- `python purge.py`

## Set up your `.env` file

Copy the provided template:

```bash
cp .env.copy .env
```

Then open `.env` and update the following values:

### **`CONFIG_PATH`**
Set this to the **absolute path** of your `config.json` file.  
Example:

```
CONFIG_PATH=/Users/alice/projects/lead-intake-worker-simulator/config.json
```

### **`SERVICE_BUS_CONNECTION_STRING`**
The included connection string is the publicly available one for the emulator. No changes need to be made.

### **`INTERVAL`**
Controls how often the worker publishes new leads:

```
INTERVAL=10
```

---

## Start the Service Bus Emulator

From the project root:

```bash
docker compose up -d
```

This launches:

- SQL Server (required by the emulator)
- Azure Service Bus Emulator
- Loads your `config.json` (including the `leads` queue)

---

## Run the worker locally

With the emulator running, start the worker:

```bash
py worker.py
```

You should see:

```
Starting lead simulation worker...
Interval: 10 seconds

[SB] Published message for lead: lead_ab12cd
```

The worker will continue publishing new leads every `INTERVAL` seconds.

---

## Peek inside the queue (optional)

Use the included `peek.py` script to inspect messages without consuming them:

```bash
py peek.py
```

## Purge the queue (optional)

Use the included `purge.py` script to purge all messages in the queue:

```bash
py purge.py
```
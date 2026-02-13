# Lead Intake Worker Simulator

## Example Message

```bash
Message: {'lead_id': 73366, 'conversation_id': '410', 'fname': 'David', 'lname': 'Patel', 'email': 'david.patel@example.com', 'vehicle': {'vehicle_id': 382591, 'dealer_id': 148, 'stock_id': 'STK-8345', 'status': 0, 'year': 2013, 'vin': '6B7C7237-7D98-4DC', 'make': 'Nissan', 'model': 'Altima', 'trim': 'S', 'mileage': '179224 km', 'transmission': 'Manual', 'comments': 'Fully loaded with premium package.', 'category': 'Budget / Older Vehicles'}, 'notes': 'Asked about trade-in', 'created_at': '2026-02-13T11:01:32.083453'}
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
python worker.py
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
python peek.py
```

This script connects to the emulator and prints the next batch of messages currently in the `leads` queue.

# QAG System Monitor (Power BI → CSV → Alerts)

Automates downloading two Power BI exports (CSV), runs health checks, generates grouped JSON alerts, and optionally sends alerts to a Push API.  
Logs are saved under `logs/YYYY-MM-DD/HH-MM-SS.txt`.

## What it does
- Downloads:
  - Health Algo Daily Statistics (CSV)
  - Health Algo Commands Daily Statistics (CSV)
- Skips rows that contain: `not available yet`
- Runs validations (status, mismatches, stale reporting, LED started late, LED state mismatch)
- Groups alerts by company + reason (tags are grouped)
- Optional: sends alerts to Push API (disabled by default)

## Setup
1. Create a virtual environment (recommended)
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
Create .env (copy from .env.example) and fill values.
Run
python main.py
Notes
reports/ and logs/ are generated automatically and are ignored by git.
SEND_ALERTS=false by default. Set to true only when your Push API credentials are configured.
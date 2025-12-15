# Email Automation Scripts

This folder contains scripts for automating email notifications based on Zoho Projects and Sprints time logs, and leave handling.

## Files

- **NEXT_STEPS.py** - Main Python script for fetching time logs and sending automated emails
- **Leave Handling.sql** - SQL query for fetching leave data
- **Zoho Projects TimeLog.sql** - SQL query for Projects time log data
- **Sprints TimeLog.sql** - SQL query for Sprints time log data

## Setup Instructions

### 1. Configure Credentials

Before running the script, you need to update the following credentials in `NEXT_STEPS.py`:

```python
# Zoho Analytics API Credentials
CLIENT_ID = "YOUR_CLIENT_ID_HERE"
CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE"
REFRESH_TOKEN = "YOUR_REFRESH_TOKEN_HERE"
ORG_ID = "YOUR_ORG_ID_HERE"

# Email SMTP Configuration
EMAIL_PASS = "YOUR_EMAIL_PASSWORD_HERE"

# Claude API (for AI summaries)
CLAUDE_API_KEY = "YOUR_CLAUDE_API_KEY_HERE"
```

### 2. Install Dependencies

```bash
pip install requests
```

### 3. Test Mode

By default, `TEST_MODE = True` to prevent accidentally sending emails to all users. 
- When testing, emails are sent to `TEST_EMAIL` address only
- Set `TEST_MODE = False` when ready for production

### 4. Run the Script

```bash
python3 NEXT_STEPS.py
```

## Configuration

Update the following paths and IDs in the script as needed:
- Workspace IDs for Projects, Sprints, and Leave data
- View IDs for each workspace
- Logo file path: `LOGO_PNG_PATH`
- Token cache path: `TOKEN_CACHE_PATH`

## Features

- Fetches time log data from Zoho Projects and Sprints
- Combines time logs by user email
- Checks for leave status
- Generates AI-powered summaries using Claude
- Sends formatted HTML emails with weekly breakdown
- Provides recommendations for meeting hour targets

## Contact

For issues or questions, contact the HR team at Annam AI.

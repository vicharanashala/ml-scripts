#!/usr/bin/env python3
"""
NEXT_STEPS.py — All values hardcoded (no env vars).
Place this on your server at /home/adityabmv/zoho_scripts/NEXT_STEPS.py and run:
  python3 /home/adityabmv/zoho_scripts/NEXT_STEPS.py
Note: TEST_MODE is True by default to avoid accidentally emailing everyone.
Change TEST_MODE = False to send real emails.
"""

import requests
import time
import csv
import io
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
import math
import os
import html
import json
import pathlib
import mimetypes

# ---------------------- HARDCODED CONFIG ----------------------
# Kshitij Pandey's credentials (Self Client)
CLIENT_ID = "YOUR_CLIENT_ID_HERE"
CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE"
REFRESH_TOKEN = "YOUR_REFRESH_TOKEN_HERE"
ORG_ID = "YOUR_ORG_ID_HERE"

# Access token is refreshed automatically when empty or expired
ACCESS_TOKEN = ""

# Workspace and View IDs
PROJECTS_WORKSPACE_ID = "461220000000134964"  # For Projects time log data
PROJECTS_VIEW_ID = "461220000000226002"       # For Projects time log data

SPRINTS_WORKSPACE_ID = "461220000000436002"   # For Sprints time log data
SPRINTS_VIEW_ID = "461220000000440407"        # For Sprints time log data

LEAVE_WORKSPACE_ID = "461220000000137931"  # For leave data (from your URL)
LEAVE_VIEW_ID = "461220000000327006"       # For leave data (from your URL)

# Email / SMTP (hardcoded)
SMTP_SERVER = "smtppro.zoho.in"
SMTP_PORT = 465
EMAIL_USER = "zohosyncsage@annam.ai"
EMAIL_PASS = "YOUR_EMAIL_PASSWORD_HERE"

# Safety/test
TEST_MODE = True             # set to False when ready to send real emails

TEST_EMAIL = "kshitij.pandey@annam.ai"

# Logo files (hardcoded paths)
LOGO_PNG_PATH = "/home/adityabmv/zoho_scripts/logo.png"

# Claude optional (hardcode blank)
CLAUDE_API_KEY = "YOUR_CLAUDE_API_KEY_HERE"
CLAUDE_MODEL = "claude-sonnet-4-5-20250929"

# Token cache path (hardcoded)
TOKEN_CACHE_PATH = "/home/adityabmv/.zoho_token.json"
# ----------------------------------------------------------------

# ---------------- Token caching & refresh ----------------
def _load_cached_token():
    try:
        if not os.path.exists(TOKEN_CACHE_PATH):
            return None
        with open(TOKEN_CACHE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if "access_token" in data and "expires_at" in data:
            return data
    except Exception:
        pass
    return None

def _save_cached_token(access_token, expires_in_seconds):
    try:
        expires_at = int(datetime.now(tz=timezone.utc).timestamp()) + int(expires_in_seconds) - 60
        payload = {"access_token": access_token, "expires_at": expires_at}
        tmp = TOKEN_CACHE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        os.replace(tmp, TOKEN_CACHE_PATH)
        try:
            os.chmod(TOKEN_CACHE_PATH, 0o600)
        except Exception:
            pass
    except Exception as e:
        print("⚠️ Failed to save token cache:", e)

def get_access_token(force_refresh=False):
    global ACCESS_TOKEN
    if ACCESS_TOKEN and not force_refresh:
        return ACCESS_TOKEN

    if not force_refresh:
        cached = _load_cached_token()
        if cached:
            now_ts = int(datetime.now(tz=timezone.utc).timestamp())
            if cached.get("expires_at", 0) > now_ts + 5:
                ACCESS_TOKEN = cached["access_token"]
                print("✅ Loaded access token from cache.")
                return ACCESS_TOKEN

    url = "https://accounts.zoho.in/oauth/v2/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token"
    }
    headers = {"User-Agent": "NEXT_STEPS/1.0"}
    try:
        r = requests.post(url, data=payload, headers=headers, timeout=20)
    except Exception as e:
        print("⚠️ Token refresh request failed:", e)
        return None

    if r.status_code == 200:
        try:
            data = r.json()
            token = data.get("access_token")
            expires_in = data.get("expires_in", 3600)
            if token:
                ACCESS_TOKEN = token
                _save_cached_token(token, expires_in)
                print("✅ Access token refreshed and cached.")
                return ACCESS_TOKEN
            else:
                print("⚠️ Refresh response missing access_token:", data)
                return None
        except Exception as e:
            print("⚠️ Failed to parse token refresh response:", e, r.text[:400])
            return None
    else:
        print("⚠️ Token refresh failed:", r.status_code, r.text[:800])
        return None

# ---------------- HTTP helpers for create_export_job ----------------
def create_export_job(access_token, workspace_id, view_id):
    url = f"https://analyticsapi.zoho.in/restapi/v2/bulk/workspaces/{workspace_id}/views/{view_id}/data"
    params = {"CONFIG": '{"responseFormat":"csv"}'}

    def attempt(auth_header_value):
        headers = {
            "ZANALYTICS-ORGID": ORG_ID,
            "Authorization": auth_header_value
        }
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
        except Exception as e:
            print("⚠️ create_export_job request exception:", e)
            return None, None
        print("DEBUG: create_export_job status_code:", r.status_code)
        try:
            token_part = auth_header_value.split()[-1]
            print(f"DEBUG: used auth header type '{auth_header_value.split()[0]}' token-length={len(token_part)}")
        except Exception:
            pass
        body = r.text or ""
        if r.status_code != 200 or "INVALID_OAUTHTOKEN" in body:
            print("DEBUG: create_export_job response body (truncated 1000 chars):")
            print(body[:1000])
        return r, body

    r, body = attempt(f"Bearer {access_token}")
    if r is not None and r.status_code == 200 and "INVALID_OAUTHTOKEN" not in (body or ""):
        try:
            data = r.json()
            job_id = data.get("data", {}).get("jobId")
            return job_id
        except Exception:
            print("⚠️ create_export_job: failed to parse JSON from first successful response.")
            return None

    print("INFO: first attempt failed - trying 'Zoho-oauthtoken' header as fallback.")
    r2, body2 = attempt(f"Zoho-oauthtoken {access_token}")
    if r2 is not None and r2.status_code == 200 and "INVALID_OAUTHTOKEN" not in (body2 or ""):
        try:
            data2 = r2.json()
            job_id = data2.get("data", {}).get("jobId")
            return job_id
        except Exception:
            print("⚠️ create_export_job: failed to parse JSON from fallback response.")
            return None

    print("⚠️ create_export_job failed after both attempts. Last response (truncated):")
    if body2:
        print(body2[:2000])
    elif body:
        print(body[:2000])
    if body and "INVALID_OAUTHTOKEN" in body:
        raise ValueError("INVALID_OAUTHTOKEN")
    if body2 and "INVALID_OAUTHTOKEN" in body2:
        raise ValueError("INVALID_OAUTHTOKEN")
    return None

def wait_for_job_completion(access_token, job_id, workspace_id, tries=20, wait_s=5):
    url = f"https://analyticsapi.zoho.in/restapi/v2/bulk/workspaces/{workspace_id}/exportjobs/{job_id}"
    headers = {
        "ZANALYTICS-ORGID": ORG_ID,
        "Authorization": f"Bearer {access_token}"
    }
    for i in range(tries):
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            print("⚠️ Error checking job status:", r.text)
            time.sleep(wait_s)
            continue
        data = r.json().get("data", {})
        status = data.get("jobStatus")
        print(f"⏳ Job status: {status}")
        if status == "JOB COMPLETED":
            return data.get("downloadUrl")
        time.sleep(wait_s)
    print("⚠️ Job did not complete in time.")
    return None

def fetch_csv_data(access_token, download_url):
    headers = {
        "ZANALYTICS-ORGID": ORG_ID,
        "Authorization": f"Bearer {access_token}",
        "CONFIG": '{"responseFormat":"json"}'
    }
    r = requests.get(download_url, headers=headers, timeout=30)
    if r.status_code == 200:
        return r.text
    else:
        print("⚠️ Failed to fetch CSV data:", r.status_code, r.text)
        return None

# ---------------- Time Log Data Fetching & Combining ----------------
def fetch_timelog_data(access_token, workspace_id, view_id, source_name):
    """Fetch time log data from a specific workspace/view"""
    try:
        job_id = create_export_job(access_token, workspace_id, view_id)
        if not job_id:
            print(f"❌ Could not create {source_name} export job.")
            return None
        
        print(f"🚀 {source_name} export job created: {job_id}")
        download_url = wait_for_job_completion(access_token, job_id, workspace_id)
        if not download_url:
            print(f"❌ No download URL available for {source_name}.")
            return None
            
        csv_text = fetch_csv_data(access_token, download_url)
        if not csv_text:
            print(f"❌ No {source_name} CSV fetched.")
            return None
            
        print(f"✅ Fetched {source_name} data successfully")
        return csv_text
        
    except Exception as e:
        print(f"⚠️ Error fetching {source_name} data: {e}")
        return None

def combine_timelog_data(projects_csv, sprints_csv):
    """Combine Projects and Sprints time log data by user email"""
    combined_data = {}
    
    # Parse Projects data
    if projects_csv:
        csv_reader = csv.DictReader(io.StringIO(projects_csv))
        for row in csv_reader:
            email = row.get("User Email", "").strip().lower()
            if email:
                combined_data[email] = {
                    "User Name": row.get("User Name", "").strip(),
                    "User Email": email,
                    "TotalSeconds_Raw": safe_float(row.get("TotalSeconds_Raw", 0)),
                    "Total Hours Logged Today": row.get("Total Hours Logged Today", "0 hr 0 mins"),
                    "Time Log Notes": row.get("Time Log Notes", ""),
                    "Week 1 (1–7)": safe_float(row.get("Week 1 (1–7)", 0)),
                    "Week 2 (8–14)": safe_float(row.get("Week 2 (8–14)", 0)),
                    "Week 3 (15–21)": safe_float(row.get("Week 3 (15–21)", 0)),
                    "Week 4 (22–28)": safe_float(row.get("Week 4 (22–28)", 0)),
                    "Week 5 (29–35)": safe_float(row.get("Week 5 (29–35)", 0)),
                    "Week 6 (36–42)": safe_float(row.get("Week 6 (36–42)", 0)),
                    "Week 7 (43–49)": safe_float(row.get("Week 7 (43–49)", 0)),
                    "Week 8 (50–56)": safe_float(row.get("Week 8 (50–56)", 0)),
                    "Monthly Total Hours": safe_float(row.get("Monthly Total Hours", 0))
                }
    
    # Add/merge Sprints data
    if sprints_csv:
        csv_reader = csv.DictReader(io.StringIO(sprints_csv))
        for row in csv_reader:
            email = row.get("User Email", "").strip().lower()
            if not email:
                continue
                
            sprints_seconds = safe_float(row.get("TotalSeconds_Raw", 0))
            sprints_notes = row.get("Time Log Notes", "")
            
            if email in combined_data:
                # User exists in Projects - add Sprints data
                combined_data[email]["TotalSeconds_Raw"] += sprints_seconds
                
                # Combine notes
                if sprints_notes:
                    if combined_data[email]["Time Log Notes"]:
                        combined_data[email]["Time Log Notes"] += "<<<ENTRY>>>" + sprints_notes
                    else:
                        combined_data[email]["Time Log Notes"] = sprints_notes
                
                # Add weekly hours
                combined_data[email]["Week 1 (1–7)"] += safe_float(row.get("Week 1 (1–7)", 0))
                combined_data[email]["Week 2 (8–14)"] += safe_float(row.get("Week 2 (8–14)", 0))
                combined_data[email]["Week 3 (15–21)"] += safe_float(row.get("Week 3 (15–21)", 0))
                combined_data[email]["Week 4 (22–28)"] += safe_float(row.get("Week 4 (22–28)", 0))
                combined_data[email]["Week 5 (29–35)"] += safe_float(row.get("Week 5 (29–35)", 0))
                combined_data[email]["Week 6 (36–42)"] += safe_float(row.get("Week 6 (36–42)", 0))
                combined_data[email]["Week 7 (43–49)"] += safe_float(row.get("Week 7 (43–49)", 0))
                combined_data[email]["Week 8 (50–56)"] += safe_float(row.get("Week 8 (50–56)", 0))
                combined_data[email]["Monthly Total Hours"] += safe_float(row.get("Monthly Total Hours", 0))
                
                # Recalculate today's hours display
                total_seconds = combined_data[email]["TotalSeconds_Raw"]
                combined_data[email]["Total Hours Logged Today"] = format_hours_decimal_to_hrmin(total_seconds / 3600.0)
                
            else:
                # User only in Sprints - add as new entry
                combined_data[email] = {
                    "User Name": row.get("User Name", "").strip(),
                    "User Email": email,
                    "TotalSeconds_Raw": sprints_seconds,
                    "Total Hours Logged Today": row.get("Total Hours Logged Today", "0 hr 0 mins"),
                    "Time Log Notes": sprints_notes,
                    "Week 1 (1–7)": safe_float(row.get("Week 1 (1–7)", 0)),
                    "Week 2 (8–14)": safe_float(row.get("Week 2 (8–14)", 0)),
                    "Week 3 (15–21)": safe_float(row.get("Week 3 (15–21)", 0)),
                    "Week 4 (22–28)": safe_float(row.get("Week 4 (22–28)", 0)),
                    "Week 5 (29–35)": safe_float(row.get("Week 5 (29–35)", 0)),
                    "Week 6 (36–42)": safe_float(row.get("Week 6 (36–42)", 0)),
                    "Week 7 (43–49)": safe_float(row.get("Week 7 (43–49)", 0)),
                    "Week 8 (50–56)": safe_float(row.get("Week 8 (50–56)", 0)),
                    "Monthly Total Hours": safe_float(row.get("Monthly Total Hours", 0))
                }
    
    print(f"✅ Combined data for {len(combined_data)} users (Projects + Sprints)")
    return combined_data

# ---------------- Leave Data Fetching ----------------
def fetch_leave_data(access_token):
    """Fetch leave data from the Leave Handling view"""
    try:
        # Create export job for leave data using LEAVE_WORKSPACE_ID
        job_id = create_export_job(access_token, LEAVE_WORKSPACE_ID, LEAVE_VIEW_ID)
        if not job_id:
            print("❌ Could not create leave data export job.")
            return {}
        
        print("🚀 Leave export job created:", job_id)
        download_url = wait_for_job_completion(access_token, job_id, LEAVE_WORKSPACE_ID)
        if not download_url:
            print("❌ No download URL available for leave data.")
            return {}
            
        csv_text = fetch_csv_data(access_token, download_url)
        if not csv_text:
            print("❌ No leave CSV fetched.")
            return {}
            
        # Parse leave data
        leave_data = {}
        csv_reader = csv.DictReader(io.StringIO(csv_text))
        for row in csv_reader:
            email = row.get("Email", "").strip().lower()
            status = row.get("Attendance Status", "").strip()
            leave_type = row.get("Leave Type", "").strip()
            
            if email and status == "On Leave":
                leave_data[email] = {
                    "status": status,
                    "leave_type": leave_type,
                    "date": row.get("Date", ""),
                    "leave_from": row.get("Leave From", ""),
                    "leave_to": row.get("Leave To", "")
                }
                print(f"📋 Found leave for {email}: {leave_type}")
                
        print(f"✅ Loaded leave data for {len(leave_data)} users")
        return leave_data
        
    except Exception as e:
        print(f"⚠️ Error fetching leave data: {e}")
        return {}

# ---------------- Summary helper (Claude) ----------------

def generate_summary_with_claude(notes_text):
    """Generate a concise summary of notes using Claude API."""
    api_key = CLAUDE_API_KEY
    model = CLAUDE_MODEL
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01"
    }
    prompt = (
        "Summarize the following time log notes in 2-3 lines, focusing on key work done today:\n\n"
        "Do not use markdown, write in plain text, do not use any heading or subheading"
        f"{notes_text}"
    )
    payload = {
        "model": model,
        "max_tokens": 150,
        "messages": [{"role": "user", "content": prompt}]
    }

    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 200:
            data = r.json()
            return data["content"][0]["text"].strip()
        else:
            print("⚠️ Claude API error:", r.text)
            return "Summary unavailable due to API error."
    except Exception as e:
        print("⚠️ Summary generation failed:", e)
        return "Summary unavailable."


# ---------------- Parsing & formatting helpers ----------------
def safe_float(x):
    try:
        if x is None or x == "":
            return 0.0
        return float(str(x).strip())
    except:
        return 0.0

def format_hours_decimal_to_hrmin(h):
    try:
        if h is None:
            return "0 hr 0 mins"
        s = str(h).strip()
        if "hr" in s or "min" in s:

            return s
        val = float(s)
        hrs = int(val)
        mins = int(round((val - hrs) * 60))
        return f"{hrs} hr {mins} mins"
    except:
        return str(h)


def build_notes_table_from_structured(notes_field, is_on_leave=False):
    if is_on_leave:
        return """<tr>
            <td colspan="3" style="border:1px solid #e2e2e2;padding:12px;text-align:center;background-color:#fff3cd;color:#856404;">
                <strong>🏖️ On Leave Today</strong>
            </td>
        </tr>"""
    
    if not notes_field:
        return "<tr><td colspan='3' style='padding:8px;'>No entries for today.</td></tr>"
    entry_sep = "<<<ENTRY>>>"
    field_sep = "|||"
    entries = notes_field.split(entry_sep)
    rows_html = ""

    def _fmt_time_range(tr):
        # Expect formats like "YYYY-MM-DD HH:MM:SS - YYYY-MM-DD HH:MM:SS"
        if not tr or " - " not in tr:
            return tr
        a,b = tr.split(" - ",1)
        def fmt(ts):
            ts = ts.strip()
            from datetime import datetime
            for fmt_in in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(ts, fmt_in)
                    return dt.strftime("%I:%M %p")
                except Exception:
                    continue
            # fallback: try to extract time portion
            if " " in ts:
                t = ts.split()[-1]
                try:
                    dt = datetime.strptime(t, "%H:%M:%S")
                    return dt.strftime("%I:%M %p")
                except Exception:
                    try:
                        dt = datetime.strptime(t, "%H:%M")
                        return dt.strftime("%I:%M %p")
                    except Exception:
                        return t
            return ts

        return f"{fmt(a)} - {fmt(b)}"

    for e in entries:
        e = e.strip()
        if not e:
            continue
        parts = e.split(field_sep)
        desc = parts[0].strip() if len(parts) > 0 else ""
        hours_raw = parts[1].strip() if len(parts) > 1 else ""
        time_range = parts[2].strip() if len(parts) > 2 else ""
        hours_fmt = format_hours_decimal_to_hrmin(hours_raw)
        # convert time range to short AM/PM form
        time_range_short = _fmt_time_range(time_range)
        desc_html = html.escape(desc)
        time_range_html = html.escape(time_range_short)
        rows_html += (
            f"<tr>"
            f"<td style='border:1px solid #e2e2e2;padding:8px;vertical-align:top;'>{desc_html}</td>"
            f"<td style='border:1px solid #e2e2e2;padding:8px;text-align:center;width:120px;'>{hours_fmt}</td>"
            f"<td style='border:1px solid #e2e2e2;padding:8px;text-align:center;width:220px;'>{time_range_html}</td>"
            f"</tr>"
        )
    return rows_html

def recommend_hours(week_info, target_hours=340):
    """Calculate recommendation based on all weeks worked so far (Nov 1 - Dec 21, 2025)"""
    # Sum all weeks (1-8)
    total_so_far = sum(safe_float(week_info.get(f"week{i}", 0)) for i in range(1, 9))
    remaining_hours = target_hours - total_so_far
    
    # Calculate based on Nov 1 - Dec 21 period
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    start_date = datetime(2025, 11, 1).date()
    end_date = datetime(2025, 12, 21).date()
    today = ist_now.date()
    
    # Calculate remaining days until Dec 21
    if today <= end_date:
        remaining_days = (end_date - today).days + 1
    else:
        remaining_days = 0
    
    if remaining_hours <= 0:
        explanation = f"🎉 Target of {target_hours} hrs already reached! Total so far: {total_so_far:.2f} hrs."
        return (0.0, remaining_days, 0, explanation)
    
    if remaining_days <= 0:
        explanation = f"⚠️ Evaluation period ended on Dec 21. You completed {total_so_far:.2f} hrs out of {target_hours} hrs target. Remaining: {remaining_hours:.2f} hrs."
        return (remaining_hours, 0, 0, explanation)
    
    rec_per_day = math.ceil(remaining_hours / remaining_days)
    explanation = (
        f"Target: {target_hours} hrs by Dec 21, 2025. "
        f"Total so far: {total_so_far:.2f} hrs. "
        f"Remaining: {remaining_hours:.2f} hrs over {remaining_days} days. "
        f"Recommend ~{rec_per_day} hrs/day."
    )
    return (remaining_hours, remaining_days, rec_per_day, explanation)

# ---------------- Email sending ----------------
def calculate_week_buckets():
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    start_date = datetime(ist_now.year, 11, 1).date()
    end_date = datetime(ist_now.year, 12, 21).date()
    
    # Calculate current week number
    days_since_start = (ist_now.date() - start_date).days + 1
    current_week = min((days_since_start - 1) // 7 + 1, 8)
    
    weeks = {}
    
    # Create weeks with actual date ranges up to current week
    for wk in range(1, current_week + 1):
        week_start_date = start_date + timedelta(days=(wk - 1) * 7)
        week_end_date = min(week_start_date + timedelta(days=6), end_date)
        
        # Format dates as "Nov 1-7" or "Nov 29 - Dec 5"
        if week_start_date.month == week_end_date.month:
            date_range = f"{week_start_date.strftime('%b')} {week_start_date.day}-{week_end_date.day}"
        else:
            date_range = f"{week_start_date.strftime('%b')} {week_start_date.day} - {week_end_date.strftime('%b')} {week_end_date.day}"
        
        weeks[wk] = date_range
    
    return weeks, current_week

def send_email(to_email, name, total_hours_display, notes_structured, summary_text, week_info, is_on_leave=False, leave_details=None):
    from email.mime.image import MIMEImage
    CC_EMAILS = [
        "rajan.gupta@annam.ai",
        "meenakshi.v@annam.ai",
        "sudarshan.iyengar@annam.ai",
        "radhika.kotwal@annam.ai",
        "harshdeep@annam.ai"
    ]
    email_to_use = to_email
    cc_to_use = CC_EMAILS.copy()
    if TEST_MODE:
        print(f"[TEST MODE] overriding recipient to {TEST_EMAIL}")
        email_to_use = TEST_EMAIL
        cc_to_use = []
    msg = EmailMessage()
    
    # Update subject for leave cases
    if is_on_leave:
        msg["Subject"] = "Your Time Log Summary for Today - On Leave"
    else:
        msg["Subject"] = "Your Time Log Summary for Today (testing)"
        
    msg["From"] = EMAIL_USER
    msg["To"] = email_to_use
    if cc_to_use:
        msg["Cc"] = ", ".join(cc_to_use)

    notes_rows_html = build_notes_table_from_structured(notes_structured, is_on_leave)
    weeks, current_week = calculate_week_buckets() 

    dynamic_week_rows = "" 

    for wk, date_range in weeks.items(): 

        val = week_info.get(f"week{wk}", 0.0) 

        dynamic_week_rows += ( 

            f"<tr>" \

            f"<td style=\"padding:8px;border:1px solid #eef2f7;\">Week {wk}: {date_range}</td>" \

            f"<td style=\"padding:8px;border:1px solid #eef2f7;text-align:right;\">{val:.2f}</td>" \

            f"</tr>" 

        ) 

    monthly = safe_float(week_info.get("monthly_total", 0))
    
    # Adjust recommendation for leave cases
    if is_on_leave:
        explanation = "🏖️ You are on leave today. No work hours expected."
        summary_text = "On leave today."
    else:
        remaining_hours, remaining_days, rec_per_day, explanation = recommend_hours(week_info, target_hours=340)
        # ---- Override recommendation message if rec_per_day > 7 hrs ----
        if rec_per_day > 7:
            explanation = (
                "If your hours are low because you're stuck on a task, unsure about priorities, or "
                "waiting on someone, please let us know. We can help clarify, unblock, or guide you "
                "so you can continue your work smoothly. Do reach out early so we can support you in "
                "meeting the 340-hour target."
            )

    html_body = f"""<html>
  <body style="font-family: Arial, Helvetica, sans-serif; color:#111827; background:#f6f7fb; padding:20px;">
    <div style="max-width:720px; margin:0 auto; background:#fff; padding:20px; border-radius:8px;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
        <h2 style="margin:0; color:#0b1726;">Your Time Log Summary for Today {'- On Leave' if is_on_leave else '(testing)'}</h2>
        <img src="cid:annamlogo" alt="Annam AI Logo" style="height:56px; width:auto; border-radius:6px; object-fit:contain;"/>
      </div>
      <p style="margin:4px 0 18px 0;">Hi {html.escape(name)},</p>
      <p style="margin:0 0 12px 0;color:#b45309;font-size:13px;">
        <b>Note:</b> This time log summary system is currently in testing. There may be inaccuracies in todays logged hours; please treat this email as experimental and report major discrepancies to HRM.
      </p>
      <p><b>Total Hours Logged (today):</b> <span style="color:#0b1726;">{html.escape(str(total_hours_display))}</span></p>

      <h3 style="margin-top:18px;">Notes (today)</h3>
      <table style="width:100%; border-collapse:collapse; font-size:14px;">
        <thead>
          <tr style="background:#f8fafc;">
            <th style="border:1px solid #e6eef6;padding:10px;text-align:left;">Work Description</th>
            <th style="border:1px solid #e6eef6;padding:10px;text-align:center;width:120px;">Hours</th>
            <th style="border:1px solid #e6eef6;padding:10px;text-align:center;width:220px;">Time Range (Start - End)</th>
          </tr>
        </thead>
        <tbody>
          {notes_rows_html}
        </tbody>
      </table>

      <h3 style="margin-top:18px;">Summary (AI-generated)</h3>
      <p style="line-height:1.45">{html.escape(summary_text)}</p>

      <hr style="margin:18px 0;border:none;border-top:1px solid #eef2f7;">

      <h3>Weekly Breakdown</h3>
      <table style="width:100%; border-collapse:collapse; margin-bottom:12px;">
        {dynamic_week_rows}
      </table>

      <p><b>Total:</b> {monthly:.2f} hrs</p>

      <h4>Recommendation to meet 340 hrs in 7 weeks</h4>
      <p style="line-height:1.45">{html.escape(explanation)}</p>

      <p style="color:#6b7280;margin-top:18px;">Regards,<br><strong>Annam AI</strong></p>
    </div>
  </body>
</html>
"""
    plain_text = (
        f"Hi {name},\n"
        "Note: This time log summary system is currently in testing, so there may be inaccuracies in the reported hours.\n"
        f"{'You are on leave today.' if is_on_leave else ''}\n"
        f"Total hours: {total_hours_display}\n"
        f"Summary: {summary_text}\n"
        f"Recommendation: {explanation}"
    )
    msg.set_content(plain_text)
    msg.add_alternative(html_body, subtype="html")

    logo_path = LOGO_PNG_PATH
    try:
        if os.path.exists(logo_path):
            with open(logo_path, "rb") as lf:
                img_data = lf.read()
            mime_img = MIMEImage(img_data)
            mime_img.add_header("Content-ID", "<annamlogo>")
            mime_img.add_header("Content-Disposition", "inline", filename=os.path.basename(logo_path))
            msg.attach(mime_img)
        else:
            print("⚠️ Logo file not found at", logo_path)
    except Exception as e:
        print("⚠️ Failed to attach logo inline:", e)

    recipients = [email_to_use] + cc_to_use
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg, to_addrs=recipients)
        print(f"✅ Email sent to {email_to_use} {'(On Leave)' if is_on_leave else ''}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# ---------------- CSV parsing ----------------
def extract_weekly_monthly_from_row(row):
    candidates = {
        "week1": ["Week 1 (1–7)", "Week 1(1-7) Total Hrs", "Week1(1-7) Total Hrs", "Week 1 Total Hrs", "Week1 Total Hrs"],
        "week2": ["Week 2 (8–14)", "Week 2(8-14) Total Hrs", "Week2 Total Hrs", "Week 2 Total Hrs"],
        "week3": ["Week 3 (15–21)", "Week 3(15-21) Total Hrs", "Week3 Total Hrs", "Week 3 Total Hrs"],
        "week4": ["Week 4 (22–28)", "Week 4(22-28) Total Hrs", "Week4 Total Hrs", "Week 4 Total Hrs"],
        "week5": ["Week 5 (29–35)", "Week 5(29-35) Total Hrs", "Week5 Total Hrs", "Week 5 Total Hrs"],
        "week6": ["Week 6 (36–42)", "Week 6(36-42) Total Hrs", "Week6 Total Hrs", "Week 6 Total Hrs"],
        "week7": ["Week 7 (43–49)", "Week 7(43-49) Total Hrs", "Week7 Total Hrs", "Week 7 Total Hrs"],
        "week8": ["Week 8 (50–56)", "Week 8(50-56) Total Hrs", "Week8 Total Hrs", "Week 8 Total Hrs"],
        "monthly": ["Monthly Total Hours", "Monthly Total", "Total Hours This Month"]
    }
    def find_first(keys):
        for k in keys:
            if k in row and row.get(k) not in (None, ""):
                return safe_float(row.get(k))
        return 0.0
    return {
        "week1": find_first(candidates["week1"]),
        "week2": find_first(candidates["week2"]),
        "week3": find_first(candidates["week3"]),
        "week4": find_first(candidates["week4"]),
        "week5": find_first(candidates["week5"]),
        "week6": find_first(candidates["week6"]),
        "week7": find_first(candidates["week7"]),
        "week8": find_first(candidates["week8"]),
        "monthly_total": find_first(candidates["monthly"])
    }

def parse_and_email(combined_data, leave_data):
    """Process combined timelog data and send emails"""
    count = 0
    for email, row in combined_data.items():
        name = row.get("User Name", "").strip() or "User"
        total_hours_display = row.get("Total Hours Logged Today", "0 hr 0 mins")
        notes_structured = row.get("Time Log Notes", "")
        
        # Create week_info from row data
        week_info = {
            "week1": row.get("Week 1 (1–7)", 0),
            "week2": row.get("Week 2 (8–14)", 0),
            "week3": row.get("Week 3 (15–21)", 0),
            "week4": row.get("Week 4 (22–28)", 0),
            "week5": row.get("Week 5 (29–35)", 0),
            "week6": row.get("Week 6 (36–42)", 0),
            "week7": row.get("Week 7 (43–49)", 0),
            "week8": row.get("Week 8 (50–56)", 0),
            "monthly_total": row.get("Monthly Total Hours", 0)
        }
        
        # Check if user is on leave
        is_on_leave = email in leave_data
        leave_details = leave_data.get(email)
        
        # Generate appropriate summary
        if is_on_leave:
            summary_text = "On leave today."
        else:
            summary_text = generate_summary_with_claude(notes_structured)
            
        send_email(email, name, total_hours_display, notes_structured, summary_text, week_info, is_on_leave, leave_details)
        count += 1
    print(f"➡️ Processed and attempted to email {count} users.")

# ---------------- Main flow ----------------
def main():
    global ACCESS_TOKEN
    if not ACCESS_TOKEN:
        ACCESS_TOKEN = get_access_token()
    
    # First, fetch leave data
    print("📋 Fetching leave data...")
    leave_data = fetch_leave_data(ACCESS_TOKEN)
    
    # Fetch Projects time log data
    print("\n📊 Fetching Zoho Projects time log data...")
    try:
        projects_csv = fetch_timelog_data(ACCESS_TOKEN, PROJECTS_WORKSPACE_ID, PROJECTS_VIEW_ID, "Projects")
    except ValueError:
        print("🔄 Token invalid, refreshing and retrying...")
        ACCESS_TOKEN = get_access_token(force_refresh=True)
        projects_csv = fetch_timelog_data(ACCESS_TOKEN, PROJECTS_WORKSPACE_ID, PROJECTS_VIEW_ID, "Projects")
    
    # Fetch Sprints time log data
    print("\n📊 Fetching Zoho Sprints time log data...")
    sprints_csv = fetch_timelog_data(ACCESS_TOKEN, SPRINTS_WORKSPACE_ID, SPRINTS_VIEW_ID, "Sprints")
    
    # Check if we have at least one data source
    if not projects_csv and not sprints_csv:
        print("❌ Could not fetch data from either Projects or Sprints.")
        return
    
    # Save debug CSVs
    if projects_csv:
        try:
            with open("/home/adityabmv/zoho_scripts/debug_projects.csv", "w", encoding="utf-8") as f:
                f.write(projects_csv)
            print("🔍 Saved Projects CSV to debug_projects.csv")
        except Exception as e:
            print("⚠️ Couldn't write Projects debug CSV:", e)
    
    if sprints_csv:
        try:
            with open("/home/adityabmv/zoho_scripts/debug_sprints.csv", "w", encoding="utf-8") as f:
                f.write(sprints_csv)
            print("🔍 Saved Sprints CSV to debug_sprints.csv")
        except Exception as e:
            print("⚠️ Couldn't write Sprints debug CSV:", e)
    
    # Combine both data sources
    print("\n🔄 Combining Projects and Sprints data...")
    combined_data = combine_timelog_data(projects_csv, sprints_csv)
    
    # Save combined data for debugging
    try:
        with open("/home/adityabmv/zoho_scripts/debug_combined.csv", "w", encoding="utf-8") as f:
            if combined_data:
                fieldnames = list(next(iter(combined_data.values())).keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(combined_data.values())
            print("🔍 Saved combined CSV to debug_combined.csv")
    except Exception as e:
        print("⚠️ Couldn't write combined debug CSV:", e)
    
    # Send emails with combined data
    print("\n📧 Sending emails...")
    parse_and_email(combined_data, leave_data)

if __name__ == "__main__":
    main()
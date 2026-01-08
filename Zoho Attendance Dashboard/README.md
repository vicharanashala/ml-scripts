# Zoho Attendance Dashboard

A comprehensive SQL-based attendance monitoring dashboard built in Zoho Analytics for real-time workforce insights.

## Overview

This dashboard provides automated daily attendance tracking for 83 employees with visual analytics, KPI metrics, and actionable insights for HR and management teams.

**Live Dashboard:** https://analytics.zoho.in/open-view/461220000000686047

## Features

### KPI Metrics
- **Total Employees:** 83 current working employees
- **Attendance Rate:** Real-time percentage calculation
- **Present/Absent Count:** Daily employee status
- **Late Arrivals:** Employees checking in >15 minutes late
- **Early Exits:** Employees leaving before scheduled time
- **Average Working Hours:** Calculated from check-in/out times
- **Overtime Workers:** Employees working >8 hours

### Visual Analytics
- **Department Attendance Rate:** Color-coded bar chart (Green >80%, Orange 60-80%, Red <60%)
- **Department Status Breakdown:** Stacked bar chart showing Present/Absent/Late by department

### Action Tables
- **Late Arrivals Table:** Employee names, departments, exact check-in times
- **Early Exits Table:** Employee names, departments, checkout times
- **Overtime Workers Table:** Employee names, departments, precise hours worked (decimal format)

## Dashboard Settings
- **Auto-refresh:** Every 30 minutes
- **Access:** Organization-wide view permissions
- **Data Source:** Previous day's attendance (updates daily)

## SQL Queries

### 1. daily_attendance_detail.sql
**Purpose:** Employee-wise attendance details with 23+ columns

**Key Features:**
- Joins Employee, Current Working Employees, Department, and Attendance tables
- Converts time format from "HH:MM" to decimal hours
- Calculates late minutes and early exit minutes
- Identifies check-in/check-out status

**Output Columns:** employee_id, employee_name, email, department, check_in_time, check_out_time, total_hours_decimal, attendance_status, is_late, is_early_exit, etc.

### 2. attendance_statistics.sql
**Purpose:** Single-row aggregate producing all KPI metrics

**Key Calculations:**
- Total employees count using DISTINCT Employee ID
- Attendance percentage: (Present / Total) × 100
- Average working hours with ROUND to 2 decimals
- Overtime count: employees with hours > 8
- Late arrival percentage: (Late / Total) × 100

**Output:** report_date, total_employees, employees_present, employees_absent, late_check_ins, early_exits, avg_working_hours, overtime_count, attendance_percentage

### 3. dashboard_queries.sql
**Purpose:** Department-wise attendance summary for charts

**Aggregations:**
- Groups by department name
- Counts total, present, absent, late employees per department
- Calculates department-level attendance rate
- Orders by attendance rate (descending)

**Output:** department, total_employees, present_count, absent_count, late_count, attendance_rate, avg_hours_worked

## Technical Implementation

### Time Format Conversion
**Problem:** Total hours stored as "HH:MM" text format caused zero values in calculations

**Solution:**
```sql
ROUND(
    CAST(SUBSTRING(a."Total hours", 1, 2) AS DECIMAL) + 
    CAST(SUBSTRING(a."Total hours", 4, 2) AS DECIMAL) / 60.0, 
    2
) AS total_hours_decimal
```

Converts "10:30" → 10.5 hours

### Employee Filtering
**Method:** INNER JOIN with "Current Working Employees" table using Email address

**Why Email?** Employee IDs had duplicates; email addresses are unique identifiers

```sql
FROM "Employee" e
INNER JOIN "Current Working Employees" cwe
    ON e."Email address" = cwe."Email address"
```

### Date Filter
All queries use `ADDDATE(CURDATE(), -1)` to fetch previous day's attendance

## Data Schema

### Source Tables
1. **Employee** - Master employee database (ID, name, email, department, status)
2. **Current Working Employees** - HR-maintained active employee list (83 employees)
3. **Department** - Department ID to name mapping
4. **Attendance User Report** - Daily check-in/out records

### Key Fields
- `Employee ID` / `Email address` - Employee identifiers
- `Check in` / `Check out` - Timestamp fields
- `Total hours` - Duration in "HH:MM" format
- `Status` - Present / Absent / Work From Home
- `Late entry minutes` - Minutes late beyond grace period
- `Early exit minutes` - Minutes before scheduled end

## Setup Instructions

### 1. Import Data Tables to Zoho Analytics
- Upload Employee.csv
- Upload Current_Working_Employees.csv
- Upload Department.csv
- Upload Attendance_User_Report.csv
- Connect to Zoho People Attendance module (if available)

### 2. Create Query Tables
1. Go to **Data → Query Tables → New Query Table**
2. Create three query tables:
   - `daily_attendance_detail` (paste daily_attendance_detail.sql)
   - `attendance_statistics` (paste attendance_statistics.sql)
   - `dashboard_queries` (paste dashboard_queries.sql)
3. Execute each query to verify data

### 3. Build Dashboard
1. Create new dashboard: **Daily Attendance Monitor**
2. Add 8 KPI widgets using `attendance_statistics` query
3. Add 2 charts using `dashboard_queries` query:
   - Bar chart for attendance rate
   - Stacked bar for status breakdown
4. Add 3 table widgets using filtered `daily_attendance_detail` query:
   - Late Arrivals (filter: is_late = 'Yes')
   - Early Exits (filter: is_early_exit = 'Yes')
   - Overtime Workers (filter: total_hours_decimal > 8)

### 4. Configure Settings
- Enable auto-refresh: 30 minutes (1800 seconds)
- Set access permissions: Organization-wide
- Generate public/embed URL if needed

## Project Structure
```
zoho-dashboard/
├── README.md                          # Project documentation
├── sql-queries/
│   ├── daily_attendance_detail.sql    # Employee-wise details (23 columns)
│   ├── attendance_statistics.sql      # KPI metrics aggregation
│   └── dashboard_queries.sql          # Department-wise summary
├── sample-data/
│   ├── Employee.csv                   # Sample employee master data
│   ├── Attendance_User_Report.csv     # Sample attendance records
│   └── Current_Working_Employees.csv  # Sample active employee list
└── docs/
    └── SCHEMA.md                      # Data model documentation
```

## Troubleshooting

### Dashboard shows 0 for avg_working_hours
- Check if time format is "HH:MM" string
- Verify SUBSTRING and CAST functions are working
- Ensure decimal conversion formula is applied

### Employee count mismatch
- Verify INNER JOIN is using Email address, not Employee ID
- Check for duplicate entries in Current Working Employees table
- Confirm COUNT(DISTINCT) is used

### Charts not displaying
- Verify department names are not NULL (using COALESCE)
- Check if CAST(a."DATE" AS DATE) matches date format
- Ensure GROUP BY includes all non-aggregated columns

## Future Enhancements
- Weekly/Monthly attendance trends
- Employee-wise attendance history
- Department comparison analytics
- Automated email reports
- Integration with Zoho Cliq for notifications

## Credits
Developed by Kshitij Pandey  
Organization: Annamai  
Date: January 8, 2026

## License
Internal use only - Annamai organization

# Data Schema Documentation

## Table Relationships

```
Employee (master)
    ↓ INNER JOIN (Email address)
Current Working Employees (filter: 83 active)
    ↓ LEFT JOIN (Department ID)
Department (names)
    ↓ LEFT JOIN (Employee ID)
Attendance User Report (daily records)
```

## Table Schemas

### Employee Table
| Column | Type | Description |
|--------|------|-------------|
| Employee ID | TEXT | Unique identifier (may have duplicates) |
| Email address | TEXT | Unique email (primary key for joins) |
| First Name | TEXT | Employee first name |
| Last Name | TEXT | Employee last name |
| Employee Name | TEXT | Full name with nickname |
| Department | TEXT | Department ID (foreign key) |
| Employee Status | TEXT | Active/Resigned/On Leave |
| Designation | TEXT | Job title/role |
| Location | TEXT | Work location |
| Reporting To (Name) | TEXT | Manager name |

### Current Working Employees Table
| Column | Type | Description |
|--------|------|-------------|
| Employee ID | TEXT | Employee identifier |
| Email address | TEXT | Unique email (join key) |
| First Name | TEXT | Employee first name |
| Last Name | TEXT | Employee last name |
| Employee Status | TEXT | All "Active" (83 rows) |

**Note:** This is the HR-maintained accurate list of current employees. Join on Email address to avoid duplicate Employee ID issues.

### Attendance User Report Table
| Column | Type | Description |
|--------|------|-------------|
| Employee | TEXT | Reference to Employee.ID |
| DATE | DATE | Attendance date |
| Check in | TIMESTAMP | Check-in time |
| Check out | TIMESTAMP | Check-out time |
| Total hours | TEXT | Duration in "HH:MM" format |
| Office in hours | TEXT | Office time in "HH:MM" |
| Status | TEXT | Present/Absent/Work From Home |
| Late entry minutes | INTEGER | Minutes late (0 if on time) |
| Early exit minutes | INTEGER | Minutes early (0 if stayed) |
| Deviation time | TEXT | Time difference from schedule |

### Department Table
| Column | Type | Description |
|--------|------|-------------|
| ID | TEXT | Department identifier |
| Department Name | TEXT | Human-readable name |

## Join Keys

### Primary Joins
- **Employee ↔ Current Working Employees:** `e."Email address" = cwe."Email address"`
- **Employee ↔ Department:** `e."Department" = d."ID"`
- **Employee ↔ Attendance:** `e."ID" = a."Employee"`

### Date Filter
All queries use: `CAST(a."DATE" AS DATE) = ADDDATE(CURDATE(), -1)`

## Data Types & Conversions

### Time Format Conversion
**Input:** "10:30" (TEXT)  
**Output:** 10.5 (DECIMAL)

**Formula:**
```sql
CAST(SUBSTRING(field, 1, 2) AS DECIMAL) +   -- Hours
CAST(SUBSTRING(field, 4, 2) AS DECIMAL) / 60.0  -- Minutes to decimal
```

### Boolean Flags
```sql
CASE 
    WHEN condition THEN 'Yes'
    ELSE 'No'
END
```

## Sample Data Counts
- Total Employees in Employee table: 118
- Current Working Employees: 83
- Departments: 10
- Daily Attendance Records: ~83 per day

## Known Data Quality Issues

### Issue 1: Duplicate Employee IDs
**Problem:** Employee ID "1" appears twice in Current Working Employees
**Solution:** Use Email address for joins (guaranteed unique)

### Issue 2: Time Format
**Problem:** Total hours stored as TEXT "HH:MM" instead of DECIMAL
**Solution:** Use SUBSTRING and CAST for conversion

### Issue 3: NULL Departments
**Problem:** Some employees have NULL or missing Department IDs
**Solution:** Use COALESCE(d."Department Name", 'Not Assigned')

## Query Performance Tips
- Use COUNT(DISTINCT) to avoid double-counting
- Apply ROUND() to limit decimal precision
- Filter by date early in WHERE clause
- Use INNER JOIN for Current Working Employees filter
- Use LEFT JOIN for optional fields (Department, Attendance)

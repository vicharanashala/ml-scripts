SELECT 
    e."Employee ID" AS employee_id,
    e."Employee Name" AS employee_name,
    e."First Name" AS first_name,
    e."Last Name" AS last_name,
    e."Email address" AS email,
    COALESCE(d."Department Name", 'Not Assigned') AS department,
    e."Designation" AS designation,
    e."Location" AS location,
    e."Reporting To (Name)" AS reporting_manager,
    e."Employee Status" AS employment_status,
    a."DATE" AS attendance_date,
    a."Check in" AS check_in_time,
    a."Check out" AS check_out_time,
    a."Total hours" AS total_hours,
    ROUND(CAST(SUBSTRING(a."Total hours", 1, 2) AS DECIMAL) + CAST(SUBSTRING(a."Total hours", 4, 2) AS DECIMAL) / 60.0, 2) AS total_hours_decimal,
    a."Office in hours" AS office_hours,
    a."Status" AS attendance_status,
    a."Deviation time" AS deviation_time,
    a."Late entry minutes" AS late_minutes,
    a."Early exit minutes" AS early_exit_minutes,
    CASE 
        WHEN a."Late entry minutes" > 0 THEN 'Yes'
        ELSE 'No'
    END AS is_late,
    CASE 
        WHEN a."Early exit minutes" > 0 THEN 'Yes'
        ELSE 'No'
    END AS is_early_exit,
    CASE 
        WHEN a."Check in" IS NULL THEN 'Not Checked In'
        WHEN a."Check out" IS NULL THEN 'Not Checked Out'
        WHEN a."Status" = 'Present' THEN 'Complete'
        ELSE a."Status"
    END AS check_status
FROM 
    "Employee" e
INNER JOIN 
    "Current Working Employees" cwe
    ON e."Email address" = cwe."Email address"
LEFT JOIN 
    "Department" d
    ON e."Department" = d."ID"
LEFT JOIN 
    "Attendance User Report" a
    ON e."ID" = a."Employee"
    AND CAST(a."DATE" AS DATE) = ADDDATE(CURDATE(), -1)
ORDER BY 
    d."Department Name" ASC,
    a."Late entry minutes" DESC,
    e."Employee Name" ASC

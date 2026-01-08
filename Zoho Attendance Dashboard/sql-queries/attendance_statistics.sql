SELECT 
    ADDDATE(CURDATE(), -1) AS report_date,
    DATE_FORMAT(ADDDATE(CURDATE(), -1), '%d %b %Y') AS formatted_date,
    COUNT(DISTINCT e."Employee ID") AS total_employees,
    COUNT(DISTINCT CASE 
        WHEN a."Status" IN ('Present', 'Work From Home') 
        THEN e."Employee ID" 
    END) AS employees_present,
    COUNT(DISTINCT CASE 
        WHEN a."Status" = 'Absent' OR a."Check in" IS NULL
        THEN e."Employee ID" 
    END) AS employees_absent,
    COUNT(DISTINCT CASE 
        WHEN a."Late entry minutes" > 0 
        THEN e."Employee ID" 
    END) AS late_check_ins,
    COUNT(DISTINCT CASE 
        WHEN a."Early exit minutes" > 0 
        THEN e."Employee ID" 
    END) AS early_exits,
    COUNT(DISTINCT CASE 
        WHEN a."Status" = 'Work From Home' 
        THEN e."Employee ID" 
    END) AS work_from_home,
    ROUND(AVG(CAST(SUBSTRING(a."Total hours", 1, 2) AS DECIMAL) + CAST(SUBSTRING(a."Total hours", 4, 2) AS DECIMAL) / 60.0), 2) AS avg_working_hours,
    ROUND(MAX(CAST(SUBSTRING(a."Total hours", 1, 2) AS DECIMAL) + CAST(SUBSTRING(a."Total hours", 4, 2) AS DECIMAL) / 60.0), 2) AS max_working_hours,
    ROUND(MIN(CAST(SUBSTRING(a."Total hours", 1, 2) AS DECIMAL) + CAST(SUBSTRING(a."Total hours", 4, 2) AS DECIMAL) / 60.0), 2) AS min_working_hours,
    ROUND(AVG(CAST(SUBSTRING(a."Office in hours", 1, 2) AS DECIMAL) + CAST(SUBSTRING(a."Office in hours", 4, 2) AS DECIMAL) / 60.0), 2) AS avg_office_hours,
    COUNT(DISTINCT CASE 
        WHEN (CAST(SUBSTRING(a."Total hours", 1, 2) AS DECIMAL) + CAST(SUBSTRING(a."Total hours", 4, 2) AS DECIMAL) / 60.0) > 8 
        THEN e."Employee ID" 
    END) AS overtime_count,
    SUM(CASE 
        WHEN (CAST(SUBSTRING(a."Total hours", 1, 2) AS DECIMAL) + CAST(SUBSTRING(a."Total hours", 4, 2) AS DECIMAL) / 60.0) > 8 
        THEN (CAST(SUBSTRING(a."Total hours", 1, 2) AS DECIMAL) + CAST(SUBSTRING(a."Total hours", 4, 2) AS DECIMAL) / 60.0) - 8 
        ELSE 0 
    END) AS total_overtime_hours,
    ROUND((COUNT(DISTINCT CASE 
        WHEN a."Status" IN ('Present', 'Work From Home') 
        THEN e."Employee ID" 
    END) * 100.0) / COUNT(DISTINCT e."Employee ID"), 2) AS attendance_percentage,
    ROUND((COUNT(DISTINCT CASE 
        WHEN a."Late entry minutes" > 0 
        THEN e."Employee ID" 
    END) * 100.0) / COUNT(DISTINCT e."Employee ID"), 2) AS late_arrival_percentage
FROM 
    "Employee" e
INNER JOIN 
    "Current Working Employees" cwe
    ON e."Email address" = cwe."Email address"
LEFT JOIN 
    "Attendance User Report" a
    ON e."ID" = a."Employee"
    AND CAST(a."DATE" AS DATE) = ADDDATE(CURDATE(), -1)

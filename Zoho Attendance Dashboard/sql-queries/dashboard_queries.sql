SELECT 
    COALESCE(d."Department Name", 'Not Assigned') AS department,
    COUNT(DISTINCT e."Employee ID") AS total_employees,
    COUNT(DISTINCT CASE 
        WHEN a."Status" IN ('Present', 'Work From Home') 
        THEN e."Employee ID" 
    END) AS present_count,
    COUNT(DISTINCT CASE 
        WHEN a."Status" = 'Absent' OR a."Check in" IS NULL 
        THEN e."Employee ID" 
    END) AS absent_count,
    COUNT(DISTINCT CASE  
        WHEN a."Late entry minutes" > 0 
        THEN e."Employee ID" 
    END) AS late_count,
    ROUND((COUNT(DISTINCT CASE 
        WHEN a."Status" IN ('Present', 'Work From Home') 
        THEN e."Employee ID" 
    END) * 100.0) / COUNT(DISTINCT e."Employee ID"), 2) AS attendance_rate,
    AVG(a."Total hours") AS avg_hours_worked
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
GROUP BY 
    d."Department Name"
ORDER BY 
    attendance_rate DESC,
    d."Department Name" ASC

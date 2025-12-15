SELECT
		 E."Email address" AS "Email",
		 AUR."DATE" AS "Date",
		 
			CASE
				 WHEN L."Employee ID"  IS NOT NULL THEN 'On Leave'
				 WHEN AUR."Status"  = 'Absent' THEN 'Absent'
				 ELSE AUR."Status"
			 END AS "Attendance Status",
		 L."Leave type" AS "Leave Type",
		 L."From" AS "Leave From",
		 L."To" AS "Leave To"
FROM  "Attendance User Report" AUR
LEFT JOIN "Employee" E ON AUR."ID"  = E."ID" 
LEFT JOIN "Leave" L ON AUR."Employee"  = L."Employee ID"
	 AND	CURDATE()  BETWEEN L."From"  AND  L."To"  
WHERE	 AUR."DATE"  = CURDATE()
 AND	(AUR."Status"  = 'Absent'
 OR	L."Employee ID"  IS NOT NULL)
ORDER BY E."Email address" 
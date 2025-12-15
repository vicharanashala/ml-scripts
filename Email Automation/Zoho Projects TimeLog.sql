SELECT
		 U."User Name" AS "User Name",
		 U."User Email" AS "User Email",
		 /* total seconds for today */ SUM(CASE
				 WHEN T."Date"  >= CURDATE()
				 AND	T."Date"  < ADDDATE(CURDATE(), 1) THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) AS "TotalSeconds_Raw",
		 /* Today's hours decimal */ ROUND(SUM(CASE
				 WHEN T."Date"  >= CURDATE()
				 AND	T."Date"  < ADDDATE(CURDATE(), 1) THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "TotalHoursDecimal",
		 /* Today's formatted hours */ CONCAT(FLOOR(SUM(CASE
				 WHEN T."Date"  >= CURDATE()
				 AND	T."Date"  < ADDDATE(CURDATE(), 1) THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0), ' hr ', ROUND(MOD((SUM(CASE
				 WHEN T."Date"  >= CURDATE()
				 AND	T."Date"  < ADDDATE(CURDATE(), 1) THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 60.0), 60)), ' mins') AS "Total Hours Logged Today",
		 /* Today's structured notes */ GROUP_CONCAT(
			CASE
				 WHEN T."Date"  >= CURDATE()
				 AND	T."Date"  < ADDDATE(CURDATE(), 1) THEN CONCAT(REPLACE(COALESCE(T."Notes", ''), '\n', ' '), '|||', ROUND(COALESCE(T."Hours in Duration", 0) / 3600.0, 4), '|||', CONCAT(COALESCE(CAST(T."Start Time" AS CHAR), ''), ' - ', COALESCE(CAST(T."End Time" AS CHAR), '')))
				 ELSE NULL
			 END SEPARATOR '<<<ENTRY>>>') AS "Time Log Notes",
		 /* ---------- WEEKLY BUCKETS (1 to 8) ---------- */ ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-11-01'
				 AND	T."Date"  < '2025-11-08' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 1 (1–7)",
		 ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-11-08'
				 AND	T."Date"  < '2025-11-15' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 2 (8–14)",
		 ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-11-15'
				 AND	T."Date"  < '2025-11-22' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 3 (15–21)",
		 ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-11-22'
				 AND	T."Date"  < '2025-11-29' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 4 (22–28)",
		 ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-11-29'
				 AND	T."Date"  < '2025-12-06' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 5 (29–35)",
		 ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-12-06'
				 AND	T."Date"  < '2025-12-13' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 6 (36–42)",
		 ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-12-13'
				 AND	T."Date"  < '2025-12-20' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 7 (43–49)",
		 ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-12-20'
				 AND	T."Date"  <= '2025-12-21' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 8 (50–56)",
		 /* Monthly hours */ ROUND(SUM(CASE
				 WHEN T."Date"  >= '2025-11-01'
				 AND	T."Date"  <= '2025-12-21' THEN COALESCE(T."Hours in Duration", 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Monthly Total Hours"
FROM  "Time Logs" AS  T
JOIN "Users" AS  U ON U."User ID"  = T."User ID"  
WHERE	 U."Status"  = 'active'
GROUP BY U."User Name",
	  U."User Email" 
ORDER BY U."User Name" 
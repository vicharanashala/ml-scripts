/* ========================================*/
/* ZOHO SPRINTS EQUIVALENT QUERY*/
/* Maps Sprints tables (Users + Timesheets) to match Projects Time Logs structure*/
/* Column mappings:*/
/*   Timesheets."Log Date" → T."Date"*/
/*   Timesheets."Log Time in Minutes" * 60 → T."Hours in Duration" (convert to seconds)*/
/*   Timesheets."Description" → T."Notes"*/
/*   Users."Email ID" → U."User Email"*/
/*   Users."User Name" → U."User Name"*/
/*   Users."User Status" → U."Status"*/
SELECT
		 U."User Name" AS "User Name",
		 U."Email ID" AS "User Email",
		 /* total seconds for today */ SUM(CASE
				 WHEN T."Log Date"  >= CURDATE()
				 AND	T."Log Date"  < ADDDATE(CURDATE(), 1) THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) AS "TotalSeconds_Raw",
		 /* Today's hours decimal */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= CURDATE()
				 AND	T."Log Date"  < ADDDATE(CURDATE(), 1) THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "TotalHoursDecimal",
		 /* Today's formatted hours */ CONCAT(FLOOR(SUM(CASE
				 WHEN T."Log Date"  >= CURDATE()
				 AND	T."Log Date"  < ADDDATE(CURDATE(), 1) THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0), ' hr ', ROUND(MOD((SUM(CASE
				 WHEN T."Log Date"  >= CURDATE()
				 AND	T."Log Date"  < ADDDATE(CURDATE(), 1) THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 60.0), 60)), ' mins') AS "Total Hours Logged Today",
		 /* Today's structured notes - Sprints doesn't have Start/End time, only description */ GROUP_CONCAT(
			CASE
				 WHEN T."Log Date"  >= CURDATE()
				 AND	T."Log Date"  < ADDDATE(CURDATE(), 1) THEN CONCAT(REPLACE(COALESCE(T."Description", ''), '\n', ' '), '|||', ROUND(COALESCE(T."Log Time in Minutes" * 60, 0) / 3600.0, 4), '|||', 'N/A - N/A')
				 ELSE NULL
			 END SEPARATOR '<<<ENTRY>>>') AS "Time Log Notes",
		 /* ---------- WEEKLY BUCKETS (1 to 8) ---------- */
/* Week 1: Nov 1-7 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-11-01'
				 AND	T."Log Date"  < '2025-11-08' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 1 (1–7)",
		 /* Week 2: Nov 8-14 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-11-08'
				 AND	T."Log Date"  < '2025-11-15' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 2 (8–14)",
		 /* Week 3: Nov 15-21 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-11-15'
				 AND	T."Log Date"  < '2025-11-22' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 3 (15–21)",
		 /* Week 4: Nov 22-28 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-11-22'
				 AND	T."Log Date"  < '2025-11-29' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 4 (22–28)",
		 /* Week 5: Nov 29 - Dec 5 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-11-29'
				 AND	T."Log Date"  < '2025-12-06' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 5 (29–35)",
		 /* Week 6: Dec 6-12 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-12-06'
				 AND	T."Log Date"  < '2025-12-13' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 6 (36–42)",
		 /* Week 7: Dec 13-19 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-12-13'
				 AND	T."Log Date"  < '2025-12-20' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 7 (43–49)",
		 /* Week 8: Dec 20-21 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-12-20'
				 AND	T."Log Date"  <= '2025-12-21' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Week 8 (50–56)",
		 /* Monthly hours - counts Nov 1 to Dec 21 */ ROUND(SUM(CASE
				 WHEN T."Log Date"  >= '2025-11-01'
				 AND	T."Log Date"  <= '2025-12-21' THEN COALESCE(T."Log Time in Minutes" * 60, 0)
				 ELSE 0
			 END) / 3600.0, 3) AS "Monthly Total Hours"
FROM  "Timesheets" AS  T
JOIN "Users" AS  U ON U."ZSUser ID"  = T."Added By"  
WHERE	 U."User Status"  = 'ACTIVE'
GROUP BY U."User Name",
	  U."Email ID" 
ORDER BY U."User Name" 
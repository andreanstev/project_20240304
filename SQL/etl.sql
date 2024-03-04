DROP TABLE IF EXISTS monthly_salary_per_hour;
CREATE TABLE monthly_salary_per_hour AS
WITH employee_cleaned AS ( -- make sure that employee_id are unique for each branch
	SELECT branch_id, employee_id, 
    MAX(salary) salary,  -- Get biggest salary if duplicate employee record exists i.e. for employee_id 218078
    MIN(join_date) join_date, 
    MAX(resign_date) resign_date
	FROM employee
    WHERE join_date IS NOT NULL -- assume that record are invalid if join_date is null
	GROUP BY branch_id, employee_id
), current_timesheet AS (
	SELECT *,
	EXTRACT(year FROM t.date) "year", 
	EXTRACT(month FROM t.date) "month", 
	EXTRACT(epoch FROM 
       CASE WHEN checkout < checkin AND checkout IS NOT NULL AND checkin IS NOT NULL THEN
           (checkout + interval '1 day')::time - checkin -- assume that if checkout is before checkin, the employee are checkout on the next day
       ELSE
           (COALESCE(t.checkout, '18:00:00'::time)  - COALESCE(t.checkin, '09:00:00'::time)) -- checkin and checkout can be fill with default value instead dropping it (assume that employee must checkin and checkout at least once a day)
       END) / 3600 work_hour
    FROM timesheets t
	WHERE date < now()::date -- Fetch all data until previous day (assume that the script is run after 12am daily)
	-- AND t.checkin IS NOT NULL
	-- AND t.checkout IS NOT NULL
),
combined AS (
	SELECT 
	e.branch_id,e.employee_id,
	year, 
	month, 
	MAX(e.salary) total_salary,
	SUM(t.work_hour) total_work_hour
	FROM current_timesheet t
	LEFT JOIN employee e ON t.employee_id = e.employee_id
	WHERE t.date <= COALESCE(e.resign_date, '9999-12-31') -- assume that resign_date as reference, timesheets that happen after resign_date are invalid i.e. for employee_id 116539
	AND t.date>= e.join_date -- assume that join_date as reference, timesheets that happen before join_date are invalid
	GROUP BY e.branch_id, e.employee_id, year, month
)
SELECT 
year::int, 
month::int, 
branch_id::bigint, 
ROUND(SUM(total_salary)/SUM(total_work_hour), 0)::bigint salary_per_hour -- round to integer for readability
FROM combined
GROUP BY branch_id, year, month
ORDER BY year, month, branch_id

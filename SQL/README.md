# Prerequisite
- postgresql
- pgAdmin or other database management tool
- employees.csv & timesheets.csv

# Quick note
Just run the "etl.sql" script to do the task. 
Make sure that the table "employee" and "timesheets" exist and following the schema in the "source_table.sql".
monthly_salary_per_hour.csv -> check this out to see the final result

# Procedure
Here are steps to do the task:
1. Run sql script using "source_table.sql".
2. Load the data into "employee" and "timesheets" tables from CSV.
3. Run the "etl.sql" script.
4. Check the result on table "monthly_salary_per_hour".

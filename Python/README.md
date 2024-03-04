# Prerequisite:
- Python 3.9.13
- Package on requirements.txt are installed
- postgresql

# Quick note
Just run the "etl.py" script to do the task. For example: "python etl.py 2019-08-23".  
The script are designed to run daily. It only need one parameter which is the business date.  
Make sure that the CSV "employee" and "timesheets" exist.  
monthly_salary_per_hour.csv -> check this out to see the final result

# How it works
1. Extract CSV "employee" and "timesheets" into pandas dataframe.
2. Extract timesheets for specific business date, then combined with employee data.
3. Incrementally store combined data into "staging_timesheets" table.
4. For the last step, fetch all data from "staging_timesheets" table. Then aggregate the data to calculate salary/hourly rate. 
5. Aggregated data will be store into "monthly_salary_per_hour" table (overwrite existing table).

# Additional note:
Step 4 and 5 can be modify into incremental so it does not overwrite the entire table, instead it only update for specific branch, year, and month data. But for simplify the job, overwrite method are used instead.

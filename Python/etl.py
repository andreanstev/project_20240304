import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from psycopg2.errors import UniqueViolation, IntegrityError
import os
import sys

from dotenv import load_dotenv
_ = load_dotenv()

pd.options.mode.chained_assignment = None

# Initialize postgresql connection
PSQL_USER = os.getenv('PSQL_USER')
PSQL_PASSWORD = os.getenv('PSQL_PASSWORD')
PSQL_HOST =os.getenv('PSQL_HOST')
PSQL_PORT = os.getenv('PSQL_PORT')
PSQL_DB = os.getenv('PSQL_DB')

engine = create_engine(f'postgresql://{PSQL_USER}:{PSQL_PASSWORD}@{PSQL_HOST}:{PSQL_PORT}/{PSQL_DB}')

def process_data_staging(employee_df, timesheet_df_daily):
    # Process timesheets
    timesheet_df_daily['year'] = timesheet_df_daily['date'].dt.year
    timesheet_df_daily['month'] = timesheet_df_daily['date'].dt.month
    timesheet_df_daily['checkin'] = pd.to_datetime(timesheet_df_daily['checkin'].fillna("09:00:00"), format='%H:%M:%S')
    timesheet_df_daily['checkout'] = pd.to_datetime(timesheet_df_daily['checkout'].fillna("18:00:00"), format='%H:%M:%S')

    def calculate_hours(row):
        checkin, checkout = row['checkin'], row['checkout']
        if checkout < checkin:
            return (checkout + pd.Timedelta(days=1)) - checkin
        else:
            return checkout - checkin
    timesheet_df_daily['work_hour'] = timesheet_df_daily[['checkin', 'checkout']].apply(calculate_hours, axis=1)
    timesheet_df_daily['work_hour'] = timesheet_df_daily['work_hour'].dt.total_seconds() / 3600

    # timesheet_df_daily = timesheet_df_daily[timesheet_df_daily['checkin'].notnull() & timesheet_df_daily['checkout'].notnull()]

    # EMPLOYEE DATA
    # Adjust data type
    employee_df["join_date"] = pd.to_datetime(employee_df["join_date"])
    employee_df["resign_date"] = pd.to_datetime(employee_df["resign_date"])

    # Adjust column name
    employee_cleaned_df = employee_df.rename(columns = {"employe_id":"employee_id"})

    # MERGE DATA
    combined_df = timesheet_df_daily.merge(employee_cleaned_df, on='employee_id', how='left')
    combined_df = combined_df[(combined_df['date'] <= combined_df['resign_date'].fillna(pd.Timestamp('2100-12-31'))) & (combined_df['date'] >= combined_df['join_date'])]
    # print(combined_df.shape)
    # print(combined_df.head())

    # LOAD STAGING DATA
    try:
        combined_df.to_sql("staging_timesheets", engine, if_exists="append", index=False, method='multi')
        process_data_final()
    except IntegrityError as e:
        if isinstance(e.orig, UniqueViolation):
            print("Data for current business date already exists. Please delete existing data and try again.")
        else:
            # If the error is not a UniqueViolation, re-raise the exception
            raise

def process_data_final():
    # TRANSFORM DATA
    # Fetch all data from the staging_timesheets
    query = "SELECT * FROM staging_timesheets"

    # Use pandas to load the query result into a DataFrame
    staging_timesheets = pd.read_sql(text(query), engine.connect())

    monthly_employee_df = staging_timesheets.groupby(['branch_id', 'employee_id', 'year', 'month']).agg(
        total_salary=('salary', 'max'),
        total_work_hour=('work_hour', 'sum')
    ).reset_index()

    monthly_salary_per_hour_df = monthly_employee_df.groupby(['branch_id', 'year', 'month']).agg(
        total_salary=('total_salary', 'sum'),
        total_work_hour=('total_work_hour', 'sum')
        ).reset_index()

    # Calculate monthly salary per hour
    monthly_salary_per_hour_df['salary_per_hour'] = round(monthly_salary_per_hour_df['total_salary']/monthly_salary_per_hour_df['total_work_hour'])
    monthly_salary_per_hour_df['salary_per_hour'] = monthly_salary_per_hour_df['salary_per_hour'].astype('int')

    # Final data to be loaded
    monthly_salary_per_hour_df = monthly_salary_per_hour_df[['branch_id', 'year', 'month', 'salary_per_hour']].sort_values(['year', 'month', 'branch_id'])
    monthly_salary_per_hour_df.to_sql("monthly_salary_per_hour", engine, if_exists="replace", index=False, method='multi')
    print(monthly_salary_per_hour_df.head())

if __name__ == "__main__":
    # Initialize business date
    # business_dt = (datetime.now()-timedelta(days=1)).date() # assume that the script is run after 12am daily
    # business_dt = str(business_dt)
    business_dt = str(sys.argv[1])
    # business_dt = "2019-08-22" # For testing purpose

    if business_dt:
        # Extract from CSV
        employee_df = pd.read_csv("employees.csv")
        timesheet_df = pd.read_csv("timesheets.csv")

        # TIMESHEETS DATA
        # Adjust data type
        timesheet_df["date"] = pd.to_datetime(timesheet_df["date"])

        # Get timesheets for current date
        date_obj = datetime.strptime(business_dt, "%Y-%m-%d")
        timesheet_df_daily = timesheet_df[timesheet_df["date"]==business_dt]

        if not timesheet_df_daily.empty:   
            process_data_staging(employee_df, timesheet_df_daily)
            print("Data processed successfully.")
        else:
            print("No data to be processed for current business date")
    else:
        print(f"Please provide business date. For example: python etl.py 2019-08-21")

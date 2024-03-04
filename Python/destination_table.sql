CREATE TABLE staging_timesheets (
    timesheet_id BIGINT PRIMARY KEY NOT NULL,
    employee_id BIGINT NOT NULL,
    date DATE NOT NULL,
    checkin TIME,
    checkout TIME,
    year INTEGER,
    month INTEGER,
    work_hour FLOAT,
    branch_id BIGINT NOT NULL,
    salary BIGINT NOT NULL,
    join_date DATE NOT NULL,
    resign_date DATE
);
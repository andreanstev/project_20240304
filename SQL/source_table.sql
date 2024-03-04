CREATE TABLE employee (
    employee_id BIGINT NOT NULL,
    branch_id BIGINT NOT NULL,
    salary BIGINT NOT NULL,
    join_date DATE NOT NULL,
    resign_date DATE
);

CREATE TABLE timesheets(
    timesheet_id BIGINT PRIMARY KEY NOT NULL,
    employee_id BIGINT NOT NULL,
    date DATE NOT NULL,
    checkin TIME,
    checkout TIME
);
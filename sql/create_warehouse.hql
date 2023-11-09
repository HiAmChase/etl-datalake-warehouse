create database if not exists reports with dbproperties('location'='/warehouse/reports');

create external table if not exists reports.daily_gross_revenue (
    Name string,
    Category string,
    Sales integer,
    Revenue decimal
)
partitioned by (year string, month string, day string)
stored as parquet;
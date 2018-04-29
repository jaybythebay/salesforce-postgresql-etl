# salesforce-postgresql-etl

A ETL script to move data from Salesforce into a local PostgreSQL Database built using the [simple-salesforce](https://pypi.python.org/pypi/simple-salesforce) package.

## Overview

The script bulk downloads data from Salesforce and loads it into a PostgreSQL Database. The tables to retrieve can be configured by setting the whitelist and blacklist in `settings.py`. If the schema for the table changes the entire table is dropped and all rows are re-loaded.

## Running
To run:
* Configure your environment variables for Salesforce and the target PostgreSQL DB in `settings.py`
* Configure the table prefix, whitelist, blacklist, and log file path in `settings.py`
* run `salesforce_postgresql.py`






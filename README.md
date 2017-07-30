# salesforce-postgresql-etl

A ETL script to move data from Salesforce into a local PostgreSQL Database built using the [simple-salesforce](https://pypi.python.org/pypi/simple-salesforce) package.


## Package Dependencies

Packages required can be installed via pip

~~~~
pip install simple_salesforce
pip install sqlalchemy
pip install sqlalchemy_utils
pip install pandas
pip install logging
pip install pprint
pip install alembic

~~~~

## Overview

The module bulk downloads data from all Salesforce tables and puts it into a PostgreSQL Database. 

Schema changes are not yet accounted for. If a schema change has occured manually drop the table and let it re-create.  This will be fixed in later version.


## Running
To run:
* Create a copy of sample_settings.py named as settings.py and enter the proper credentials
* run salesforce_postgresql.py





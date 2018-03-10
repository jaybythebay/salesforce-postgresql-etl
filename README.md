# salesforce-postgresql-etl

A ETL script to move data from Salesforce into a local PostgreSQL Database built using the [simple-salesforce](https://pypi.python.org/pypi/simple-salesforce) package.

## Overview

The module bulk downloads data from Salesforce tables and puts it into a PostgreSQL Database. The whitelist and blacklist in the `settings.py` file can be adjusted to exclude certain objects or to only retrieve particular objects.

Schema changes are not yet accounted for. If a schema change has occured manually drop the table and let it re-create.  This will be fixed in later version.

## Package Dependencies

Packages need are in the requirements.txt file

## Running
To run:
* Rename sample_settings.py to settings.py and enter the proper credentials
* Adjust the whitelist and blacklists in settings.py if you wish to limit the objects retrieved
* run salesforce_postgresql.py






# Database settings for the target
DATABASE = {
    'drivername': 'postgres',
    'host': 'localhost',
    'port': '5433',
    'username': 'USERNAME',
    'password': 'PASSWORD',
    'database': 'sf_test_dev'
}

# Salesforce API credentials
salesforce_api = {'username': 'USERNAME',
'password': 'PASSWORD',
'security_token': 'SECURITY TOKEN'
}

# The path and name for where to write the log file
log_file_path = '/Users/jayrosenthal/Desktop/salesforce_etl.log'

# Object black list.  These are objects to never include even if dumping all from Salesforce otherwise.  The black list overrides the whitelist.
object_blacklist = ['collaborationgrouprecord', 'contentdocumentlink', 'ideacomment', 'scontrol', 'vote']

# Object white list.  This are objects to use.  If this list contains values then salesforce will only get these objects.
object_whitelist = ['opportunity', 'user', 'account']

# Table prefix.  You must add a prefix to keep tables such as 'user' and 'case' to keep from interferring with PostgreSQL reserved words.
prefix = "salesforce_"

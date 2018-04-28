from os import environ

# Salesforce API credentials
salesforce_api = {'username': environ.get('SALESFORCE_USERNAME'),
                  'password': environ.get('SALESFORCE_PASSWORD'),
                  'security_token': environ.get('SALESFORCE_SECURITY_TOKEN')
                  }

# Database settings for the target PostgreSQL DB
DATABASE = {
    'drivername': 'postgres',
    'host': environ.get('TARGET_HOST'),
    'port': environ.get('TARGET_PORT'),
    'username': environ.get('TARGET_USERNAME'),
    'password': environ.get('TARGET_PASSWORD'),
    'database': environ.get('TARGET_DATABASE')
}

# The path and name for where to write the log file
log_file_path = '/Users/jayrosenthal/Desktop/salesforce_etl.log'

# Object black list.  These are objects tgit o never include even if dumping all from Salesforce otherwise.  The black list overrides the whitelist.
object_blacklist = ['collaborationgrouprecord', 'contentdocumentlink', 'ideacomment', 'scontrol', 'vote']

# Object white list.  This are objects to use.  If this list contains values then salesforce will only get these objects
# object_whitelist = ('opportunity', 'user', 'account', 'pricebookentry', 'opportunitylineitem', 'task', 'contact', 'Product2', 'pricebookentry')
object_whitelist = ['opportunity', 'account']

# Table prefix.  You must add a prefix to keep tables such as 'user' and 'case' to keep from interferring with PostgreSQL reserved words.
prefix = "salesforce_"

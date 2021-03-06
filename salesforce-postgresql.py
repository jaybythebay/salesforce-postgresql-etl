from simple_salesforce import Salesforce
import pandas as pd
import logging
from tabulate import tabulate
from migrate.changeset.schema import rename_table

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.engine import reflection, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import MetaData, Table, Column
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import select, text
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.sqltypes import DateTime, Float, String

import settings

# Authenticate Salesforce
sf = Salesforce(username=settings.salesforce_api['username'],
                password=settings.salesforce_api['password'],
                security_token=settings.salesforce_api['security_token'])


def salesforce_get_objects():

    object_list = []

    try:
        sf.describe()
        logging.info('Retrieved objects from Salesforce')

        for o in sf.describe()["sobjects"]:
            # print o["label"]
            # print o["name"]
            # counter += 1
            # print counter
            object_list.append(o["name"])

    except:
        logging.warning('salesforce desribe object failed')
        object_list = []

    return object_list


def salesforce_table_description(table_name):

    try:
        description = eval("sf." + table_name + ".describe()")
        logging.info('retrieved table description for %s', table_name)
    except:
        logging.warning('failed to retrieve table description for %s', table_name)

    return description


def salesforce_date_column_for_updates(engine, object_name):

    insp = reflection.Inspector.from_engine(engine)
    table_description = insp.get_columns(object_name)

    column_name_list = [c["name"] for c in table_description]

    if 'systemmodstamp' in column_name_list:
        column_name_for_updates = 'systemmodstamp'
    elif 'lastmodifieddate' in column_name_list:
        column_name_for_updates = 'lastmodifieddate'
    elif 'createddate' in column_name_list:
        column_name_for_updates = 'createddate'
    else:
        column_name_for_updates = None

    return column_name_for_updates


def salesforce_column_list(sf_table_description):

    items = list(sf_table_description.items())
    column_lst = []
    salesforce_column_dict = {}


    for i in items:
        if i[0] == "fields":
            field_list = i[1]

            for f in field_list:
                # print f["name"]
                if f["soapType"] == "tns:ID" and  str(f["name"].lower()) == "id":
                    data_type = String(18)
                    primary_key = True
                elif f["soapType"] == "tns:ID":
                    data_type = String(18)
                    primary_key = False
                elif f["soapType"] == "xsd:boolean":
                    data_type = String(5)
                    primary_key = False
                elif f["soapType"] == "xsd:date" or f["soapType"] == "xsd:dateTime" == "xsd:dateTime":
                    data_type = DateTime
                    primary_key = False
                elif f["soapType"] == "xsd:double":
                    data_type = Float
                    primary_key = False
                elif f["soapType"] == "xsd:int":
                    data_type = Float
                    primary_key = False
                elif f["soapType"] == "xsd:string":
                    data_type = String(f["length"])
                    primary_key = False

                column_lst.append(Column(name=str(f["name"].lower()), type_=data_type, primary_key=primary_key))

    logging.info('created column list: %s', column_lst)
    return column_lst


def get_data_last_updated_timestamp(sess, date_column):

    max_in_db = sess.query(func.max(date_column)).one()[0]

    if max_in_db is None:
        max_in_db = '1999-01-01T00:00:00.000+0000'
    else:
        max_in_db = format_datetime(max_in_db)

    logging.info('max date to use for query is: %s', max_in_db)

    return max_in_db


def format_datetime(datetime):
    format = "%Y-%m-%dT%H:%M:%S.000+0000"
    return datetime.strftime(format)


def get_and_load_data(engine, metadata, sf_table_name, object_name, table_definition, date_column, max_in_db):

    # table = metadata.tables[object_name]
    # stmt = select([table], table.c.systemmodstamp >= text(max_in_db))
    stmt = select([table_definition], date_column >= text(max_in_db))
    logging.info('database select statement from sqlalchemy: %s', stmt)


    string_query = str(stmt)
    salesforce_query = string_query.replace(object_name, sf_table_name)
    logging.info('salesforce query to use with table name updated: %s', salesforce_query)

    data = sf.query(salesforce_query)
    lst = data["records"]

    df = parse_data(lst)
    delete_updated_records(df, object_name, metadata, engine)
    load_data(df, object_name, engine)

    if data.has_key("nextRecordsUrl"):
        next_records_url = data["nextRecordsUrl"]
    done = data["done"]

    while done is False:
        # print "Query: ", next_records_url
        data = sf.query_more(next_records_url, True)
        lst = data["records"]

        # Parse the dictionary and upload the data to the DB
        df = parse_data(lst)
        delete_updated_records(df, object_name, metadata, engine)
        load_data(df, object_name, engine)

        if data.has_key("nextRecordsUrl"):
            next_records_url = data["nextRecordsUrl"]
        done = data["done"]


def parse_data(lst):

    values = []

    for l in lst:
        # print l
        l.pop('attributes', None)
        values.append(l)

    df = pd.DataFrame(values)
    df.rename(columns=lambda x: str(x).lower(), inplace=True)
    # print df
    return df


def load_data(df, object_name, engine, print_df=False):

    if print_df:
        print tabulate(df, headers='keys', tablefmt='psql')

    try:
        df.to_sql(object_name, engine, if_exists='append', index=False)
        logging.info(' %s - data loaded %s rows', object_name.upper(), df.shape[0])
        print object_name.upper(), "loaded", df.shape[0], "row(s)"

    except Exception as e:
        logging.warning(' %s - error loading data - message: %s', object_name.upper(), e.message)
        print(e.message)


def delete_updated_records(df, object_name, metadata, engine):

    table = metadata.tables[object_name]

    if not df.empty:
        id_list = df['id'].tolist()
        delete_statement = table.delete().where(table.c.id.in_(id_list))
        logging.info(' %s - delete statement %s', object_name.upper(), delete_statement)
        engine.execute(delete_statement)


def objects_to_load():

    # Gets list of Salesforce Objects
    object_list = salesforce_get_objects()
    object_list = [o.lower() for o in object_list]

    logging.info('salesforce object list %s', object_list)

    if len(settings.object_whitelist) != 0:
        object_list = [o.lower() for o in settings.object_whitelist]
        logging.info('using the object whitelist: %s', settings.object_whitelist)

    if len(settings.object_blacklist) != 0:
        object_blacklist = [o.lower() for o in settings.object_blacklist]
        object_list = [o for o in object_list if o.lower() not in object_blacklist]
        logging.info('removing objects from the blacklist: %s', settings.object_blacklist)
        print settings.object_blacklist

    logging.info('object list to load data for %s', object_list)

    return object_list


def postgresql_create_table(metadata, engine, columns, object_name):
    table = Table(object_name, metadata, *columns, extend_existing=True)
    table.create(engine, checkfirst=True)
    logging.info('created table in database: %s', object_name)


def check_schemas(engine, object_name):
    inspector = inspect(engine)
    tmp_table = inspector.get_columns('tmp')
    existing_table = inspector.get_columns(object_name)

    tmp_table_dict = {}
    for t in tmp_table:
        tmp_table_dict[t["name"]] = {"default": t["default"], "autoincrement": t["autoincrement"], "type": str(t["type"]), "nullable": t["nullable"]}

    existing_table_dict = {}
    for e in existing_table:
        existing_table_dict[e["name"]] = {"default": e["default"], "autoincrement": e["autoincrement"], "type": str(e["type"]), "nullable": e["nullable"]}

    logging.info(' %s - tmp table dict %s', object_name.upper(), tmp_table_dict)
    logging.info(' %s - existing table dict %s', object_name.upper(), existing_table_dict)
    return cmp(tmp_table_dict, existing_table_dict)

    tmp.rename(object_name)


def postgresql_drop_table(engine, metadata, object_name):
    table_to_drop = metadata.tables[object_name]
    table_to_drop.drop(engine, checkfirst=True)


def main():

    logging.basicConfig(filename=settings.log_file_path,
                        format='%(asctime)s %(levelname)s:%(message)s',
                        datefmt='%Y-%m-%d %I:%M:%S',
                        level=logging.DEBUG)

    # This successfully connects to Postgres and Reads Data from a Table
    # engine = create_engine('postgresql+psycopg2://postgres:jay@localhost/test')
    engine = create_engine(URL(**settings.DATABASE))

    if not database_exists(engine.url):
        create_database(engine.url)

    logging.info('connected to and created if needed: %s', engine.url)

    session = sessionmaker()
    session.configure(bind=engine)
    sess = session()

    metadata = MetaData()
    metadata = MetaData(engine, reflect=True)

    object_list = objects_to_load()
    prefix = settings.prefix

    for object_name in object_list:

        sf_table_name = object_name
        object_name = prefix + sf_table_name

        # Get Table Description From Salesforce and set the queryable variable
        sf_table_description = salesforce_table_description(sf_table_name)
        queryable = sf_table_description["queryable"]
        logging.info(' %s - object queryable %s', object_name.upper(), queryable)

        if queryable is True:
            print object_name.upper()

            # Parse Salesforce Columns to list
            columns = salesforce_column_list(sf_table_description)

            # create a table if not in the database
            if not engine.dialect.has_table(engine, object_name):
                postgresql_create_table(metadata, engine, columns, object_name)
                logging.info(' %s - created the table', object_name.upper())

            # create a table with the new schema
            # check if the table schema has changed
            # if it's changed drop and re-create the table
            else:
                print object_name.upper(), "checking for schema changes"
                postgresql_create_table(metadata, engine, columns, object_name='tmp')
                match = check_schemas(engine, object_name)
                logging.info(' %s - schema match score = %s', object_name.upper(), match)

                if match != 0:
                    postgresql_drop_table(engine, metadata, object_name)
                    rename_table(table='tmp', name=object_name, engine=engine)
                    print object_name.upper(), "schema changes applied"
                    logging.info(' %s - schema changes applied', object_name.upper())
                else:
                    postgresql_drop_table(engine, metadata, object_name='tmp')
                    print object_name.upper(), "no schema changes needed"
                    logging.info(' %s - no schema changes needed', object_name.upper())

            # Select the proper date field to use for identifying new data
            date_column_for_updates = salesforce_date_column_for_updates(engine, object_name)
            logging.info(' %s - selected date column for table updates: %s', object_name.upper(), date_column_for_updates)

            if date_column_for_updates is not None:

                # Reset the metadata
                metadata = MetaData()
                metadata = MetaData(engine, reflect=True)

                # Create metadata for queries
                table_definition = Table(object_name, metadata, autoload=True)
                date_column = table_definition.columns[date_column_for_updates]

                # Get max in database
                max_in_db = get_data_last_updated_timestamp(sess, date_column)

                # Get Data and load data
                get_and_load_data(engine, metadata, sf_table_name, object_name, table_definition, date_column, max_in_db)


if __name__ == '__main__':
    main()




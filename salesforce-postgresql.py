from simple_salesforce import Salesforce
from sqlalchemy import *
from sqlalchemy.engine.url import URL
from sqlalchemy_utils import database_exists, create_database, get_type
from sqlalchemy import select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import reflection
import pandas as pd
import logging


import settings

from sqlalchemy import (create_engine, MetaData, Column, Integer, String, Table)


# Authenticate Salesforce
sf = Salesforce(username=settings.salesforce_api['username'],
                password=settings.salesforce_api['password'],
                security_token=settings.salesforce_api['security_token'])



def salesforce_get_objects():

    object_list = []

    sf.describe()

    for o in sf.describe()["sobjects"]:
        # print o["label"]
        # print o["name"]
        # counter += 1
        # print counter
        object_list.append(o["name"])

    return object_list


def salesforce_table_description(table_name):
    description = eval("sf." + table_name + ".describe()")

    # print table_name, description["updateable"]

    # for key in description.iteritems():
    #     print key
    #
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

    return column_lst



def get_data_last_updated_timestamp(sess, date_column):
    # def get_data_last_updated_timestamp(sess, metadata, object_name, date_column_for_updates):

    # table_name = Table(object_name, metadata, autoload=True)
    # date_column = table_name.columns[date_column_for_updates]

    max_in_db = sess.query(func.max(date_column)).one()[0]

    if max_in_db is None:
        max_in_db = '1999-01-01T00:00:00.000+0000'
    else:
        max_in_db = format_datetime(max_in_db)

    return max_in_db


def format_datetime(datetime):
    format = "%Y-%m-%dT%H:%M:%S.000+0000"
    return datetime.strftime(format)


def get_and_load_data(engine, metadata, sf_table_name, object_name, table_definition, date_column, max_in_db):

    # table = metadata.tables[object_name]
    # stmt = select([table], table.c.systemmodstamp >= text(max_in_db))
    stmt = select([table_definition], date_column >= text(max_in_db))
    print stmt
    print type(stmt)

    string_query = str(stmt)
    print "String query before: ", string_query
    print "object_name: ", object_name, " sf_table_name: ", sf_table_name

    string_query = string_query.replace(object_name, sf_table_name)
    print string_query

    data = sf.query(string_query)
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


def load_data(df, object_name, engine):

    try:
        df.to_sql(object_name, engine, if_exists='append', index=False)
        print "data loaded for:", object_name

    except Exception as e:
        print(e.message)


def delete_updated_records(df, object_name, metadata, engine):

    table = metadata.tables[object_name]

    if not df.empty:
        idlist = df['id'].tolist()
        # print idlist

        delete_statement = table.delete().where(table.c.id.in_(idlist))
        # print delete_statement
        engine.execute(delete_statement)


def main():

    # This successfully connects to Postgres and Reads Data from a Table
    # engine = create_engine('postgresql+psycopg2://postgres:jay@localhost/test')
    engine = create_engine(URL(**settings.DATABASE))

    if not database_exists(engine.url):
        create_database(engine.url)

    print "DATABASE CREATED"

    session = sessionmaker()
    session.configure(bind=engine)
    sess = session()

    # data = pd.read_sql('SELECT count(1) FROM accountfeed', engine)
    # print data

    metadata = MetaData()

    # https://docs.python.org/2/howto/logging.html
    # logging.basicConfig(filename='example.log', level=logging.DEBUG)
    # logging.debug('This message should go to the log file')
    # logging.info('So should this')
    # logging.warning('And this, too')

    # metadata = MetaData(bind=engine)

    # # Gets list of Salesforce Objects
    object_list = salesforce_get_objects()
    object_list = [o.lower() for o in object_list]
    object_list = ('opportunity', 'user', 'account')
    exclude_objects = ('collaborationgrouprecord', 'contentdocumentlink', 'ideacomment', 'scontrol', 'vote')

    object_list = [o for o in object_list if o not in exclude_objects]

    # object_list = exclude_objects

    print object_list

    prefix = "salesforce_"

    for object_name in object_list:
        print "---------------------"

        sf_table_name = str(object_name).lower()
        object_name = prefix + sf_table_name
        print sf_table_name


        # Get Table Description From Salesforce
        sf_table_description = salesforce_table_description(sf_table_name)
        # print sf_table_description
        print "Got table description"
        queryable = sf_table_description["queryable"]
        print queryable

        if queryable is True:
            # Parse Salesforce Columns to list
            columns = salesforce_column_list(sf_table_description)
            print "Created Column DDL"

            # Create Tables in PostgreSQL DB
            table = Table(object_name, metadata, *columns, extend_existing=True)
            # metadata.create_all(engine)
            table.create(engine, checkfirst=True)
            print "Created table"

            # Select the proper date field to use for identifying new data
            date_column_for_updates = salesforce_date_column_for_updates(engine, object_name)
            print "Date Column For Updates:", date_column_for_updates

            # # Check schema
            # postgresql_column_list(metadata, object_name)
            # check_schema_change()

            if date_column_for_updates is not None:
                # Create metadata for queries
                table_definition = Table(object_name, metadata, autoload=True)
                date_column = table_definition.columns[date_column_for_updates]

                # Get max in database
                max_in_db = get_data_last_updated_timestamp(sess, date_column)
                print "Got the max"

                # Get Data and load data
                get_and_load_data(engine, metadata, sf_table_name, object_name, table_definition, date_column, max_in_db)


if __name__ == '__main__':
    main()




from simple_salesforce import Salesforce
# from credentials_salesforce import salesforce_api
# from credentials_postgresql import DATABASE
from sqlalchemy import *
from sqlalchemy.engine.url import URL
from sqlalchemy_utils import database_exists, create_database, get_type
from sqlalchemy import select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import reflection
import pandas as pd
import logging


import settings

# These packages were imported for the Schema comparison which doesn't yet work
from alembic.migration import MigrationContext
from alembic.autogenerate import compare_metadata
from sqlalchemy.schema import SchemaItem
from sqlalchemy.types import TypeEngine
from sqlalchemy import (create_engine, MetaData, Column,
        Integer, String, Table)
import pprint


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


def postgresql_column_list(metadata, object):
    # for table in metadata.tables:
    #     print table

    # for table in metadata.tables:
    #     print table

    # print "get columns function"
    # print insp.get_columns(metadata.tables[object_name])
    # print insp.get_columns(table_c)

        # postgres_column_dict = {}
        #
        # results = metadata.tables[object]
        #
        # for col in results.c:
        #     # print col, col.type
        #     # print col, col.name, col.type, col.nullable, col.primary_key, col.foreign_keys
        #     postgres_column_dict[col.name] = get_type(col)
        #
        # # print type(table_column_dict)
        # return postgres_column_dict
    pass


def check_schema_change(columns_postgresql, columns_salesforce, engine):
    # print type(columns_postgresql)
    # print columns_postgresql
    # print type(columns_salesforce)
    # print columns_salesforce
    # for c in columns_salesforce:
    #     print type(c), c, c.type

    insp = reflection.Inspector.from_engine(engine)

    print "get columns function"
    print insp.get_columns(columns_postgresql)
    print insp.get_columns(columns_salesforce)
    print type(columns_postgresql)
    print type(columns_salesforce)

    # salesforce_column_dict = {}
    #
    # for col in columns_salesforce:
    #     # print col, col.type
    #     # print col, col.name, col.type, col.nullable, col.primary_key, col.foreign_keys
    #     salesforce_column_dict[col.name] = col.type
    #
    # print salesforce_column_dict
    # print columns_postgresql

    # if engine.dialect.has_table(connection, table_name):

    # if engine.dialect.has_table(engine, "tmp_salesforce_table_schema"):
    #     # Drop the tmp table
    #     salesforce_schema_test = Table("tmp_salesforce_table_schema", metadata)
    #     salesforce_schema_test.drop(engine)
    #
    # # Refresh the metadata and re-create the tmp table
    # metadata = MetaData()
    # salesforce_schema_test = Table("tmp_salesforce_table_schema", metadata, *columns_salesforce)
    # salesforce_schema_test.create(engine)

    # Table(object_name, metadata, *columns_salesforce)

    # mc = MigrationContext.configure(engine.connect())

    mc = MigrationContext.configure(engine.connect(), )

    # include_object = None,

    meta = metadata.reflect(engine, only=[object_name], extend_existing=True)
    print meta


    # diff = compare_metadata(mc, metadata)
    # diff = compare_metadata(mc, metadata.reflect(engine, only=[object_name]))
    # pprint.pprint(diff, indent=2, width=20)




    # insp = reflection.Inspector.from_engine(engine)
    # print "printing inspection"
    # print insp.get_columns("tmp_salesforce_table_schema")

    # new_columns = insp.get_columns("tmp_salesforce_table_schema")
    # old_columns = insp.get_columns(object_name)

    # new_columns = Table("tmp_salesforce_table_schema", metadata)
    # old_columns = Table(object_name, metadata)
    #
    # print type(new_columns)
    # print type(old_columns)

    # if new_columns == old_columns:
    #     print "Columns Match: ", object_name
    # else:
    #     print "Columns Are Different: ", object_name
    #




    # new_object = "new_sf_object"

    # Create Tables in PostgreSQL DB
    # table_c = Table(str(object_name).lower(), metadata, *columns_salesforce, extend_existing=True)
    # table_c = Table(new_object, metadata, *columns_salesforce)

    # mytable = Table("mytable", metadata, *columns_salesforce, extend_existing=True)

    # print "print table", mytable

    # insp = reflection.Inspector.from_engine(engine)
    # print "printing inspection"
    # print insp.get_columns(object_name)



    # Check for schema changes and delete table if they don't match.
    # columns_postgresql = postgresql_column_list(metadata.tables[object_name], table_c, engine)
    # check_schema_change(metadata.tables[object_name], table_c, engine)
    # print tables_changed


    # print "get columns function"
    # print insp.get_columns(metadata.tables[object_name])


    # if metadata.tables[object_name] == table_c:
    #     print "printing metadata etc", metadata.tables[object_name]
    #     print "these suckers match"
    # else:
    #     print "something changed"
    # # metadata.create_all(engine)
    # table_c.create(engine, checkfirst=True)
    # print "created table"


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


def get_and_load_data(engine, metadata, object_name, table_definition, date_column, max_in_db):

    # table = metadata.tables[object_name]
    # stmt = select([table], table.c.systemmodstamp >= text(max_in_db))
    stmt = select([table_definition], date_column >= text(max_in_db))
    print stmt

    data = sf.query(stmt)
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
    # metadata = MetaData(bind=engine)

    # # Gets list of Salesforce Objects
    object_list = salesforce_get_objects()
    object_list = [o.lower() for o in object_list]
    # object_list = ('account', 'accountpartner', 'accountshare', 'accounthistory', 'activityhistory')
    # print object_list

    for object_name in object_list:
        print "---------------------"
        print object_name

        # Get Table Description From Salesforce
        sf_table_description = salesforce_table_description(object_name)
        # print sf_table_description
        print "Got table description"
        queryable = sf_table_description["queryable"]

        if queryable is True:
            # Parse Salesforce Columns to list
            columns = salesforce_column_list(sf_table_description)
            print "Created Column DDL"

            # Create Tables in PostgreSQL DB
            table = Table(str(object_name).lower(), metadata, *columns, extend_existing=True)
            # metadata.create_all(engine)
            table.create(engine, checkfirst=True)
            print "Created table"

            # Select the proper date field to use for identifying new data
            date_column_for_updates = salesforce_date_column_for_updates(engine, object_name)
            print "Date Coluumn For Updates:", date_column_for_updates

            # # Check schema
            # postgresql_column_list(metadata, object_name)
            # check_schema_change()

            # Create metadata for queries
            table_definition = Table(object_name, metadata, autoload=True)
            date_column = table_definition.columns[date_column_for_updates]

            # Get max in database
            max_in_db = get_data_last_updated_timestamp(sess, date_column)
            print "Got the max"

            # Get Data and load data
            get_and_load_data(engine, metadata, object_name, table_definition, date_column, max_in_db)


if __name__ == '__main__':
    main()




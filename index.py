import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def createTable(engine, tablename):
    try:
        with engine.connect() as con:
            con.execute("call createCountryTable(\"" + tablename + "\")")
    except Exception as e:
        print(e)


def createTables(engine, inspector, db, distinct_countries, existing_tables):
    for tbl in distinct_countries:
        if tbl not in existing_tables:
            print("trying to create " + tbl)
            try:
                createTable(engine, tbl)
                print("Created")
            except Exception as e:
                print(e)
        else:
            print(tbl + " already exists")


def getTables(engine):
    # This function is Performing database inspection so it will refresh the table from database
    inspector = inspect(engine)

    # method get_table_names for getting list of tables from "incu" database
    all_tables = [tbl for tbl in inspector.get_table_names(schema=db)]

    return all_tables, inspector


df = pd.read_csv('Customer.txt', sep="|", header=None)

# without skipping row check for header
is_header = df.iloc[0, 0]
is_trailer = df.iloc[df.shape[0] - 1, 0]

# checking if Header Records exists
if is_header == 'H':
    df.drop(df.head(1).index, inplace=True)



# Naming the Columns
df.columns = ["D",
              "Customer_Name", "Customer_ID",
              "Open_Date", "Last_Consulted_Date",
              "Vaccination_Id", "Dr_Name",
              "State", "Country",
              "DOB", "Is_Active"]



# Dropping D columns as it of no use
del df['D']




# customerID is considered as float by pandas, so casting to int
df['Customer_ID'] = df['Customer_ID'].apply(np.int64)

# Setting customerID as index for faster operations
df.set_index('Customer_ID')

# here date is treated as string
print(df.info(), end="\n\n")

# Converting String to Dates
try:
    df['Open_Date'] = pd.to_datetime(
        df['Open_Date'], format='%Y%m%d')
    df['Last_Consulted_Date'] = pd.to_datetime(
        df['Last_Consulted_Date'], format='%Y%m%d')
    df['DOB'] = pd.to_datetime(
        df['DOB'], format='%d%m%Y')
except Exception as e:
    print(e)

# here date is treated as date
print(df.info(), end="\n\n")
print(df)

# lower is applied here and not in `distinct_countries` to fetch rows

df['Country'] = df['Country'].str.upper()
# Getting Distinct Countries
distinct_countries = df['Country'].drop_duplicates()

print("\nDistinct Countries:\n", distinct_countries)

# Connecting to Database
print()
#db = "incubyte"
db = "incu"
try:
    engine = create_engine(
     #   "mysql+mysqlconnector://root:password@localhost/" + db)
        "mysql+mysqlconnector://root:root@localhost/" + db)
    engine.connect()
    print("Database Connected")
except Exception as e:
    print(e)

# Getting inspector and list of tables from "incubyte" database
existing_tables, inspector = getTables(engine)
print("Existing Tables:", existing_tables)

# creating tables that does not exists in distinct_countries
createTables(engine, inspector, db, distinct_countries, existing_tables)

# Getting inspector and list of tables from "incubyte" database
existing_tables, inspector = getTables(engine)
print("Existing Tables:", existing_tables)

# inserting records as per country
for country in distinct_countries:
    my_filt = (df['Country'] == country)
    try:
        print("Inserting Records in " + country)

        # `to_sql` this will create table if table does not exists,

        if country in existing_tables:
            df[my_filt].to_sql(
                name=country, con=engine,
                if_exists='replace', index=False)
            print("Inserted")
        else:
            print(country + " table does Not exists")
    except Exception as e:
        print(e)
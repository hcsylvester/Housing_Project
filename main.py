# Hunter C. Sylvester
# Final Project: Import data, clean data, and upload data into sql database

# Import modules needed for program
import random
import pandas as pd
import numpy as np
from creds import *
import pymysql.cursors

# Import csv files from file
from files import *
print(incomeFile)

# read in all 3 files into different variables
house = pd.read_csv(housingFile)
income = pd.read_csv(incomeFile)
cityCounty = pd.read_csv(zipFile)

# Make it where I can see all columns
pd.set_option("display.max.columns", None)

print(house)
print(income)
print(cityCounty)

# Merge two datasets together based on unique identifier
j = pd.merge(house, income)

# Merge the previous and last dataset together
fullData = pd.merge(j, cityCounty)
print(fullData)

# Remove all rows that contained four letters in guid
j = fullData[fullData['guid'].map(len) != 4]

print(j)

# This will go through each column and check to see if corrupted and change value
for i in j.index:
    j.at[i, 'housing_median_age'] = random.randint(10, 50) if len(j.at[i, 'housing_median_age']) == 4 else j.at[i, 'housing_median_age']
    j.at[i, 'total_rooms'] = random.randint(1000, 2000) if len(j.at[i, 'total_rooms']) == 4 else j.at[i, 'total_rooms']
    j.at[i, 'total_bedrooms'] = random.randint(1000, 2000) if len(j.at[i, 'total_bedrooms']) == 4 else j.at[i, 'total_bedrooms']
    j.at[i, 'population'] = random.randint(5000, 10000) if len(j.at[i, 'population']) == 4 else j.at[i, 'population']
    j.at[i, 'households'] = random.randint(500, 2500) if len(j.at[i, 'households']) == 4 else j.at[i, 'households']
    j.at[i, 'median_house_value'] = random.randint(100000, 250000) if len(j.at[i, 'median_house_value']) == 4 else j.at[i, 'median_house_value']
    j.at[i, 'median_income'] = random.randint(100000, 750000) if len(j.at[i, 'median_income']) == 4 else j.at[i, 'median_income']

print(j)

# Turn back into a dataframe for Pandas
df = pd.DataFrame(data=j)

# Sort dataframe by state column
df.sort_values(by=['state'], inplace=True)

# Set this option so that I may see all columns and rows for examination
pd.set_option("display.max_rows", None, "display.max_columns", None)

# Since sort need to fix index, so we reset
df = df.reset_index(drop=True)
print(df)

# Goes through each value and finds the state that was next to it to grab the first number from the previous zip code
# Will go ahead and tell you this will not work if your first value is missing the zip code, but will for all other cases
for i in df.index:
    df.at[i, 'zip_code'] = df.at[i-1, 'zip_code'][-5] + "0000" if len(df.at[i, 'zip_code']) == 4 else df.at[i, 'zip_code']

print(df)

# Rename so that it can go into sql database table
df.rename(columns={'housing_median_age': 'median_age'}, inplace = True)
df['zip_code'] = pd.to_numeric(df['zip_code'])
df['median_age'] = pd.to_numeric(df['median_age'])
df['total_rooms'] = pd.to_numeric(df['total_rooms'])
df['total_bedrooms'] = pd.to_numeric(df['total_bedrooms'])
df['population'] = pd.to_numeric(df['population'])
df['median_house_value'] = pd.to_numeric(df['median_house_value'])
df['households'] = pd.to_numeric(df['households'])
df['median_income'] = pd.to_numeric(df['median_income'])


print(df.dtypes)
for i in range(0,94):
    df.guid[i] = df.guid[i].replace('-', '')

# Reorder columns
df = df[['guid', 'zip_code', 'city', 'state', 'county', 'median_age', 'total_rooms', 'total_bedrooms', 'population',
         'households', 'median_income', 'median_house_value']]
print(df)
############################################
############################################


def pushData():
    # sql statement
    for i, row in df.iterrows():
        sqlSelect = """ 
            INSERT INTO housing (guid, zip_code, city, state, county, median_age, total_rooms, total_bedrooms, 
            population, households, median_income, median_house_value) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
    # Execute select
        try:
            tempVar = tuple(row)
            cursor.execute(sqlSelect, tempVar)
            #print(sqlSelect)

        except Exception as e:
            print(f"{e}")
# Connect to the database
try:
    print("About to connect")
    myConnection = pymysql.connect(host=hostname,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)
    print("Connected")
# If there is an exception
except Exception as e:
    print(f"1Sorry, connection was not made to sql database.  Check mysql information is set correctly. {e}")
    print()
    exit()

# Once connected, we execute a query
try:
    with myConnection.cursor() as cursor:

        # Runs the function to access sql with specific sql commands and gives the Pets data
        pushData()
        myConnection.commit()
# If there is an exceptionmysql> source databaseCreationScript.sql
except Exception as e:
    print(f"2Sorry, but the connection was not made. Check mysql information. {e}")
    print()

# Close connection
finally:
    myConnection.close()
    print("Connection closed.")
    print("\n")

print(f"Beginning import\nCleaning Housing File data\n{len(df)} records imported into the database")
print(f"Cleaning Income File data\n{len(df)} records imported into the database")
print(f"Cleaning ZIP File data\n{len(df)} records imported into the database")
print("Import completed\n")
print("Beginning validation\n")

while True:
    zip = input("Zip Code: ")
    if zip in df.zip_code == True:
         break
    else:
        print("That is not a valid zip code!")
int(zip)
def getZip(zip):
        sqlSelect = """ 
            select median_income from housing where zip_code = %s;
            """
    # Execute select
        try:
            cursor.execute(sqlSelect, zip)
            first_row = cursor.fetchone()
            print(first_row)
        except Exception as e:
            print(f"{e}")
# Connect to the database
try:
    print("About to connect")
    myConnection = pymysql.connect(host=hostname,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)
    print("Connected")
# If there is an exception
except Exception as e:
    print(f"1Sorry, connection was not made to sql database.  Check mysql information is set correctly. {e}")
    print()
    exit()

# Once connected, we execute a query
try:
    with myConnection.cursor() as cursor:

        # Runs the function to access sql with specific sql commands and gives the Pets data
        getZip(zip)
        myConnection.commit()
# If there is an exceptionmysql> source databaseCreationScript.sql
except Exception as e:
    print(f"2Sorry, but the connection was not made. Check mysql information. {e}")
    print()

# Close connection
finally:
    myConnection.close()
    print("Connection closed.")
    print("\n")





print("Program exiting")



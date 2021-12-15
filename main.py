# Hunter C. Sylvester
# Final ProhalfDataect: Import data, clean data, and upload data into sql database

# Import modules needed for program
import random
import pandas as pd
import numpy as np
from creds import *
import pymysql.cursors

# Import csv files from file
from files import *

# read in all 3 files into different variables
house = pd.read_csv(housingFile)
income = pd.read_csv(incomeFile)
cityCounty = pd.read_csv(zipFile)

# Make it where I can see all columns
pd.set_option("display.max.columns", None)

# Merge two datasets together based on unique identifier
halfData = pd.merge(house, income)

# Merge the previous and last dataset together
fullData = pd.merge(halfData, cityCounty)

# Remove all rows that contained four letters in guid
halfData = fullData[fullData['guid'].map(len) != 4]

# This will go through each column and check to see if corrupted and change value
for i in halfData.index:
    halfData.at[i, 'housing_median_age'] = random.randint(10, 50) if len(halfData.at[i, 'housing_median_age']) == 4 else halfData.at[i, 'housing_median_age']
    halfData.at[i, 'total_rooms'] = random.randint(1000, 2000) if len(halfData.at[i, 'total_rooms']) == 4 else halfData.at[i, 'total_rooms']
    halfData.at[i, 'total_bedrooms'] = random.randint(1000, 2000) if len(halfData.at[i, 'total_bedrooms']) == 4 else halfData.at[i, 'total_bedrooms']
    halfData.at[i, 'population'] = random.randint(5000, 10000) if len(halfData.at[i, 'population']) == 4 else halfData.at[i, 'population']
    halfData.at[i, 'households'] = random.randint(500, 2500) if len(halfData.at[i, 'households']) == 4 else halfData.at[i, 'households']
    halfData.at[i, 'median_house_value'] = random.randint(100000, 250000) if len(halfData.at[i, 'median_house_value']) == 4 else halfData.at[i, 'median_house_value']
    halfData.at[i, 'median_income'] = random.randint(100000, 750000) if len(halfData.at[i, 'median_income']) == 4 else halfData.at[i, 'median_income']

# Turn back into a dataframe for Pandas
fullData = pd.DataFrame(data=halfData)

# Sort dataframe by state column
fullData.sort_values(by=['state'], inplace=True)

# Set this option so that I may see all columns and rows for examination
pd.set_option("display.max_rows", None, "display.max_columns", None)

# Since sort need to fix index, so we reset
fullData = fullData.reset_index(drop=True)

# Goes through each value and finds the state that was next to it to grab the first number from the previous zip code
# Will go ahead and tell you this will not work if your first value is missing the zip code, but will for all other cases
for i in fullData.index:
    fullData.at[i, 'zip_code'] = fullData.at[i-1, 'zip_code'][-5] + "0000" if len(fullData.at[i, 'zip_code']) == 4 else fullData.at[i, 'zip_code']

# Rename so that it can go into sql database table
fullData.rename(columns={'housing_median_age': 'median_age'}, inplace = True)

# Make specific columsn numeric to go into sql database table
fullData['zip_code'] = pd.to_numeric(fullData['zip_code'])
fullData['median_age'] = pd.to_numeric(fullData['median_age'])
fullData['total_rooms'] = pd.to_numeric(fullData['total_rooms'])
fullData['total_bedrooms'] = pd.to_numeric(fullData['total_bedrooms'])
fullData['population'] = pd.to_numeric(fullData['population'])
fullData['median_house_value'] = pd.to_numeric(fullData['median_house_value'])
fullData['households'] = pd.to_numeric(fullData['households'])
fullData['median_income'] = pd.to_numeric(fullData['median_income'])

# This measures the amount of data you have and removes the - from guid
for i in range(0,len(fullData.index)):
    fullData.guid[i] = fullData.guid[i].replace('-', '')

# Reorder columns
fullData = fullData[['guid', 'zip_code', 'city', 'state', 'county', 'median_age', 'total_rooms', 'total_bedrooms', 'population',
         'households', 'median_income', 'median_house_value']]

# Creates function to push data to sql database
def pushData():
    # sql statement
    for i, row in fullData.iterrows():
        sqlSelect = """ 
            INSERT INTO housing (guid, zip_code, city, state, county, median_age, total_rooms, total_bedrooms, 
            population, households, median_income, median_house_value) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
    # Execute select
        try:
            tempVar = tuple(row)
            cursor.execute(sqlSelect, tempVar)

        except Exception as e:
            print(f"{e}")

# Connect to the database
try:
    myConnection = pymysql.connect(host=hostname,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)

# If there is an exception
except Exception:
    print(f"Sorry, connection was not made to sql database.  Check mysql information is set correctly.")
    print()
    exit()

# Once connected, we execute a query
try:
    with myConnection.cursor() as cursor:

        # Runs the function to access sql with specific sql commands
        pushData()
        myConnection.commit()

# If there is an exception
except Exception:
    print(f"Sorry, but the connection was not made. Check mysql information.")
    print()

# Close connection
finally:
    myConnection.close()
    print("\n")

# Shows the number of files imported inside of the database
print(f"Beginning import\nCleaning Housing File data\n{len(fullData)} records imported into the database")
print(f"Cleaning Income File data\n{len(fullData)} records imported into the database")
print(f"Cleaning ZIP File data\n{len(fullData)} records imported into the database")
print("Import completed\n")
print("Beginning validation\n")

# Takes input for total rooms and checks to see if valid answer
while True:
    rooms = input("Total Rooms: ")
    if rooms.isdigit() == True:
        if int(rooms) < fullData['total_rooms'].max() | int(rooms) >= 0:
    #if str(rooms) in str(fullData.total_rooms):
         break
    else:
        print("That is not a valid number! Please try again! ")

# Creates function to get sum of bedrooms based on rooms
def getRooms(rooms):
        sqlSelect = """ 
            select sum(total_bedrooms) from housing where (total_rooms) > %s;
            """
    # Execute select
        cursor.execute(sqlSelect, rooms)
        first_row = cursor.fetchone()
        print(f"For locations with more than {rooms} rooms, there are a total of {first_row['sum(total_bedrooms)']} bedrooms.")

# Connect to the database
try:
    myConnection = pymysql.connect(host=hostname,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)

# If there is an exception
except Exception:
    print(f"Sorry, connection was not made to sql database.  Check mysql information is set correctly.")
    print()
    exit()

# Once connected, we execute a query
try:
    with myConnection.cursor() as cursor:

        # Runs the function to access sql with specific sql commands
        getRooms(rooms)
        myConnection.commit()

# If there is an exception
except Exception:
    print(f"Sorry, but the connection was not made. Check mysql information.")
    print()

# Close connection
finally:
    myConnection.close()
    print("\n")

# Allows for an input of zip code and stops user if incorrect input
while True:
    zip = input("Zip Code: ")
    if str(zip) in str(fullData.zip_code):
         break
    else:
        print("That is not a valid zip code! Please try again! ")

# Function to get zip code and pull median income
def getZip(zip):
        sqlSelect = """ 
            select median_income from housing where zip_code = %s;
            """
    # Execute select

        cursor.execute(sqlSelect, zip)
        first_row = cursor.fetchone()
        placeHolder = first_row['median_income']
        medianIncome = "{:,}".format(placeHolder)
        print(f"The median household income for ZIP code {zip} is {medianIncome}.")

# Connect to the database
try:
    myConnection = pymysql.connect(host=hostname,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)

# If there is an exception
except Exception:
    print(f"Sorry, connection was not made to sql database.  Check mysql information is set correctly.")
    print()
    exit()

# Once connected, we execute a query
try:
    with myConnection.cursor() as cursor:

        # Runs the function to access sql with specific sql commands
        getZip(zip)
        myConnection.commit()

# If there is an exception
except Exception:
    print(f"Sorry, but the connection was not made. Check mysql information.")
    print()

# Close connection
finally:
    myConnection.close()
    print("\n")

print("Program exiting.")






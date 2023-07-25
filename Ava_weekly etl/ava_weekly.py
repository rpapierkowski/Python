import mysql.connector  as connection
import pandas as pd
import datetime
import numpy as np
import csv


file_dir = 'G:/Reports/32_forecast_tool/ava history/'

ava = pd.DataFrame({}, columns=['article_id', 'date', 'stock', 'reserved', 'deleted'])

date_check = pd.read_csv(file_dir + 'ava_weekly.csv')
date_column = date_check['date']

# Checking if the date column is empty
if date_column.empty:
    start = '2023-03-20'
else:
    last_date_str = str(date_column.iloc[-1])
    last_date = datetime.datetime.strptime(last_date_str, '%Y-%m-%d').date()
    start = (last_date + pd.DateOffset(days=7)).strftime('%Y-%m-%d')


# Set a date of first update
start_date = pd.to_datetime(start).date()

# Set date when we want to end the process
today = datetime.date.today()
offset = (today.weekday() - 0) % 7
first_day_of_currweek = today - datetime.timedelta(days=offset)

# Set period you want to process
end_date = pd.to_datetime((start_date+ pd.DateOffset(days=7)).date()).date()

while start_date <= end_date: #first_day_of_currweek: 

    if start_date > first_day_of_currweek:
        break

    # Run MySQL query to fetch data
    # Putty must be running while running the script
    with open(file_dir + 'db_credentials.txt', 'r') as f:
        db_credentials = f.read().splitlines()

    try:
        db_connections = connection.connect(
            host=db_credentials[0],
            user=db_credentials[1],
            password=db_credentials[2],
            database=db_credentials[3]
        )
        sql_file = open(file_dir + 'ava.sql')
        sql_query = sql_file.read().format(start_date)
        result = pd.read_sql(sql_query,db_connections)

        # Union result with ava
        ava = pd.concat([ava, result], axis=0)

        # close the connection
        db_connections.close()  
    except Exception as e:
        db_connections.close()
        print(str(e))

    # Add 1 week to previously updated
    start_date = (start_date+ pd.DateOffset(days=7)).date() 

# Putty must be running while running the script
with open(file_dir + 'db_credentials.txt', 'r') as f:
    db_credentials = f.read().splitlines()

try:
    db_connections = connection.connect(
        host=db_credentials[0],
        user=db_credentials[1],
        password=db_credentials[2],
        database=db_credentials[3]
    )
    sql_file2 = open(file_dir + 'sales.sql')
    sql_query2 = sql_file2.read()
    sales = pd.read_sql(sql_query2,db_connections)

    # Close the connection
    db_connections.close()  
except Exception as e:
    db_connections.close()
    print(str(e))

ava['key_column'] = pd.to_datetime(ava['date']).astype(np.int64) // 10**9
ava['key_column'] = ava['key_column'].astype(str) + '-' + ava['article_id'].astype(str)

sales['date2'] = pd.to_datetime(sales['date']).dt.to_period('W').dt.start_time
sales = sales.groupby(['date2', 'article_id']).size().reset_index(name='sale')
sales['key_column'] = pd.to_datetime(sales['date2']).astype(np.int64) // 10**9
sales['key_column'] = sales['key_column'].astype(str) + '-' + sales['article_id'].astype(str)
sales = sales.rename(columns={'date2': 'date'})
sales = sales.drop(['date', 'article_id'], axis=1)

ava_full = pd.merge(ava, sales, on='key_column', how='left')
ava_full['sale'] = ava_full['sale'].fillna(0)
ava_full = ava_full.drop(['key_column'], axis=1)

ava_full['sale'] = ava_full['sale'].astype(str)
ava_full['sale'] = ava_full['sale'].replace(r'\.0', '', regex=True)

# If it's all good, write the result into the file
with open(file_dir + 'ava_weekly.csv', 'a', newline='') as csvfile:
    writer = csv.writer(csvfile)
    if csvfile.tell() == 0:  # Check if the file is empty
        writer.writerow(result.columns)  # Write header row if file is empty

    ava_full.to_csv(csvfile, index=False, header=False)

ava_full.rename(columns=lambda x: x.capitalize(), inplace=True)
ava_full.rename(columns={'Sales': 'Sale'}, inplace=True)
ava_full.rename(columns={'Article_id': 'ID Article'}, inplace=True)

with open('G:/Reports/32_forecast_tool/source/ava.csv', 'a', newline='') as csvfile:
    writer = csv.writer(csvfile)
    if csvfile.tell() == 0:  # Check if the file is empty
        writer.writerow(result.columns)  # Write header row if file is empty

    ava_full.to_csv(csvfile, index=False, header=False)
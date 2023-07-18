import mysql.connector  as connection
import pandas as pd
import numpy as np
import datetime as dt
import os

# Name directory variables
scripts_dir = 'C:/Users/User/Desktop/Moje dokumenty/Raporty moje/063_Revenue recognition report/Scripts/'
cred_file = 'I:/db_credentials.txt'
files_dir = 'C:/Users/User/Desktop/Moje dokumenty/Raporty moje/063_Revenue recognition report/Files/'

# Add existing files
DIM_auftrag_temp = pd.read_csv(files_dir + 'starting_files/DIM_all_auction.csv' ) 
payments_2019_2022_Sylwia_temp_all = pd.read_csv(files_dir + 'starting_files/FACT_all_transactions.csv' ) 

# Run MySQL query to fetch auftrags. To refresh data, keep in mind to set correct dates in MySQL query
# Putty must be running while running the script
with open(cred_file, 'r') as f:
    db_credentials = f.read().splitlines()

try:
    db_connections = connection.connect(
        host=db_credentials[0],
        user=db_credentials[1],
        password=db_credentials[2],
        database=db_credentials[3]
    )
    # loading auftrags
    print("loading Auftrag_2022_refresh ...")
    dim_auftrag = open(scripts_dir + 'SQL/Dim_auftrag.sql')
    dim_auftrag_query = dim_auftrag.read()
    Auftrag_2022_refresh = pd.read_sql(dim_auftrag_query,db_connections)
    print('Auftrag_2022_refresh is ready')

    # close the connection
    db_connections.close()  
    print('All MySQL queries are loaded and db_connections is closed ')
except Exception as e:
    db_connections.close()
    print(str(e))


# Run MySQL query to fetch shipping. To refresh data, keep in mind to set correct dates in MySQL query
# Putty must be running while running the script
with open(cred_file, 'r') as f:
    db_credentials = f.read().splitlines()

try:
    db_connections = connection.connect(
        host=db_credentials[0],
        user=db_credentials[1],
        password=db_credentials[2],
        database=db_credentials[3]
    )
        # loading shippings
    print("loading shipping ...")
    shipping_issue_158946 = open(scripts_dir + 'SQL/shipping.sql')
    shipping_issue_158946_query = shipping_issue_158946.read()
    shipping = pd.read_sql(shipping_issue_158946_query,db_connections)
    print('shipping is ready')

    # close the connection
    db_connections.close()  
    print('All MySQL queries are loaded and db_connections is closed ')
except Exception as e:
    db_connections.close()
    print(str(e))


# Run MySQL query to fetch ticket & payment. To refresh data, keep in mind to set correct dates in MySQL query
# Putty must be running while running the script
with open(cred_file, 'r') as f:
    db_credentials = f.read().splitlines()

try:
    db_connections = connection.connect(
        host=db_credentials[0],
        user=db_credentials[1],
        password=db_credentials[2],
        database=db_credentials[3]
    )
        # loading tickets
    print("loading ticket_payment_2022_refresh ...")
    ticket_payment_union = open(scripts_dir + 'SQL/Ticket_payment_union_new.sql')
    ticket_payment_union_query = ticket_payment_union.read()
    ticket_payment_2022_refresh = pd.read_sql(ticket_payment_union_query,db_connections)
    print('ticket_payment_2022_refresh is ready')

    # close the connection
    db_connections.close()  
    print('All MySQL queries are loaded and db_connections is closed ')
except Exception as e:
    db_connections.close()
    print(str(e))

# Run MySQL query to fetch currency rates. To refresh data, keep in mind to set correct dates in MySQL query
# Putty must be running while running the script
with open(cred_file, 'r') as f:
    db_credentials = f.read().splitlines()

try:
    db_connections = connection.connect(
        host=db_credentials[0],
        user=db_credentials[1],
        password=db_credentials[2],
        database=db_credentials[3]
    )
        # loading tickets
    print("loading Curr_rate ...")
    Curr_rate_file = open(scripts_dir + 'SQL/Curr_rate.sql')
    Curr_rate_query = Curr_rate_file.read()
    Curr_rate = pd.read_sql(Curr_rate_query,db_connections)
    print('Curr_rate is ready')

    # close the connection
    db_connections.close()  
    print('All MySQL queries are loaded and db_connections is closed ')
except Exception as e:
    db_connections.close()
    print(str(e))


# Change date columns into datetime
Auftrag_2022_refresh['Auftrag_create_date'] = pd.to_datetime(Auftrag_2022_refresh['Auftrag_create_date'])
Auftrag_2022_refresh['Invoice_date'] = pd.to_datetime(Auftrag_2022_refresh['Invoice_date'])

# Create company column accordingly with the issue
Auftrag_2022_refresh['Company'] = np.select(
    [
        Auftrag_2022_refresh['Seller'] == 'Beliani PL',
        (Auftrag_2022_refresh['Seller'] == 'Beliani NL') & (Auftrag_2022_refresh['Auftrag_create_date'] > pd.to_datetime('2022-11-04')),
        (Auftrag_2022_refresh['Seller'] == 'Beliani NO') & (Auftrag_2022_refresh['Auftrag_create_date'] > pd.to_datetime('2022-08-16')),
        Auftrag_2022_refresh['Seller'] == 'Beliani (International) GmbH',
        Auftrag_2022_refresh['Seller'].isin(['Beliani UK', 'Design_UK']),
        Auftrag_2022_refresh['Seller'].isin(['Beliani', 'ayohama', 'Ricardo', 'CH: Express24', 'Allegro Broken PL'])
    ],
    [
        'Beliani PL',
        'Beliani EUR',
        'Beliani NOR',
        'Beliani INT',
        'Beliani UK',
        'Beliani CH'
    ],
    default='Beliani DE'
)

# Extract currency from the countries
Auftrag_2022_refresh['Currency'] = np.select(
    [
        Auftrag_2022_refresh['Sellers country'] == 'DK',
        Auftrag_2022_refresh['Sellers country'] == 'HU',
        Auftrag_2022_refresh['Sellers country'] == 'NO',
        Auftrag_2022_refresh['Sellers country'] == 'SE',
        Auftrag_2022_refresh['Sellers country'] == 'PL',
        Auftrag_2022_refresh['Sellers country'] == 'GB',
        Auftrag_2022_refresh['Sellers country'] == 'CH',
        Auftrag_2022_refresh['Sellers country'] == 'CZ',
        Auftrag_2022_refresh['Sellers country'] == 'PB'
    ],
    [
        'DKK',
        'HUF',
        'NOK',
        'SEK',
        'PLN',
        'GBP',
        'CHF',
        'CZK',
        'PLN'
    ],
    default='EUR'
)

# Create Auftrag_link column
Auftrag_2022_refresh['Auftrag_link'] = 'https://www.prologistics.info/auction.php?number=' + Auftrag_2022_refresh['Auction number'].astype(str) + '&txnid=' + Auftrag_2022_refresh['Txnid'].astype(str)

# Remove not needed column
Auftrag_2022_refresh = Auftrag_2022_refresh.dropna(subset=['open_amount'])

# Left join shipping info into auftrags 
Auftrag_2022_refresh = pd.merge(Auftrag_2022_refresh, shipping, left_on='MAU_Main auction', right_on='MAU_Main_Auction', how='left')

# Create join_date column - key needed to for joining currency rates
Auftrag_2022_refresh['join_date'] = pd.to_datetime(Auftrag_2022_refresh['Invoice_date']).dt.to_period('M').dt.to_timestamp()

# Change date columns into datetime
Curr_rate['rate_month'] = pd.to_datetime(Curr_rate['rate_month'])

# Left join currency info into auftrags 
Auftrag_2022_refresh = pd.merge(
    Auftrag_2022_refresh,
    Curr_rate,
    left_on=['Currency', 'join_date'],
    right_on=['Currency', 'rate_month'],
    how='left'
)

# Add missing value for CHF
Auftrag_2022_refresh['value'] = np.where(
    (Auftrag_2022_refresh['Currency'] == 'CHF') & Auftrag_2022_refresh['rate_month'].isna(),
    1,
    Auftrag_2022_refresh['value']
)

# Rename value column
Auftrag_2022_refresh = Auftrag_2022_refresh.rename(columns={'value': 'Exchange'})

# Clean data
Auftrag_2022_refresh['Order_not_sent'] = Auftrag_2022_refresh['Order_not_sent'].astype(str)
Auftrag_2022_refresh['Order_not_sent'] = Auftrag_2022_refresh['Order_not_sent'].astype(str).replace(r'\.0', '', regex=True)
Auftrag_2022_refresh['Txnid'] = Auftrag_2022_refresh['Txnid'].astype(str)
Auftrag_2022_refresh['Txnid'] = Auftrag_2022_refresh['Txnid'].astype(str).replace(r'\.0', '', regex=True)
Auftrag_2022_refresh['Auction number'] = Auftrag_2022_refresh['Auction number'].astype(str)
Auftrag_2022_refresh['Auction number'] = Auftrag_2022_refresh['Auction number'].astype(str).replace(r'\.0', '', regex=True)

# Drop not needed columns
Auftrag_2022_refresh = Auftrag_2022_refresh.drop(['rate_month', 'join_date','MAU_Main_Auction'], axis=1)

# Calculate columns in CHF
Auftrag_2022_refresh['open_amount_CHF'] = np.where(Auftrag_2022_refresh['Currency'] == 'CHF',
                                                  Auftrag_2022_refresh['open_amount'],
                                                  Auftrag_2022_refresh['open_amount'] * Auftrag_2022_refresh['Exchange'])
Auftrag_2022_refresh['open_amount_CHF_netto'] = np.where(Auftrag_2022_refresh['Currency'] == 'CHF',
                                                         Auftrag_2022_refresh['Open_amount_netto'],
                                                         Auftrag_2022_refresh['Open_amount_netto'] * Auftrag_2022_refresh['Exchange'])

# Pick and keep rows where auction is main auction and is deleted
DIM_auftrag_deleted_keep = Auftrag_2022_refresh[
    (Auftrag_2022_refresh['deleted'] == 1) &
    (Auftrag_2022_refresh['MAU_Main auction'].isin(ticket_payment_2022_refresh['Main auction']))
]

# Pick rows where which wasn't deleted
Auftrag_2022_refresh = Auftrag_2022_refresh[Auftrag_2022_refresh['deleted'] == 0]

# Concat 2 df above. In this process we exclude all deleted auftrags which aren't main auctions
Auftrag_2022_refresh = pd.concat([Auftrag_2022_refresh, DIM_auftrag_deleted_keep], ignore_index=True)

# Deleted sup auctions
DIM_auftrag_deleted_subs = Auftrag_2022_refresh[
    (Auftrag_2022_refresh['deleted'] == 1) &
    (Auftrag_2022_refresh['MAU_Main auction'] != Auftrag_2022_refresh['Main auction'])
]

# Exclude deleted subs just in case there are any left
Auftrag_2022_refresh = pd.merge(Auftrag_2022_refresh, DIM_auftrag_deleted_subs, how='left', indicator=True)
Auftrag_2022_refresh = Auftrag_2022_refresh[Auftrag_2022_refresh['_merge'] == 'left_only']
Auftrag_2022_refresh = Auftrag_2022_refresh.drop('_merge', axis=1)

# Pick deleted auftrags
DIM_auftrag_temp_to_move = Auftrag_2022_refresh[Auftrag_2022_refresh['deleted'] == 1]

# Now leave only those which weren't sent and open_amount is 0
Auftrag_2022_refresh.loc[Auftrag_2022_refresh['Main auction'].isin(DIM_auftrag_temp_to_move['Main auction']), 'Order_not_sent'] = 1
Auftrag_2022_refresh.loc[Auftrag_2022_refresh['Main auction'].isin(DIM_auftrag_temp_to_move['Main auction']), 'open_amount'] = 0

# Just in case drop duplicates 
Auftrag_2022_refresh = Auftrag_2022_refresh.drop_duplicates(subset='Main auction', keep='first')

# Filter out 2023. THere should be only the values needed, because this condition exists in SQL, but better double check.
Auftrag_2022_refresh = Auftrag_2022_refresh[pd.to_datetime(Auftrag_2022_refresh['Auftrag_create_date']).dt.year >= 2023 ]
DIM_auftrag_temp = DIM_auftrag_temp[pd.to_datetime(DIM_auftrag_temp['Auftrag_create_date']).dt.year < 2023 ]

# Clear DIM_auftrag_temp from matching values in Auftrag_2022_refresh
All_auftrag = pd.merge(DIM_auftrag_temp, Auftrag_2022_refresh[['Main auction']], on='Main auction', how='left', indicator=True)
All_auftrag = All_auftrag[All_auftrag['_merge'] == 'left_only']
All_auftrag = All_auftrag.drop('_merge', axis=1)

# Concat both df from above
All_auftrag = pd.concat([All_auftrag, Auftrag_2022_refresh], ignore_index=True)

print('DIM_all_auction is ready')

# Create company column accordingly with the issue
ticket_payment_2022_refresh['Currency'] = np.select(
    [
        ticket_payment_2022_refresh['Sellers country'] == 'DK',
        ticket_payment_2022_refresh['Sellers country'] == 'HU',
        ticket_payment_2022_refresh['Sellers country'] == 'NO',
        ticket_payment_2022_refresh['Sellers country'] == 'SE',
        ticket_payment_2022_refresh['Sellers country'] == 'PL',
        ticket_payment_2022_refresh['Sellers country'] == 'GB',
        ticket_payment_2022_refresh['Sellers country'] == 'CH',
        ticket_payment_2022_refresh['Sellers country'] == 'CZ',
        ticket_payment_2022_refresh['Sellers country'] == 'PB'
    ],
    [
        'DKK',
        'HUF',
        'NOK',
        'SEK',
        'PLN',
        'GBP',
        'CHF',
        'CZK',
        'PLN'
    ],
    default='EUR'
)

# Create join_date column - key needed to for joining currency rates 
ticket_payment_2022_refresh['join_date'] = pd.to_datetime(ticket_payment_2022_refresh['Transaction_Date']).dt.to_period('M').dt.to_timestamp()

# Left join currency info into tickets & payments 
ticket_payment_2022_refresh = pd.merge(
    ticket_payment_2022_refresh,
    Curr_rate,
    left_on=['Currency', 'join_date'],
    right_on=['Currency', 'rate_month'],
    how='left'
)

# Add missing value for CHF
ticket_payment_2022_refresh['value'] = np.where(
    (ticket_payment_2022_refresh['Currency'] == 'CHF') & ticket_payment_2022_refresh['rate_month'].isna(),
    1,
    ticket_payment_2022_refresh['value']
)

# Rename value column
ticket_payment_2022_refresh = ticket_payment_2022_refresh.rename(columns={'value': 'Exchange'})

# Drop not needed columns
ticket_payment_2022_refresh = ticket_payment_2022_refresh.drop(['rate_month', 'join_date'], axis=1)

# Calculate columns in CHF
ticket_payment_2022_refresh['Amount_netto(CHF)'] = np.where(ticket_payment_2022_refresh['Currency'] == 'CHF',
                                                  ticket_payment_2022_refresh['Amount_Netto(local)'],
                                                  ticket_payment_2022_refresh['Amount_Netto(local)'] * ticket_payment_2022_refresh['Exchange'])
ticket_payment_2022_refresh['Amount_netto(CHF)_web'] = np.where(ticket_payment_2022_refresh['Currency'] == 'CHF',
                                                  ticket_payment_2022_refresh['Amount_Netto(local)'],
                                                  ticket_payment_2022_refresh['Amount_Netto(local)'] * ticket_payment_2022_refresh['Exchange'])

# Clean data
ticket_payment_2022_refresh['Txnid'] = ticket_payment_2022_refresh['Txnid'].astype(str)
ticket_payment_2022_refresh['Txnid'] = ticket_payment_2022_refresh['Txnid'].astype(str).replace(r'\.0', '', regex=True)
ticket_payment_2022_refresh['Auction number'] = ticket_payment_2022_refresh['Auction number'].astype(str)
ticket_payment_2022_refresh['Auction number'] = ticket_payment_2022_refresh['Auction number'].astype(str).replace(r'\.0', '', regex=True)

# Create Ticket_link column
def generate_ticket_link(row):
    if row['Refund?[0-no,1-del_auf,2-ticket]'] == 2:
        return f"https://www.prologistics.info/rma.php?rma_id={row['Payment_id/Ticket_id']}&number={row['Auction number']}&txnid={row['Txnid']}"
    else:
        return ''
ticket_payment_2022_refresh['Ticket_link'] = ticket_payment_2022_refresh.apply(generate_ticket_link, axis=1)

# Left join shipping info into tickets & payments 
ticket_payment_2022_refresh = pd.merge(ticket_payment_2022_refresh, shipping, left_on='Main auction', right_on='MAU_Main_Auction', how='left')

# Drop not needed columns
ticket_payment_2022_refresh = ticket_payment_2022_refresh.drop(['MAU_Main_Auction', 'Order_not_sent'], axis=1)
ticket_payment_2022_refresh = ticket_payment_2022_refresh.drop(columns=['Auction number', 'Txnid', 'Seller', 'Sellers country', 'Customer ID', 'Currency'])

# Filter out 2023. THere should be only the values needed, because this condition exists in SQL, but better double check.
ticket_payment_2022_refresh = ticket_payment_2022_refresh[pd.to_datetime(ticket_payment_2022_refresh['Transaction_Date']).dt.year >= 2023 ]
payments_2019_2022_Sylwia_temp_all = payments_2019_2022_Sylwia_temp_all[pd.to_datetime(payments_2019_2022_Sylwia_temp_all['Transaction_Date']).dt.year < 2023 ]

# Concat both dataframes
All_ticket_payment = pd.concat([payments_2019_2022_Sylwia_temp_all, ticket_payment_2022_refresh], ignore_index=True)

# Change date columns into datetime
date_columns = ['Create_date', 'Transaction_Date', 'Shipped_date', 'Ship_create_date']
All_ticket_payment[date_columns] = All_ticket_payment[date_columns].apply(pd.to_datetime)
for column in date_columns:
    All_ticket_payment[column] = All_ticket_payment[column].dt.date
date_columns = ['Invoice_date', 'Auftrag_create_date', 'Shipped_date']
All_auftrag[date_columns] = All_auftrag[date_columns].apply(pd.to_datetime)
for column in date_columns:
    All_auftrag[column] = All_auftrag[column].dt.date

# Remove potential Power Bi error rows
All_auftrag = All_auftrag[All_auftrag['Order_not_sent'] != 'nan']

# Save created files
All_ticket_payment.to_csv(files_dir + 'FACT_all_transactions.csv', index=False, encoding='utf-8')
All_auftrag.to_csv(files_dir + 'DIM_all_auction.csv', index=False, encoding='utf-8')

print('Script is fully processed')

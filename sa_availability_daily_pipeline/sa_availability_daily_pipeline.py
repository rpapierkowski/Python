import mysql.connector  as connection
import pandas as pd
import csv
import datetime


files_dir = 'G:/Data Marts/sa_avaiblaility_daily/Files/'
scripts_dir = 'G:/Data Marts/sa_avaiblaility_daily/Scripts/'

# Putty must be running while running the script
with open(files_dir + 'db_credentials.txt', 'r') as f:
    db_credentials = f.read().splitlines()

try:
    db_connections = connection.connect(
        host=db_credentials[0],
        user=db_credentials[1],
        password=db_credentials[2],
        database=db_credentials[3]
    )
    sql_file = open(scripts_dir + 'stating_sa_list.sql')
    sql_query = sql_file.read()
    sa_list = pd.read_sql(sql_query,db_connections)

    # Close the connection
    db_connections.close()  

    salist_ids = ','.join(map(str, sa_list['id']))

except Exception as e:
    db_connections.close()
    print(str(e))

# Read the last_updated_date to fetch last updated date
last_updated_date_file = pd.read_csv(files_dir + 'last_updated_date.csv')

# Set it as date to sql be able to read it
last_updated_date_file['Date'] = pd.to_datetime(last_updated_date_file['Date']).dt.date

# Separate the value
last_updated_date = last_updated_date_file.iloc[0]['Date']

# Set end date
end_date = (last_updated_date+ pd.DateOffset(days=7)).date() 

# Set period you want to process
tday = datetime.date.today() 

while last_updated_date < end_date: 

    if last_updated_date >= tday:
        break

    # Add 1 day to previously updated
    last_updated_date = (last_updated_date+ pd.DateOffset(days=1)).date() 

    # Run MySQL query to fetch data
    # Putty must be running while running the script
    with open(files_dir + 'db_credentials.txt', 'r') as f:
        db_credentials = f.read().splitlines()

    try:
        db_connections = connection.connect(
            host=db_credentials[0],
            user=db_credentials[1],
            password=db_credentials[2],
            database=db_credentials[3]
        )

        # Find total_log_id matching date
        total_log_date = (last_updated_date+ pd.DateOffset(days=1)).date() 
        total_log = """
            SELECT total_log_id
            FROM total_log_id 
            WHERE Updated = '{0}'
        """.format(last_updated_date)

        total_log_id = pd.read_sql(total_log,db_connections)
        
        # data fetching query
        sql_main_query = """
            SELECT c.date
            ,	(
                SELECT updated
                FROM total_log tl 
                WHERE tl.table_name_id = 495 
                AND tl.tableid = sa.id  
                AND tl.field_name_id = 5926
                AND tl.ID < {2}
                ORDER BY tl.ID DESC
                LIMIT 1
                ) minava_updated
            ,	sa.id AS sa_id  
            ,	(
                SELECT new_value
                FROM total_log tl 
                WHERE tl.table_name_id = 495 
                AND tl.tableid = sa.id 
                AND tl.field_name_id = 36
                AND tl.ID < {2}
                ORDER BY tl.ID DESC
                LIMIT 1
                ) sa_inactive
            ,	(
                SELECT new_value
                FROM total_log tl 
                WHERE tl.table_name_id = 495 
                AND tl.tableid = sa.id 
                AND tl.field_name_id = 5926
                AND tl.ID < {2}               
                ORDER BY tl.ID DESC
                LIMIT 1
                ) sa_minava
            ,	(
                SELECT new_value
                FROM total_log tl 
                WHERE tl.table_name_id = 495 
                AND tl.tableid = sa.id 
                AND tl.field_name_id = 747
                AND tl.ID < {2}               
                ORDER BY tl.ID DESC
                LIMIT 1
                ) sa_old
            ,	IFNULL(sa.master_sa, sa.id) master_sa
                
            FROM calendar c

            CROSS JOIN (SELECT sa.id, master_sa, sa.old
            FROM saved_auctions sa
            WHERE sa.id IN ({1})
            ) sa

            WHERE c.date = '{0}'
                AND IF(sa.old = 1
                ,	{2}  < (
                    SELECT tl.ID 
                    FROM total_log tl 
                    WHERE tl.table_name_id = 495 
                    AND tl.tableid = sa.id 
                    AND tl.field_name_id = 747
                    AND New_value = 1
                    ORDER BY tl.ID DESC
                    LIMIT 1)
                ,	sa.old = 0
                    ) 
            GROUP BY c.date, sa.id

            ORDER BY c.date
            ;
            """.format(last_updated_date, salist_ids, total_log_id.iloc[0, 0])
        
        # Fill variable with the fetched batch 
        result = pd.read_sql(sql_main_query,db_connections)

        # Fetch the number of rows which suppose to be fetched
        sql_check_query= """
        SELECT COUNT(*)
        FROM(
              SELECT c.date
            ,	(
                SELECT updated
                FROM total_log tl 
                WHERE tl.table_name_id = 495 
                AND tl.tableid = sa.id  
                AND tl.field_name_id = 5926
                AND tl.ID < {2}
                ORDER BY tl.ID DESC
                LIMIT 1
                ) minava_updated
            ,	sa.id AS sa_id  
            ,	(
                SELECT new_value
                FROM total_log tl 
                WHERE tl.table_name_id = 495 
                AND tl.tableid = sa.id 
                AND tl.field_name_id = 36
                AND tl.ID < {2}
                ORDER BY tl.ID DESC
                LIMIT 1
                ) sa_inactive
            ,	(
                SELECT new_value
                FROM total_log tl 
                WHERE tl.table_name_id = 495 
                AND tl.tableid = sa.id 
                AND tl.field_name_id = 5926
                AND tl.ID < {2}               
                ORDER BY tl.ID DESC
                LIMIT 1
                ) sa_minava
            ,	(
                SELECT new_value
                FROM total_log tl 
                WHERE tl.table_name_id = 495 
                AND tl.tableid = sa.id 
                AND tl.field_name_id = 747
                AND tl.ID < {2}               
                ORDER BY tl.ID DESC
                LIMIT 1
                ) sa_old
            ,	IFNULL(sa.master_sa, sa.id) master_sa
                
            FROM calendar c

            CROSS JOIN (SELECT sa.id, master_sa, sa.old
            FROM saved_auctions sa
            WHERE sa.id IN ({1})
            ) sa

            WHERE c.date = '{0}'
                AND IF(sa.old = 1
                ,	{2}  < (
                    SELECT tl.ID 
                    FROM total_log tl 
                    WHERE tl.table_name_id = 495 
                    AND tl.tableid = sa.id 
                    AND tl.field_name_id = 747
                    AND New_value = 1
                    ORDER BY tl.ID DESC
                    LIMIT 1)
                ,	sa.old = 0
                    ) 
            GROUP BY c.date, sa.id

            ORDER BY c.date
            ) che
            ;
            """.format(last_updated_date, salist_ids, total_log_id.iloc[0, 0])
        
        # Fill variable with the number of rows which suppose to be fetched
        check = pd.read_sql(sql_check_query,db_connections)

        # Check both values, it doesn't match break the script and do not update the file
        if len(result) != check.iloc[0, 0]:
            db_connections.close()  
            print('Script has been terminated because values for ' + str(last_updated_date) + ' do not match')
            break
        
        result_to_save = result.copy()
        result_to_save.dropna(subset=['sa_minava'], inplace=True)    


        # If it's all good, write the result into the file
        with open(files_dir + 'final_result.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if csvfile.tell() == 0:  # Check if the file is empty
                writer.writerow(result.columns)  # Write header row if file is empty
 
            result_to_save.to_csv(csvfile, index=False, header=False)

        # close the connection
        db_connections.close()  
    except Exception as e:
        db_connections.close()
        print(str(e))

    # Check again to break the loop
    if len(result) != check.iloc[0, 0]:
        break
    print(str(last_updated_date) + ' is done')

    # Update previously updated
    last_updated_date_file.loc[0, 'Date'] = last_updated_date
    # Save new date into the file
    last_updated_date_file.to_csv(files_dir + 'last_updated_date.csv', index=False, encoding='utf-8')

print('Script is fully processed')

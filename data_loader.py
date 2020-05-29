import csv
# from glob import iglob
import sys
import glob
import errno
import psycopg2
import pandas as pd
import os.path
# import config

def check_columns(file):
    path_out = 'Outbox_clean/'
    files = glob.glob(file)

    for name in files:
        delimiter = ','
        write_file_name = name.split('/')[-1]
        write_file_name = write_file_name.split('.')[0]+ '_clean.csv'
        print('File to write: ', write_file_name)

        write_file = open(write_file_name, 'w', encoding='utf-8')
        writer = csv.writer(write_file, delimiter='|')

        read_file = open(name, 'rt', encoding='utf-8')
        reader = csv.reader(read_file, delimiter=delimiter)

        headers = next(reader)
        # print('headers: ',headers)
        headers = [ val.replace('(','').replace(')','').replace('%','').replace(' ','').replace('*','asterisk').replace('-','').replace('+','') for val in headers]
        # writer.writerow(headers)

        count = 0
        cur_row = ''
        for line in reader:
            cur_row = line
            cur_row = [val.replace("'","").replace('"', '').replace('|', '').replace(';','').replace('-','_') for val in cur_row]
            writer.writerow(cur_row)
            count += 1



def csv_db_data_copy_old(file_name):
    # Construct connection string
    # Update connection string information obtained from the portal
    host = "addediqdemoserver-gen.postgres.database.azure.com"
    user = "myadmin@addediqdemoserver-gen"
    dbname = "data"
    password = "passcode123@"
    sslmode = "require"

    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    path = file_name
    files = glob.glob(path)

    try:
        for name in files:
            delimiter='|'
            data_file = open(name, 'rt', encoding='utf-8')
            table_name = name.split('.')[0]
            table_name = table_name.replace('_clean','')
            table_name = table_name.replace(' ','').replace('(','_').replace(')','').replace('-','')
            if (table_name == 'adobe_analytics_no_nulls'):
                table_name = 'Adobe_Analytics_Content_ETL'
            print('table name: {0}'.format(table_name))
            cursor.copy_from(data_file, table_name, sep=delimiter)
            conn.commit()
        cursor.close()
        conn.close()

    except IOError as exc:
        if exc.errno != errno.EISDIR: # Do not fail if a directory is found, just ignore it.
            raise # Propagate other kinds of IOError.

def csv_db_data_copy_old(file_name, table_name,delimiter='|'):
    # Construct connection string
    # Update connection string information obtained from the portal
    host = "addediqdemoserver-gen.postgres.database.azure.com"
    user = "myadmin@addediqdemoserver-gen"
    dbname = "data"
    password = "passcode123@"
    sslmode = "require"

    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    print(file_name)
    try:
        data = open(file_name,'rt',encoding='utf-8')
        cursor.copy_from(data, table_name, sep=delimiter)
        conn.commit()
        cursor.close()
        conn.close()

    except IOError as exc:
        if exc.errno != errno.EISDIR: # Do not fail if a directory is found, just ignore it.
            raise # Propagate other kinds of IOError.

def print_create_table_query(table_name,headers_list):

    clean_header_list = [ val.replace('(','').replace(')','').replace('%','').replace(' ','').replace('*','asterisk').replace('-','').replace('+','') for val in headers_list]
    
    column_string = ",".join([ "{0} TEXT".format(val) for val in clean_header_list])
    create_table_query = "create table {0} ( {1} );".format(table_name,column_string)
    print(create_table_query)



def create_reduced_clicks_to_sale():
    file_name_in = 'clickstosalescombinedupdate.csv'
    file_name_out = 'clickstosalescombinedupdate_reduced.csv'

    chunksize = 10 ** 6
    count=0
    for chunk in pd.read_csv(file_name_in, chunksize=chunksize, encoding = "ISO-8859-1"):
        print("Current iteration: {0}".format(count))
        df = chunk[['NumberViews','EDC','CustomerSeq','Date','BrandDescription','ItemTypeDescription','ItemClassDescription']]
       
        file_exist = os.path.isfile(file_name_out)
        if file_exist:
            df.to_csv(file_name_out, mode='a', header=False, index=False)
        else:
            df.to_csv(file_name_out,header=False,index=False)
        count += 1


def create_reduced_adobe_analytics(filename_in, filename_out,table_name):
    chunksize = 10 ** 6
    count=0
    for chunk in pd.read_csv(filename_in, chunksize=chunksize, encoding = "ISO-8859-1"):
        print("Current iteration: {0}".format(count))
        df = chunk[[
                    'Month','Last Touch Channel','File Downloads (e10) (event10)',
                    'Form Complete (e18) (event18)','Video 50% Milestone (e15) (event15)',
                    'Page Type', 'Package Code1', 'Project Job ID1', 'Link Clicks',
                    'Brand (Derived)','File Downloads','Visits','Page Views'
                ]]

        file_exist = os.path.isfile(filename_out)
        if file_exist:
            df.to_csv(filename_out, mode='a', header=False, index=False)
        else:
            print_create_table_query(table_name,list(df.columns) )
            df.to_csv(filename_out,header=False,index=False)

        count += 1

def run_adobe_analytics_process():
    filename_in = 'Adobe_Analytics_Content_ETL.csv'
    filename_out = "adobe_analytics_content_etl_final_reduce.csv"
    table_name = 'adobe_analytics_content_etl_final_reduce'
    create_reduced_adobe_analytics(filename_in,filename_out,table_name)
    csv_db_data_copy(filename_out,table_name,delimiter=',')

def run_profilename_map():
    #profilename_map
    filename_in = 'Marketing_Activities_Digital_EMM_ProfileName_Map.csv'
    filename_in_clean = 'Marketing_Activities_Digital_EMM_ProfileName_Map_clean.csv'
    table_name = 'profilename_map'
    check_columns(filename_in)
    csv_db_data_copy(filename_in_clean,table_name,delimiter='|')

def csv_db_data_copy(file_name, table_name,db_config,delimiter='|'):
    # Construct connection string
    # Update connection string information obtained from the portal
    host = db_config['host']
    user = db_config['user']
    dbname = db_config['database_name']
    password = db_config['password']
    sslmode = "require"

    conn_string = "host={0} user={1} dbname={2} password={3}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    print(file_name)
    try:
        data = open(file_name,'rt',encoding='utf-8')
        cursor.copy_from(data, table_name, sep=delimiter)
        conn.commit()
        cursor.close()
        conn.close()

    except IOError as exc:
        if exc.errno != errno.EISDIR: # Do not fail if a directory is found, just ignore it.
            raise # Propagate other kinds of IOError.

def load_us_accidents():
    filename_in = 'US_Accidents_Dec19.csv'
    table_name = 'us_accidents'
    read_file = open(filename_in, 'rt', encoding='utf-8')
    reader = csv.reader(read_file)
    headers = next(reader)
    headers = [ val.replace('(','').replace(')','').replace('%','').replace(' ','').replace('*','asterisk').replace('-','').replace('+','') for val in headers]
    print_create_table_query(table_name,headers)
    db_config = {
        "host": 'localhost',
        "user": 'postgres',
        "database_name": 'postgres',
        "password":'postgres'
    }
    csv_db_data_copy(filename_in,table_name,db_config,delimiter=',')

if __name__ == "__main__":
    #file_name = ['Adobe_Analytics_Content_ETL.csv','AD_Brand_Channel_Monthly_PGM.csv','Stage_Weekly_Inbound_Master (MDRDB).csv']
    # file_name = ['Stage_Weekly_Inbound_Master (MDRDB).csv']
    # file_name = ['Stage_Weekly_Inbound_Master (MDRDB)_clean.csv']
    #file_name = ['clickstosalescombinedupdate_reduced_clean.csv']
    #print_create_table_query(file_name[0])
    #create_reduced_clicks_to_sale()
    # file_name = ['adobe_analytics_no_nulls.csv']
    #check_columns(file_name[0])
    #for i in file_name:
    #    csv_db_data_copy(i)
    
    #run adobe analytics process
    load_us_accidents()

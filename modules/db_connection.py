import sqlalchemy as sa
import pandas as pd
from sqlalchemy import text
from datetime import datetime as dt
import logging

def setup_dbcon_obj(table_name):

    # Setup database connection
    
    driver = 'ODBC+Driver+17+for+SQL+Server'
    server =  '999.99.99.99:9999'
    database = 'XX_XXX'
    username = 'XXXX999'
    password = 'samplepwd'

    sql_engine = sa.create_engine(
        'mssql+pyodbc://%s:%s@%s/%s?driver=%s' % (username, password, server, database, driver), 
        fast_executemany=True
    )

    # Setup simple dict object
    dbcon_setup_obj = {
        'sql_engine':sql_engine
    }

    return dbcon_setup_obj

def write_data_database(in_df, table_name, unique_col_list, data_handling='insert'):
    # Write data from dataframe into SQL database
    # Setup database connection object
    dbcon_setup_obj = setup_dbcon_obj(table_name)

    #delete existing data from the table
    remove_existing_data(table_name, dbcon_setup_obj)

    if data_handling=='insert':
        # Load data into database
        load_data_db(data_df=in_df, table_name=table_name, dbcon_setup_obj=dbcon_setup_obj)


    #elif data_handling=='remove':
        # Remove all data from database table
        #remove_data_db(table_name=table_name, data_source_name=data_source_name, queue_name=queue_name, dbcon_setup_obj=dbcon_setup_obj)

        # Load data into database
        #load_data_db(data_df=in_df, table_name=table_name, dbcon_setup_obj=dbcon_setup_obj)
    else:
        raise 'Invalid data_handling selection.'

    return 0

def load_data_db(data_df, table_name, dbcon_setup_obj):
    print('Loading data into database - %s......' % table_name)
    logging.info('Loading data into database - %s......' % table_name)
    
    # Load data into SQL database
    with dbcon_setup_obj['sql_engine'].connect() as sql_con:
        # Process column names to fit schema definition
        #data_df.columns = data_df.columns.str.upper().str.replace(' - ', '_').str.replace(' ', '_')

        # Add data load time column
        data_df['LOADTIME'] = dt.now()

        # Load data into database
        data_df.to_sql(table_name, con=sql_con, index=False, if_exists='append', chunksize=None)
    return 0

def remove_existing_data(table_name, dbcon_setup_obj):
    print('Removing data from database - %s......' % table_name)
    logging.info('Removing data from database - %s......' % table_name)
    # Remove duplicates from SQL database
    # Must use a separate connection object to remove duplicates. pandas' to_sql does not work with autocommit=True
    #unique_col_list = [x.upper().replace(' - ', '_').replace(' ', '_') for x in unique_col_list]
    #if len(unique_col_list) > 1:
    #    concat_cols = ', '.join(unique_col_list)
    #else:
    #    concat_cols = unique_col_list[0]

    with dbcon_setup_obj['sql_engine'].connect().execution_options(autocommit=True) as sql_con:
        # Remove duplicates on database
        # This will always keep latest copy and remove older duplicates
        #remove_duplicate_statement = text('''
        #    WITH CTE AS( SELECT *, RN = ROW_NUMBER()OVER(PARTITION BY %s ORDER BY %s, LOADTIME DESC) FROM %s ) DELETE FROM CTE WHERE RN > 1;
        #''' % (concat_cols, unique_col_list[0], table_name))

        # Delete all data from the database table
        remove_data_statement = text('DELETE FROM %s' % table_name)
        res = sql_con.execute(remove_data_statement)
    return 0

def read_contractor_data():
    print('Reading contractor data...')
    logging.info('Reading contractor data...')
    bio_table_name = 'StaffBioData'
    detail_table_name = 'StaffDetails'

    read_contractor_statement = text('SELECT SgIdenEmpFname, SgIdenEmpLname, SgNricFin, Phone, GlobalId, Department, CompanyEmail \
        FROM %s detail LEFT OUTER JOIN %s bio ON detail.StaffID = bio.StaffID ' % (bio_table_name,detail_table_name ))
    
    # Setup database connection
    # TODO - do not store login details in plain text. Store in environment
    driver = 'ODBC+Driver+17+for+SQL+Server'
    server =  'XXX.XX.XX.XXX:9999'
    database = 'XXXX'
    username = 'XXXX1234'
    password = 'samplepwd'

    sql_engine = sa.create_engine(
        'mssql+pyodbc://%s:%s@%s/%s?driver=%s' % (username, password, server, database, driver), 
        fast_executemany=True
    )

    # Setup simple dict object
    dbcon_setup_obj = {
        'sql_engine':sql_engine
    }

    contractordf = pd.DataFrame()

    with dbcon_setup_obj['sql_engine'].connect() as sql_con:
        contractordf = pd.read_sql(read_contractor_statement, con=sql_con)

    return contractordf




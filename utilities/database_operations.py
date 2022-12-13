"""Module to perform operations SQLite3 database and check if data in database is empty"""


import os
import sqlite3
import warnings

import numpy as np
import pandas as pd

from utilities.module_execution import status_info


def write_database(project_constants_lst, data_names, *args):
    """Function to write table data to SQL database.
    Args are comma separated DataFrames to save."""

    project_steps_df, max_title, _, report_requisites_sr, *_ = project_constants_lst

    for data_name, data_exported in zip(data_names, args):
        # db_type = report_steps_dct[data_name][2]
        db_type = project_steps_df.loc[data_name, 'report_type']
        db_name = report_requisites_sr['customer_name'] + '_' + db_type + '_database.db'
        db_path = os.path.join(report_requisites_sr['database_folder'], db_name)

        info = f'Writing {data_name} to {db_type} database'
        print(info, end=" ")
        # saving data for DataFrame
        if isinstance(data_exported, (pd.DataFrame, pd.Series)):
            data_exported_flat, empty_data = dataframe_flatten(data_exported)
            substitute_names(data_exported_flat, 'write')
            # save single level Index DataFrame to database
            write_sql(db_path, data_name, data_exported_flat, max_title, info)
            if not empty_data:
                status_info('ok', max_title, len(info))
            else:
                status_info('empty', max_title, len(info))
        else:
            status_info('skip', max_title, len(info))


def dataframe_flatten(df):
    """Function to remove MultiIndexing in DataFrame and fill first row
    with 'NO DATA FOUND' if DataFrame is empty"""

    empty_data = False
    # check if DataFrame have MultiIndex
    # reset index if True due to MultiIndex is not saved
    if isinstance(df.index, pd.MultiIndex):
        df_flat = df.reset_index()
    # keep indexing if False
    else:
        df_flat = df.copy()
    # when DataFrame is empty fill first row values
    # with information string
    if df_flat.empty:
        empty_data = True
        if isinstance(df_flat, pd.DataFrame):
            if len(df_flat.columns) == 0:
                df_flat['EMPTY'] = np.nan
            df_flat.loc[0] = 'NO DATA FOUND'
        elif isinstance(df_flat, pd.Series):
            df_flat['EMPTY'] = 'NO DATA FOUND'

    return df_flat, empty_data


def substitute_names(df, operation):
    """Function to avoid column names duplication in sql.
    Capital and lower case letters don't differ iin sql.
    Function adds tag to duplicated column name when writes to the database
    and removes tag when read from the database."""

    if isinstance(df, pd.DataFrame):
        
        masking_tag = '_sql'
        duplicated_names = ['SwitchName', 'Fabric_Name', 'SwitchMode', 'Memory_Usage', 'Flash_Usage', 'Speed']
              
        if operation == 'write':
            # replace_dct = {orig_name: mask_name for orig_name, mask_name in substitution_names}
            replace_dct = {orig_name: orig_name + masking_tag for orig_name in duplicated_names}

            df.rename(columns=replace_dct, inplace=True)
        elif operation == 'read':
            # replace_dct = {mask_name: orig_name for orig_name, mask_name in substitution_names}
            replace_dct = {orig_name + masking_tag: orig_name for orig_name in duplicated_names}
            df.rename(columns=replace_dct, inplace=True)


def write_sql(db_path, data_name, df, max_title, info):
    """Function to write DataFrame to SQL DB"""

    with warnings.catch_warnings():
        warnings.filterwarnings(action="ignore", 
                                message="The spaces in these column names will not be changed. In pandas versions < 0.14, spaces were converted to underscores.")
        keep_index = True if isinstance(df, pd.Series) else False
        conn = None
        status = None
        try:
            conn = sqlite3.connect(db_path)
            df.to_sql(name=data_name, con=conn, index=keep_index, if_exists='replace')
        except (pd.io.sql.DatabaseError, sqlite3.OperationalError) as e:
            status = status_info('fail', max_title, len(info))
            if 'database is locked' in e.args[0]:
                print(f"\nCan't write {data_name} to {os.path.basename(db_path)}. DB is locked. Close it to proceed.\n")
            else:
                print('\n', e)
        finally:
            if conn is not None:
                conn.close()
            if status=='FAIL':
                exit()
            

def read_database(project_constants_lst, *args):
    """Function to read data from SQL.
    Args are comma separated DataFrames names.
    Returns list of loaded DataFrames or None if no data found.
    """


    project_steps_df, max_title, _, report_requisites_sr, *_ = project_constants_lst

    # if len(report_constant_lst) == 3:
    #     customer_name, _, db_dir, max_title, _ = report_constant_lst
    # else:
    #     customer_name, _, db_dir, max_title, *_ = report_constant_lst

    # list to store loaded data
    data_imported = []
    # conn = sqlite3.connect(db_path)


    for data_name in args:

        # db_type = report_steps_dct[data_name][2]

        db_type = project_steps_df.loc[data_name, 'report_type']
        db_name = report_requisites_sr['customer_name'] + '_' + db_type + '_database.db'
        
        db_path = os.path.join(report_requisites_sr['database_folder'], db_name)
        conn = sqlite3.connect(db_path)

        info = f'Reading {data_name} from {db_type} database'
        print(info, end=" ")
        data_name_in_db = conn.execute(
            f"""SELECT name FROM sqlite_master WHERE type='table' 
            AND name='{data_name}'; """).fetchall()
        if data_name_in_db:
            df = pd.read_sql(f"select * from {data_name}", con=conn)
            substitute_names(df, 'read')
            # revert single column DataFrame to Series
            if 'index' in df.columns:
                df.set_index('index', inplace=True)
                # squeezing objects with more than one value in every axis does nothing
                # single column squeezed down, resulting in a Series
                df = df.squeeze('columns')
            data_imported.append(df)
            status_info('ok', max_title, len(info))
        else:
            data_imported.append(None)
            status_info('no data', max_title, len(info))
        conn.close()
    return data_imported


def verify_read_data(max_title, data_names, *args,  show_status=True):
    """
    Function to verify if loaded DataFrame or Series contains 'NO DATA FOUND' information string.
    If yes then data converted to empty DataFrame otherwise remains unchanged.
    Function implemented to avoid multiple collection and analysis of parameters not applicable
    for the current SAN (fcr, ag, porttrunkarea) 
    """

    # *_, max_title = report_constant_lst
    
    # list to store verified data
    verified_data_lst = []
    for data_name, data_verified in zip(data_names, args):
        if show_status:
            info = f'Verifying {data_name}'
            print(info, end =" ")

        if not isinstance(data_verified, (pd.DataFrame, pd.Series)):
            if show_status:
                status_info('fail', max_title, len(info))
            print('\nERROR. Wrong datatype for verification')
            exit()
        
        first_row = data_verified.iloc[0] if isinstance(data_verified, pd.DataFrame) else data_verified
        
        if (len(data_verified.index) == 1 and # have single row
            first_row.nunique() == 1 and # have single unique value
            'NO DATA FOUND' in data_verified.values): # and this value is 'NO DATA FOUND'
            if isinstance(data_verified, pd.DataFrame):
                columns = data_verified.columns
                data_verified = pd.DataFrame(columns=columns) # for DataFrame use empty DataFrame with column names only
                # data_verified = data_verified.iloc[0:0] # for DataFrame use empty DataFrame with column names only
            else:
                name = data_verified.name
                data_verified = pd.Series(name=name, dtype='object') # for Series use empty Series
            if show_status:
                status_info('empty', max_title, len(info))
        else:
            if show_status:
                status_info('ok', max_title, len(info))

        verified_data_lst.append(data_verified)
    return verified_data_lst


def is_dataframe_empty(data_verified):

    first_row = data_verified.iloc[0]
    return (len(data_verified.index) == 1 and # have single row
        first_row.nunique() == 1 and # have single unique value
        'NO DATA FOUND' in data_verified.values) # and this value is 'NO DATA FOUND'


def add_log_entry(file_name, *args):
    """Function add lines (args) to the file_name"""
    
    # Open the file in append & read mode ('a+')
    with open(file_name, "a+") as file_object:
        appendEOL = False
        # Move read cursor to the start of file.
        file_object.seek(0)
        # Check if file is not empty
        data = file_object.read(100)
        if len(data) > 0:
            appendEOL = True
        # Iterate over each string in the list
        for log_entry in args:
            # If file is not empty then append '\n' before first line for
            # other lines always append '\n' before appending line
            if appendEOL == True:
                file_object.write("\n")
            else:
                appendEOL = True
            # Append element at the end of file
            file_object.write(log_entry)
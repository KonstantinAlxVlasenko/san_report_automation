"""Module to perform operations SQLite3 database"""


import os
import sqlite3

import pandas as pd
import numpy as np

from common_operations_miscellaneous import status_info


def write_db(report_constant_lst, report_steps_dct, data_names, *args):
    """
    Function to write table data to SQL database.
    Args are comma separated DataFrames to save.
    """

    if len(report_constant_lst) == 3:
        customer_name, _, db_dir, max_title, _ = report_constant_lst
    else:
        customer_name, _, db_dir, max_title, *_ = report_constant_lst


    for data_name, data_exported in zip(data_names, args):

        db_type = report_steps_dct[data_name][2]
        db_name = customer_name + '_' + db_type + '_database.db'
        db_path = os.path.join(db_dir, db_name)

        info = f'Writing {data_name} to {db_type} database'
        print(info, end=" ")
        # saving data for DataFrame
        if isinstance(data_exported, (pd.DataFrame, pd.Series)):
            data_exported_flat, empty_data = dataframe_flatten(data_exported)
            substitute_names(data_exported_flat, 'write')
            # save single level Index DataFrame to database
            write_sql(db_path, data_name, data_exported_flat)
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
        # substitution_names = [
        #     ('SwitchName', 'SwitchName_sql'), ('Fabric_Name', 'Fabric_Name_sql'),
        #     ('SwitchMode', 'SwitchMode_sql'), 'Memory_Usage'	'Flash_Usage']

        duplicated_names = ['SwitchName', 'Fabric_Name', 'SwitchMode', 'Memory_Usage', 'Flash_Usage', 'Speed']

                                
        if operation == 'write':
            # replace_dct = {orig_name: mask_name for orig_name, mask_name in substitution_names}
            replace_dct = {orig_name: orig_name + masking_tag for orig_name in duplicated_names}

            df.rename(columns=replace_dct, inplace=True)
        elif operation == 'read':
            # replace_dct = {mask_name: orig_name for orig_name, mask_name in substitution_names}
            replace_dct = {orig_name + masking_tag: orig_name for orig_name in duplicated_names}
            df.rename(columns=replace_dct, inplace=True)


def write_sql(db_path, data_name, df):
    """Function to write DataFrame to SQL DB"""


    keep_index = True if isinstance(df, pd.Series) else False
    conn = sqlite3.connect(db_path)
    df.to_sql(name=data_name, con=conn, index=keep_index, if_exists='replace')
    conn.close()


def read_db(report_constant_lst, report_steps_dct, *args):
    """Function to read data from SQL.
    Args are comma separated DataFrames names.
    Returns list of loaded DataFrames or None if no data found.
    """

    if len(report_constant_lst) == 3:
        customer_name, _, db_dir, max_title, _ = report_constant_lst
    else:
        customer_name, _, db_dir, max_title, *_ = report_constant_lst

    # db_name = customer_name + '_database.db'
    # db_path = os.path.join(db_dir, db_name)

    # list to store loaded data
    data_imported = []
    # conn = sqlite3.connect(db_path)

    for data_name in args:

        db_type = report_steps_dct[data_name][2]
        db_name = customer_name + '_' + db_type + '_database.db'
        db_path = os.path.join(db_dir, db_name)
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
                df = df.squeeze()
            data_imported.append(df)
            status_info('ok', max_title, len(info))
        else:
            data_imported.append(None)
            status_info('no data', max_title, len(info))
        conn.close()

    return data_imported
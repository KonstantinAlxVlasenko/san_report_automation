# -*- coding: utf-8 -*-
"""
Created on Tue Dec 21 13:13:20 2021

@author: vlasenko
"""



import os
import re
import sqlite3
import numpy as np
import pandas as pd

# import openpyxl
# from datetime import date
# import warnings
# import subprocess
# import sys
# from difflib import SequenceMatcher
# from collections import defaultdict




db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_south\database_MTS_south"
db_file = r"MTS_south_analysis_database.db"
data_names = ['NPIV_ports', 'NPIV_statistics']

portshow_npiv_df, npiv_statistics_df = read_database(db_path, db_file, *data_names)

npiv_statistics_cp_df = npiv_statistics_df.copy()

# npiv_statistics_cp_df = remove_duplicates_from_column(npiv_statistics_cp_df, column='NodeName', 
#                                                                 duplicates_subset=['Fabric_name', 'Fabric_label', 'switchWwn', 'NodeName'],
#                                                                 duplicates_free_column_name='NodeName_duplicates_free')

mask_multiple_connection = npiv_statistics_cp_df.groupby(by=['Fabric_name', 'Fabric_label', 'NodeName'])['NodeName'].transform('count') > 1
mask_nodename_notna = npiv_statistics_cp_df['NodeName'].notna()
npiv_statistics_cp_df.loc[mask_multiple_connection & mask_nodename_notna, 'NPIV_multiple_sw_connection_note'] = 'npiv_multiple_switch_connection'

    
def read_database(db_path, db_file, *args):
    """Function to read data from SQL.
    Args are comma separated DataFrames names.
    Returns list of loaded DataFrames or None if no data found.
    """

    db_filepath = os.path.join(db_path, db_file)

    # list to store loaded data
    data_imported = []
    conn = sqlite3.connect(db_filepath)

    for data_name in args:


        info = f'Reading {data_name} from database................'
        print(info, end="")
        data_name_in_db = conn.execute(
            f"""SELECT name FROM sqlite_master WHERE type='table' 
            AND name='{data_name}'; """).fetchall()
        if data_name_in_db:
            df = pd.read_sql(f"select * from {data_name}", con=conn)
            substitute_names(df)
            # revert single column DataFrame to Series
            if 'index' in df.columns:
                df.set_index('index', inplace=True)
                df = df.squeeze()
            data_imported.append(df)
            print('ok')
        else:
            data_imported.append(None)
            print('no data')
    conn.close()
    return data_imported


def substitute_names(df):
    """Function to avoid column names duplication in sql.
    Capital and lower case letters don't differ iin sql.
    Function adds tag to duplicated column name when writes to the database
    and removes tag when read from the database."""

    masking_tag = '_sql'
    duplicated_names = ['SwitchName', 'Fabric_Name', 'SwitchMode', 'Memory_Usage', 'Flash_Usage', 'Speed']
    replace_dct = {orig_name + masking_tag: orig_name for orig_name in duplicated_names}
    df.rename(columns=replace_dct, inplace=True)
    

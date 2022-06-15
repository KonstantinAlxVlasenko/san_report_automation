# -*- coding: utf-8 -*-
"""
Created on Sun Jan  2 15:27:57 2022

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
db_file = r'MTS_south_analysis_database.db'
db_file = r"MTS_south_collection_database.db"

data_names = ['fdmi', 'nsshow', 'nscamshow', 'nsshow_dedicated', 'nsportshow']

before_fabricshow_df, before_ag_principal_df = read_database(db_path, db_file, *data_names)

before_chassis_df = read_database(db_path, db_file, *data_names)

after_df = read_database(db_path, db_file, *data_names)
after_fabricshow_df, after_ag_principal_df = read_database(db_path, db_file, *data_names)

before_portcmd_df = read_database(db_path, db_file, *data_names)
after_portcmd_df = read_database(db_path, db_file, *data_names)

before_sfp_df, before_cfg_df = read_database(db_path, db_file, *data_names)
after_sfp_df, after_cfg_df = read_database(db_path, db_file, *data_names)

data_names = ['fdmi', 'nsshow', 'nscamshow', 'nsshow_dedicated', 'nsportshow']
b_fdmi_df, b_nsshow_df, b_nscamshow_df, b_nsshow_dedicated_df, b_nsportshow_df = read_database(db_path, db_file, *data_names)
a_fdmi_df, a_nsshow_df, a_nscamshow_df, a_nsshow_dedicated_df, a_nsportshow_df = read_database(db_path, db_file, *data_names)


data_names = ['isl', 'trunk', 'porttrunkarea', 'lsdb']
b_data_lst = read_database(db_path, db_file, *data_names)
a_data_lst = read_database(db_path, db_file, *data_names)



data_names = ['fcrfabric', 'fcrproxydev', 'fcrphydev', 'lsan', 'fcredge', 'fcrresource']
db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Megafon\MegafonMSK\SAN Assessment\OCT21\database_MegafonMSK"
db_file = r"MegafonMSK_collection_database.db"
b_data_lst = read_database(db_path, db_file, *data_names)
a_data_lst = read_database(db_path, db_file, *data_names)


data_names = ['cfg', 'zone', 'alias', 'cfg_effective', 'zone_effective', 'peerzone' , 'peerzone_effective']
b_data_lst = read_database(db_path, db_file, *data_names)
a_data_lst = read_database(db_path, db_file, *data_names)

data_names = ['sensor']
b_data_lst = read_database(db_path, db_file, *data_names)
a_data_lst = read_database(db_path, db_file, *data_names)

data_names = ['errdump']

data_names = ['blade_interconnect', 'blade_servers', 'blade_vc']


db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_spb\database_MTS_spb"
db_file = 'MTS_spb_collection_database.db'
data_names = ['system_3par', 'port_3par', 'host_3par']


data_names = ['portshow_aggregated']

b_data_lst = read_database(db_path, db_file, *data_names)
a_data_lst = read_database(db_path, db_file, *data_names)


for data_name, before_df, after_df in zip(data_names, b_data_lst, a_data_lst):
    print('-------')
    print(data_name)
    if before_df.empty:
        print('empty')
    for column in before_df.columns:
        if not before_df[column].equals(after_df[column]):
            print(column)
print('-------')



b_df, = b_data_lst

mask_a_noteq_b = a_df['alias_Port_group_'] != b_df['alias_Port_group_']
mask_notna = a_df['alias_Port_group_'].notna()
a_alias_df = a_df.loc[mask_a_noteq_b & mask_notna].copy()


def remove_value_from_string(df, removed_value: str, *args, sep=', '):
    """Function to remove removed_value from strings in column"""

    for column in args:
        if df[column].notna().any():
            df[column].replace(f'{removed_value}(?:{sep})?', value='', regex=True, inplace=True)
            df[column] = df[column].str.rstrip(sep)
    return df


def sort_cell_values(df, *args, sep=', '):
    
    for column in args:
        mask_notna = df[column].notna()
        df[column] = df.loc[mask_notna, column].str.split(sep).apply(sorted).str.join(sep).str.strip(',')
            


def list_is_empty(lst):
    """Function to check if nested list is empty. None considered to be empty value"""

    print(lst)
    return all(map(list_is_empty, lst)) if isinstance(lst, list) else True if lst is None else False

lst_tst = [[], [1, None]]
lst_tst = []
list_is_empty(lst_tst)





b_df['alias_Port_group_'] = b_df['alias_Port_group']
b_df = remove_value_from_string(b_df, 'nan_device', 'alias_Port_group_')
sort_cell_values(b_df, 'alias_Port_group_')

a_df['alias_Port_group_'] = a_df['alias_Port_group']
a_df = remove_value_from_string(b_df, 'nan_device', 'alias_Port_group_')
sort_cell_values(a_df, 'alias_Port_group_')

b_df['Device_Host_Name_Port_group_'] = b_df['Device_Host_Name_Port_group']
b_df = remove_value_from_string(b_df, 'nan_device', 'Device_Host_Name_Port_group_')
sort_cell_values(b_df, 'Device_Host_Name_Port_group_')

a_df['Device_Host_Name_Port_group_'] = a_df['Device_Host_Name_Port_group']
a_df = remove_value_from_string(b_df, 'nan_device', 'Device_Host_Name_Port_group_')
sort_cell_values(a_df, 'Device_Host_Name_Port_group_')



before_df = b_nsportshow_df
afetr_df = a_nsportshow_df
for column in before_df.columns:
    if not before_df[column].equals(afetr_df[column]):
        print(column)

print(before_df['snmp_server'].equals(after_df['snmp_server']))

before_df['snmp_server']


print(before_df.equals(after_df))

['fdmi_columns', *['nsshow_columns']*3, 'nsportshow_columns']

def list_from_dataframe(df, *args, drop_na=True):
    """Function """

    result = [df[column].dropna().tolist() if drop_na else df[column].tolist() for column in args]
    # for column in args:
    #     result.append(df[column].dropna().tolist() if drop_na else df[column].tolist())
        
    return result if len(args)>1 else result[0]


list_from_dataframe(before_df, 'FabricID', 'timezone')
    
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
    # return data_imported if len(data_imported)>1 else data_imported[0]


def substitute_names(df):
    """Function to avoid column names duplication in sql.
    Capital and lower case letters don't differ iin sql.
    Function adds tag to duplicated column name when writes to the database
    and removes tag when read from the database."""

    masking_tag = '_sql'
    duplicated_names = ['SwitchName', 'Fabric_Name', 'SwitchMode', 'Memory_Usage', 'Flash_Usage', 'Speed']
    replace_dct = {orig_name + masking_tag: orig_name for orig_name in duplicated_names}
    df.rename(columns=replace_dct, inplace=True)
    

def status_info(status, max_title, len_info_string, shift=0):
    """Function to print current operation status ('OK', 'SKIP', 'FAIL')"""

    # information + operation status string length in terminal
    str_length = max_title + 80 + shift
    status = status.upper()
    # status info aligned to the right side
    # space between current operation information and status of its execution filled with dots
    print(status.rjust(str_length - len_info_string, '.'))
    return status
    
    
def find_files(folder, max_title, filename_contains='', filename_extension=''):
    """
    Function to create list with files. Takes directory, regex_pattern to verify if filename
    contains that pattern (default empty string) and filename extension (default is empty string)
    as parameters. Returns list of files with the extension deteceted in root folder defined as
    folder parameter and it's nested folders. If both parameters are default functions returns
    list of all files in directory
    """

    info = f'Checking {os.path.basename(folder)} folder for configuration files'
    print(info, end =" ") 

    # # check if ssave_path folder exist
    # check_valid_path(folder)
   
    # list to save configuration data files
    files_lst = []

    # going through all directories inside ssave folder to find configuration data
    for root, _, files in os.walk(folder):
        for file in files:
            if re.search(filename_contains, file):
                file_path = os.path.normpath(os.path.join(root, file))
                if filename_extension and file.endswith(filename_extension):
                    files_lst.append(file_path)
                elif not filename_extension and not re.search('\.', file):
                    files_lst.append(file_path)
                    
    if len(files_lst) == 0:
        status_info('no data', max_title, len(info))
    else:
        status_info('ok', max_title, len(info))        
    return files_lst

folder= r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_spb\3par_configs"
filename_extension = ''
# filename_contains = "(?:array_[A-Z\d]+_)?(?:config[._])?\d{6}[._]\d{6}(?:[._]\d{4})?"
filename_contains = ""

files_lst = []
for root, _, files in os.walk(folder):
    for file in files:
        if re.search(filename_contains, file):
            file_path = os.path.normpath(os.path.join(root, file))
            if filename_extension and file.endswith(filename_extension):
                files_lst.append(file_path)
            elif filename_extension is None and not re.search('.+\.(\d+)?[A-Za-z]+', file):
                files_lst.append(file_path)
            elif filename_extension=='':
                files_lst.append(file_path)

print(files_lst)


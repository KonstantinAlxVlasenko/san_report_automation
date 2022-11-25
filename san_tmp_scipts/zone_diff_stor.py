# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 15:08:19 2022

@author: kavlasenko
"""

import os
import warnings
import numpy as np
import re

import pandas as pd
script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop




# DataLine OST
db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN OST\NOV2022\database_DataLine Nord"
db_file = r"DataLine Nord_analysis_database.db"


data_names = ['zonemember_statistics']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)

zonemember_statistics_df, *_ = data_lst


zonemember_stat_notes_df =  zonemember_statistics_df.copy()

columns_lst = zonemember_stat_notes_df.columns.to_list()
storage_model_columns = [column for column in columns_lst if 'storage' in column.lower() and not column.lower() in ['storage', 'storage unique name']]


mask_exclude_zone = ~zonemember_stat_notes_df['Target_Initiator_note'].isin(['replication_zone', 'no_initiator'])
# check if zone contains storages of different models
if len(storage_model_columns) > 1:
    # zonemember_stat_notes_df['Storage_model_note'] = np.nan
    mask_different_storages = (zonemember_stat_notes_df[storage_model_columns] != 0).sum(axis=1).gt(1)
    zonemember_stat_notes_df['Storage_model_note'] = np.where(mask_exclude_zone & mask_different_storages, 'different_storages', pd.NA)
else:
    zonemember_stat_notes_df['Storage_model_note'] = np.nan
    
    
    
zonemember_stat_notes_df['Storage_count'] = (zonemember_stat_notes_df[storage_model_columns] != 0).sum(axis=1)

mask_100 = zonemember_stat_notes_df['Storage_count'] > 100
zonemember_stat_notes_df.loc[mask_100].empty
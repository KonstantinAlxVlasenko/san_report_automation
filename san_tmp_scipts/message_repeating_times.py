# -*- coding: utf-8 -*-
"""
Created on Sat Nov 25 16:23:24 2023

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
db_path = r"D:\Documents\01.CUSTOMERS\Rostrud\SAN\NOV2023\database_Rostrud"
db_file = r"Rostrud_analysis_database.db"


data_names = ['errdump_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)

errdump_aggregated_df, *_ = data_lst


# errdump_aggregated_df =  zonemember_statistics_df.copy()

errdump_aggregated_df['Message_repeated_times_cp'] = errdump_aggregated_df['Message_repeated_times']


errdump_aggregated_df['Message_repeated_times'] = errdump_aggregated_df['Message_repeated_times'].astype(float)
errdump_aggregated_df['Message_triggered_times'] = errdump_aggregated_df['Message_triggered_times'].astype(float)



errdump_aggregated_df['Message_repeated_times_filled'] = errdump_aggregated_df['Message_repeated_times'].fillna(0)
errdump_aggregated_df['Message_triggered_times_filled'] = errdump_aggregated_df['Message_triggered_times'].fillna(1)





errdump_aggregated_df['Message_triggered_times_total'] = errdump_aggregated_df['Message_triggered_times_filled'] + errdump_aggregated_df['Message_triggered_times_filled'] * errdump_aggregated_df['Message_repeated_times_filled']
errdump_aggregated_df['Message_triggered_times_unique'] = 1 + errdump_aggregated_df['Message_repeated_times_filled']

errdump_aggregated_df = dfop.move_column(errdump_aggregated_df, cols_to_move=['Message_triggered_times_single', 'Message_triggered_times_total'], ref_col='Message_triggered_times')



errdump_aggregated_df.drop(columns=['Message_repeated_times_filled', 'Message_triggered_times_filled'], inplace=True)



errdump_aggregated_df['Message_repeated_from_triggered_times'] = errdump_aggregated_df['Message_triggered_times'] - 1
errdump_aggregated_df['Message_repeated_from_triggered_times'] = errdump_aggregated_df['Message_repeated_from_triggered_times'].fillna(0)


errdump_aggregated_df['Message_total_repeated_times'] = errdump_aggregated_df['Message_repeated_times'].fillna(0)
errdump_aggregated_df['Message_total_repeated_times'] = errdump_aggregated_df['Message_total_repeated_times'] + errdump_aggregated_df['Message_repeated_from_triggered_times']


errdump_aggregated_df = dfop.move_column(errdump_aggregated_df, cols_to_move=['Message_repeated_times_cp', 'Message_total_repeated_times', 'Message_triggered_times', 'Message_repeated_from_triggered_times'], ref_col='Message_repeated_times')



errdump_aggregated_df['Message_triggered_times_cp'] = 


mask_triggered_notna = errdump_aggregated_df['Message_triggered_times'].notna()

errdump_aggregated_df = dfop.move_column(errdump_aggregated_df, cols_to_move='Message_repeated_from_triggered_times', ref_col='Message_triggered_times')



errdump_aggregated_df['Message_repeated_times'] = errdump_aggregated_df['Message_repeated_times'] + (errdump_aggregated_df['Message_triggered_times'] - 1)


errdump_aggregated_df['Message_triggered_times'] - 1


1 + (np.nan - 1)

1 + np.nan
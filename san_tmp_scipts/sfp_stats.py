# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 13:27:50 2023

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
db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN NORD\NOV2022\database_DataLine_Nord"
db_file = r"DataLine_Nord_analysis_database.db"


data_names = ['portshow_sfp_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)

portshow_sfp_aggregated_df, *_ = data_lst


transceiver_speed_pattern = r'((?:\d+,){2}\d+_\w+)'
transceiver_mode_patternt = r'(?:\d+,){2}\d+_\w+.*?(\w+w)'
float_str_pattern = r'-?[\d\.]+'







portshow_sfp_aggregated_cp_df =  portshow_sfp_aggregated_df.copy()

# rename class to form factor
portshow_sfp_aggregated_cp_df.rename(columns={'Transceiver_class': 'Transceiver_form_factor'}, inplace=True)


# extract trnsceiver details
portshow_sfp_aggregated_cp_df['Transceiver_speed_extracted'] = portshow_sfp_aggregated_cp_df['Transceiver_mode'].str.extract(transceiver_speed_pattern)
portshow_sfp_aggregated_cp_df['Transceiver_mode_extracted'] = portshow_sfp_aggregated_cp_df['Transceiver_mode'].str.extract(transceiver_mode_patternt)
portshow_sfp_aggregated_cp_df = dfop.merge_columns(portshow_sfp_aggregated_cp_df, summary_column='Transceiver_speed_mode_extracted', 
                                                   merge_columns=['Transceiver_speed_extracted', 'Transceiver_mode_extracted'], 
                                                   sep=' ', drop_merge_columns=False, sort_summary=False)
portshow_sfp_aggregated_cp_df = dfop.merge_columns(portshow_sfp_aggregated_cp_df, summary_column='PortPhys_transceiver', 
                                                   merge_columns=['portPhys', 'Transceiver_speed_mode_extracted'], 
                                                   sep=' ', drop_merge_columns=False, sort_summary=False)

# count interval for column

# reading_column = 'Current_mAmps'
# lower_threshold = 6 
# upper_threshold = 8
# step = 1

# reading_column = 'TX_Power_dBm'
# lower_threshold = -4 
# upper_threshold = 4
# step = 2


def find_redings_intervals(readings_column: str, lower_threshold: int, upper_threshold: int, step: int, filter_online: bool=True) -> pd.DataFrame:

    float_readings_column = readings_column + '_float'
    copy_readings_column = readings_column + '_cp'
    interval_readings_column = readings_column + '_interval'
    interval_readings_tmp_column = interval_readings_column + '_tmp'
    not_float_readings = readings_column + '_not_float'
    
    # convert redings to float
    mask_float = portshow_sfp_aggregated_cp_df[readings_column].str.contains(float_str_pattern, na=False)
    portshow_sfp_aggregated_cp_df[float_readings_column] = portshow_sfp_aggregated_cp_df.loc[mask_float, readings_column]
    portshow_sfp_aggregated_cp_df[float_readings_column] = portshow_sfp_aggregated_cp_df[float_readings_column].astype('float64')
    
    # copy text reding column to visually compare
    portshow_sfp_aggregated_cp_df[copy_readings_column] = portshow_sfp_aggregated_cp_df[readings_column]
    
    
    
    # find intervals
    mask_online = portshow_sfp_aggregated_cp_df['portState'] == 'Online'
    mask_sfp_present = portshow_sfp_aggregated_cp_df['portPhys'] != 'No_Module'
    
    # less then lower_threshold
    lower_threshold_mask = portshow_sfp_aggregated_cp_df[float_readings_column] < lower_threshold
    mask_lower = mask_online & mask_sfp_present & lower_threshold_mask if filter_online else mask_sfp_present & lower_threshold_mask
    portshow_sfp_aggregated_cp_df.loc[mask_lower, interval_readings_column] = 'x < ' + str(lower_threshold)
    
    # readingss intervals
    current_lower_threshold = lower_threshold
    while current_lower_threshold < upper_threshold:
        current_upper_threshold = current_lower_threshold + step
        if current_upper_threshold > upper_threshold:
            current_upper_threshold = upper_threshold
        print(f'{current_lower_threshold, current_upper_threshold}')
        mask_within_interval = portshow_sfp_aggregated_cp_df[float_readings_column].between(left=current_lower_threshold, right=current_upper_threshold, inclusive='left')
        
        mask_interval = mask_online & mask_sfp_present & mask_within_interval if filter_online else  mask_sfp_present & mask_within_interval
        
        portshow_sfp_aggregated_cp_df.loc[mask_interval, interval_readings_tmp_column] = str(current_lower_threshold) + ' <= x < ' + str(current_upper_threshold)    
        portshow_sfp_aggregated_cp_df[interval_readings_column].fillna(portshow_sfp_aggregated_cp_df[interval_readings_tmp_column], inplace=True)
        current_lower_threshold += step
    
    # more then upper threshold
    upper_threshold_mask = portshow_sfp_aggregated_cp_df[float_readings_column] >= upper_threshold
    mask_upper = mask_online & mask_sfp_present & upper_threshold_mask if filter_online else mask_sfp_present & upper_threshold_mask
    portshow_sfp_aggregated_cp_df.loc[mask_upper, interval_readings_tmp_column] = 'x >= ' + str(upper_threshold)
    portshow_sfp_aggregated_cp_df[interval_readings_column].fillna(portshow_sfp_aggregated_cp_df[interval_readings_tmp_column], inplace=True)
    
    # find module but no float data
    mask_uncategorized = mask_online & mask_sfp_present if filter_online else mask_sfp_present
    portshow_sfp_aggregated_cp_df[interval_readings_tmp_column] =  portshow_sfp_aggregated_cp_df.loc[mask_uncategorized, interval_readings_tmp_column]
    portshow_sfp_aggregated_cp_df[interval_readings_column].fillna(portshow_sfp_aggregated_cp_df[interval_readings_tmp_column], inplace=True)
    
    # drop tmp columns
    portshow_sfp_aggregated_cp_df.drop(columns=interval_readings_tmp_column, inplace=True)
    
portshow_sfp_aggregated_cp_df.columns.tolist()

    
        
readings_lst = [['RX_Power_dBm', -6, 2, 2, True], 
                ['TX_Power_dBm', -6, 2, 2, True],
                ['RX_Power_uW', 200, 1100, 200, True],
                ['TX_Power_uW', 200, 1100, 200, True],
                ['Temperature_Centigrade', 30, 60, 30, False],
                ['Pwr_On_Time_years', 1, 3, 2, False]]


for redings_column in readings_lst:
    find_redings_intervals(*redings_column)
        


# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 18:25:59 2023

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




# # DataLine OST
# db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN NORD\NOV2022\database_DataLine_Nord"
# db_file = r"DataLine_Nord_analysis_database.db"

# DataLine SPb
db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN SPB\JUL2023\database_DataLine_SPb"
db_file = r"DataLine_SPb_analysis_database.db"

 

data_names = ['portshow_sfp_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)

portshow_sfp_aggregated_df, *_ = data_lst


transceiver_speed_pattern = r'((?:\d+,){2}\d+_\w+)'
transceiver_mode_patternt = r'(?:\d+,){2}\d+_\w+.*?(\w+w)'
float_str_pattern = r'-?[\d\.]+'







portshow_sfp_aggregated_cp_df =  portshow_sfp_aggregated_df.copy()

# # rename class to form factor
# portshow_sfp_aggregated_cp_df.rename(columns={'Transceiver_class': 'Transceiver_form_factor'}, inplace=True)


# extract trnsceiver details
portshow_sfp_aggregated_cp_df['Transceiver_speed_extracted'] = portshow_sfp_aggregated_cp_df['Transceiver_mode'].str.extract(transceiver_speed_pattern)
portshow_sfp_aggregated_cp_df['Transceiver_mode_extracted'] = portshow_sfp_aggregated_cp_df['Transceiver_mode'].str.extract(transceiver_mode_patternt)
portshow_sfp_aggregated_cp_df = dfop.merge_columns(portshow_sfp_aggregated_cp_df, summary_column='Transceiver_speed_mode_extracted', 
                                                   merge_columns=['Transceiver_speed_extracted', 'Transceiver_mode_extracted'], 
                                                   sep=' ', drop_merge_columns=False, sort_summary=False)
portshow_sfp_aggregated_cp_df = dfop.merge_columns(portshow_sfp_aggregated_cp_df, summary_column='PortPhys_transceiver', 
                                                   merge_columns=['portPhys', 'Transceiver_speed_mode_extracted'], 
                                                   sep=' ', drop_merge_columns=False, sort_summary=False)

# add annotation to the intervals
readings_columns = [['RX_Power_dBm', True],
                    ['TX_Power_dBm', True],
                    ['RX_Power_uW', True],
                    ['TX_Power_uW', True],
                    ['Temperature_Centigrade', False],
                    ['Pwr_On_Time_years', False]
                    ]

for readings_column, include_sfp_mode in readings_columns:
    
    stat_readings_column = readings_column + '_stats'
    interval_readings_column = readings_column + '_interval'
    
    # add column name
    mask_interval_notna = portshow_sfp_aggregated_cp_df[interval_readings_column].notna()
    if include_sfp_mode:
        # portshow_sfp_aggregated_cp_df[stat_readings_column] = readings_column + " " + portshow_sfp_aggregated_cp_df.loc[mask_interval_notna, 'Transceiver_mode_extracted'] +\
        #     " interval " + "'" + portshow_sfp_aggregated_cp_df.loc[mask_interval_notna, interval_readings_column] + "'"
            
        portshow_sfp_aggregated_cp_df[stat_readings_column] = readings_column + " interval " + \
            "(sfp " + portshow_sfp_aggregated_cp_df.loc[mask_interval_notna, 'Transceiver_mode_extracted'] + ") " +\
            "'" + portshow_sfp_aggregated_cp_df.loc[mask_interval_notna, interval_readings_column] + "'"
    else:
        portshow_sfp_aggregated_cp_df[stat_readings_column] = readings_column + " interval " + "'" + \
            portshow_sfp_aggregated_cp_df.loc[mask_interval_notna, interval_readings_column] + "'"
            
# transceiver support
mask_supported = portshow_sfp_aggregated_cp_df['Transceiver_Supported'] == 'Yes'
mask_not_supported = portshow_sfp_aggregated_cp_df['Transceiver_Supported'] == 'No'
portshow_sfp_aggregated_cp_df.loc[mask_supported, 'Transceiver_Supported_stats'] = 'Supported SFP'
portshow_sfp_aggregated_cp_df.loc[mask_not_supported, 'Transceiver_Supported_stats'] = 'Not supported SFP'
portshow_sfp_aggregated_cp_df['Transceiver_Supported_stats'].fillna(portshow_sfp_aggregated_cp_df['Transceiver_Supported'], inplace=True)


def find_special_sfp(sfp_df, specification_column: str, specification_name: str, normal_value: str, upper_case_spec: bool=False):

    special_sfp_column = specification_column + '_stats'    

    if specification_column in sfp_df.columns \
        and sfp_df[specification_column].nunique() > 1: 
            mask_normal_sfp = sfp_df[specification_column] == normal_value
            sfp_df.loc[~mask_normal_sfp, special_sfp_column] = specification_name + ' ' + (
                sfp_df[specification_column].str.upper() if upper_case_spec else sfp_df[specification_column])
    else:
        sfp_df[special_sfp_column] = np.nan    



# transceiver form factor
find_special_sfp(portshow_sfp_aggregated_cp_df, specification_column='Transceiver_form_factor', specification_name='Form factor', normal_value='sfp', upper_case_spec=True)

if 'Transceiver_form_factor' in portshow_sfp_aggregated_cp_df.columns \
    and portshow_sfp_aggregated_cp_df['Transceiver_form_factor'].nunique() > 1: 
        mask_regular_sfp = portshow_sfp_aggregated_cp_df['Transceiver_form_factor'] == 'sfp'
        portshow_sfp_aggregated_cp_df.loc[mask_regular_sfp, 'Transceiver_form_factor_stats'] = 'Form factor ' + portshow_sfp_aggregated_cp_df['Transceiver_form_factor'].str.upper()
else:
    portshow_sfp_aggregated_cp_df['Transceiver_form_factor_stats'] = np.nan
    
    
# distance
find_special_sfp(portshow_sfp_aggregated_cp_df, specification_column='Transceiver_distanceMax', specification_name='Distance', normal_value='normal')
if 'Transceiver_distanceMax' in portshow_sfp_aggregated_cp_df.columns \
    and portshow_sfp_aggregated_cp_df['Transceiver_distanceMax'].nunique() > 1: 
        mask_normal_distance = portshow_sfp_aggregated_cp_df['Transceiver_distanceMax'] == 'normal'
        portshow_sfp_aggregated_cp_df.loc[~mask_normal_distance, 'Transceiver_distanceMax_stats'] = 'Distance ' + portshow_sfp_aggregated_cp_df['Transceiver_distanceMax']
else:
    portshow_sfp_aggregated_cp_df['Transceiver_distanceMax_stats'] = np.nan 

# vendor and part number    
portshow_sfp_aggregated_cp_df = dfop.merge_columns(portshow_sfp_aggregated_cp_df, summary_column='Transceiver_Name_PN', merge_columns=['Transceiver_Name', 'Transceiver_PN', 'Transceiver_speed_mode_extracted'], sep=' ', drop_merge_columns=False)


# drop duplicated port rows
portshow_sfp_aggregated_cp_df.drop_duplicates(subset=['configname', 'chassis_name', 'chassis_wwn', 
                                                        'switchName', 'switchWwn', 'slot', 'port'], inplace=True)

# port_quantity column
portshow_sfp_aggregated_cp_df['Port_quantity'] = 'Port_quantity'

# transceiver quantity column
mask_sfp_pn_notna = portshow_sfp_aggregated_cp_df['Transceiver_PN'].notna()
portshow_sfp_aggregated_cp_df.loc[mask_sfp_pn_notna, 'Transceiver_quantity'] = 'Transceiver_quantity'



sfp_stats_columns = ['Port_quantity', 'Port_license', 'Transceiver_quantity', 'Transceiver_Supported_stats',
                    'Transceiver_form_factor_stats', 'Transceiver_distanceMax_stats', 
                    'Transceiver_speed_mode_extracted', 'Transceiver_Name_PN',
                    'PortPhys_transceiver',
                    'RX_Power_dBm_stats', 'TX_Power_dBm_stats', 'RX_Power_uW_stats', 'TX_Power_uW_stats',
                    'Temperature_Centigrade_stats', 'Pwr_On_Time_years_stats']


switch_columns = ['Fabric_name', 'Fabric_label', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn']


# sfp_statistics_switch_df = dfop.count_frequency(portshow_sfp_aggregated_cp_df, count_columns=sfp_stats_columns, 
#                                          group_columns=switch_columns, margin_column_row=(False, False))


sfp_statistics_switch_df = dfop.count_statistics(portshow_sfp_aggregated_cp_df, connection_grp_columns=switch_columns, stat_columns=sfp_stats_columns)

sfp_statistics_summary_df = dfop.count_summary(sfp_statistics_switch_df, group_columns=['Fabric_name', 'Fabric_label'])


sfp_statistics_all_df = dfop.count_all_row(sfp_statistics_summary_df)

sfp_statistics_df = dfop.concat_statistics(sfp_statistics_switch_df, sfp_statistics_summary_df, sfp_statistics_all_df, sort_columns=switch_columns)

portshow_sfp_aggregated_cp_df.columns.tolist()
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 15 17:53:17 2023

@author: kavlasenko
"""

import os
import warnings
import numpy as np
import re
import warnings

import pandas as pd
script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop


# DataLine VC67
db_path = r"D:\Documents\01.CUSTOMERS\Hoff\SAN\AUG2023\database_Hoff_3Data"

# db_file = r"Hoff_3Data_analysis_database.db"
# data_names = ['switch_params_aggregated']
# data_lst = dfop.read_database(db_path, db_file, *data_names)
# data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
# switch_params_aggregated_df, *_ = data_lst


db_file = r"Hoff_3Data_collection_database.db"
data_names = ['chassis_parameters', 'chassisshow']
data_lst = dfop.read_database(db_path, db_file, *data_names)
chassis_params_df, chassisshow_df, *_ = data_lst

# filter chassis/wwn slots
mask_slot_wwn = chassisshow_df['Slot_Unit_name'].str.contains(pat='wwn', regex=True, flags=re.IGNORECASE, na=None)
chassisshow_wwn_df = chassisshow_df.loc[mask_slot_wwn].copy()


# clean 'none' ssn
mask_sn_none = chassis_params_df['ssn'] == 'none'
chassis_params_df.loc[mask_sn_none, 'ssn'] = None

# # chassis factory sn (directors)
# chassis_factory_sn_df = chassisshow_wwn_df.dropna(subset=['Chassis_Factory_Serial_Num'])
# chassis_factory_sn_df['ssn'] = chassis_factory_sn_df['Chassis_Factory_Serial_Num']
# chassis_params_df = dfop.dataframe_fillna(chassis_params_df, chassis_factory_sn_df, join_lst=['configname'], filled_lst=['ssn'])

# chassis wwn sn (OEM switches with sn)
# chassis_wwn_sn_df = chassisshow_wwn_df.dropna(subset=['Serial_Num'])
# if chassis_wwn_sn_df['Serial_Num'].notna().any():
#     chassis_wwn_sn_df = chassis_wwn_sn_df.groupby(['configname', 'chassis_name'])['Serial_Num'].agg(', '.join).reset_index()
#     chassis_wwn_sn_df['ssn'] = chassis_wwn_sn_df['Serial_Num']
#     chassis_params_df = dfop.dataframe_fillna(chassis_params_df, chassis_wwn_sn_df, join_lst=['configname'], filled_lst=['ssn'])

# # chassis wwn factory sn (Brocade switches with factory sn)
# chassis_wwn_factory_sn_df = chassisshow_wwn_df.dropna(subset=['Factory_Serial_Num'])
# chassis_wwn_factory_sn_df['ssn'] = chassis_wwn_factory_sn_df['Factory_Serial_Num']
# chassis_params_df = dfop.dataframe_fillna(chassis_params_df, chassis_wwn_factory_sn_df, join_lst=['configname'], filled_lst=['ssn'])

# # add OEM_ID
# oem_id_df = chassisshow_wwn_df.dropna(subset=['OEM_ID'])
# chassis_params_df = dfop.dataframe_fillna(chassis_params_df, oem_id_df, join_lst=['configname'], filled_lst=['OEM_ID'])


def chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column, filled_column):
    """Function to fill chassisshow_column in chassis_params_df dataframe with values
    from filled_column of chassisshow_wwn_df dataframe"""
    
    # drop rows with empty values in chassisshow_column
    chassisshow_wwn_nafree_df = chassisshow_wwn_df.dropna(subset=[chassisshow_column]).copy()
    # for directors join slot factory serial numbers
    if chassisshow_wwn_nafree_df[chassisshow_column].notna().any():
        chassisshow_wwn_nafree_df = chassisshow_wwn_nafree_df.groupby('configname')[chassisshow_column].agg(', '.join).reset_index()
    # fill empty values in chassis_params_df
    chassisshow_wwn_nafree_df[filled_column] = chassisshow_wwn_nafree_df[chassisshow_column]
    chassis_params_df = dfop.dataframe_fillna(chassis_params_df, chassisshow_wwn_nafree_df, join_lst=['configname'], filled_lst=[filled_column])
    return chassis_params_df

# chassis/wwn sn (OEM switches with sn)
chassis_params_df = chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column='Serial_Num', filled_column='ssn')
# chassis factory sn (directors)
chassis_params_df = chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column='Chassis_Factory_Serial_Num', filled_column='factory_sn')
# chassis/wwn factory sn (Brocade switches with factory sn)
chassis_params_df = chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column='Factory_Serial_Num', filled_column='factory_sn')
# add OEM_ID
chassis_params_df = chassis_param_fillna(chassis_params_df, chassisshow_wwn_df, chassisshow_column='OEM_ID', filled_column='OEM_ID')



chassisshow_wwn_df.columns.to_list()
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 10:54:36 2023

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
db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN NORD\VC67\JUL2023\database_DataLine_VC6-VC7"
db_file = r"DataLine_VC6-VC7_collection_cp_database.db"

 

data_names = ['system_oceanstor', 'port_oceanstor']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
system_oceanstor_df, port_oceanstor_df, *_ = data_lst

db_file = r"DataLine_VC6-VC7_analysis_database.db"
data_names = ['portshow_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
portshow_aggregated_df, *_ = data_lst

# system_oceanstor_df.columns.tolist()


mask_4u_enclosure = port_oceanstor_df['ID'].str.contains(pat='(H|L)(\d+)')
port_oceanstor_df['deviceSubtype'] = 'HUAWEI'
mask_huawei = port_oceanstor_df['deviceSubtype'] == 'HUAWEI'

mask_huawei & mask_4u_enclosure

        # mask = df[extracted_column].str.contains(pattern, regex=True)
port_oceanstor_df.loc[mask_huawei & mask_4u_enclosure, ['Quadrant_location', 'Quadrant_slot']] = port_oceanstor_df.loc[mask_huawei & mask_4u_enclosure, 'ID'].str.extract(pat='(H|L)(\d+)').values
port_oceanstor_df['Quadrant_slot'] = port_oceanstor_df['Quadrant_slot'].astype('float', errors='ignore').astype('int', errors='ignore')


mask_high = port_oceanstor_df['Quadrant_location'] == 'H'
mask_low = port_oceanstor_df['Quadrant_location'] == 'L'
mask_left = port_oceanstor_df['Quadrant_slot'].between(0, 6, inclusive='both')
mask_right = port_oceanstor_df['Quadrant_slot'].between(7, 13, inclusive='both')

port_oceanstor_df.loc[mask_high & mask_left, 'Quadrant_controller'] = 'A'
port_oceanstor_df.loc[mask_low & mask_left, 'Quadrant_controller'] = 'B'

port_oceanstor_df.loc[mask_high & mask_right, 'Quadrant_controller'] = 'C'
port_oceanstor_df.loc[mask_low & mask_right, 'Quadrant_controller'] = 'D'



# mask_hw = (portshow_aggregated_df[['deviceType', 'deviceSubtype']].apply()  str.lower() == ('storage', 'huawei')).all(axis=1)
portshow_aggregated_df.loc[mask_hw, ['Device_Name', 'Device_Host_Name']] = (None, None)

def storage_oceanstore_fillna(portshow_aggregated_df, system_oceanstor_df, port_oceanstor_df):
    """Function to add Huawei OceanStore information collected from configuration files to
    portshow_aggregated_df"""
    
    if not port_oceanstor_df.empty and not system_oceanstor_df.empty:
        # system information
        system_columns = ['configname', 'Product_Model', 'System_Name', 
                            'Product_Serial_Number', 'IP_Address', 'Point Release', 'System_Location']
        system_oceanstor_cp_df = system_oceanstor_df[system_columns].copy()
        system_oceanstor_cp_df.drop_duplicates(inplace=True)
    
        # add system information to 3PAR ports DataFrame
        system_port_oceanstor_df = port_oceanstor_df.merge(system_oceanstor_cp_df, how='left', on=['configname'])
        # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
        system_port_oceanstor_df = dfop.convert_wwn(system_port_oceanstor_df, ['WWN'])
        # max speed
        system_port_oceanstor_df['Device_portSpeed_max'] = \
            system_port_oceanstor_df['Max_Speed(Mbps)'].astype('int32', errors='ignore')/1000
        # rename columns to correspond portshow_aggregated_df
        rename_columns = {'System_Name': 'Device_Name',	'Product_Model': 'Device_Model', 
                            'Product_Serial_Number': 'Device_SN', 'System_Location': 'Device_Location',
                            'Point Release': 'Device_Fw', 'ID': 'Device_Port', 'WWN': 'PortName',
                            'Type': 'Storage_Port_Type', 'Role': 'Storage_Port_Mode'}
        system_port_oceanstor_df.rename(columns=rename_columns, inplace=True)
        system_port_oceanstor_df['Device_Host_Name'] = system_port_oceanstor_df['Device_Name']
        # add OceanStore information to portshow_aggregated_df
        fillna_wwpn_columns = ['Device_Name', 'Device_Host_Name', 'Device_Model', 'Device_SN', 
                               'Device_Location', 'Device_Fw', 'Device_Port', 'IP_Address', 
                               'Storage_Port_Type', 'Storage_Port_Mode', 'Device_portSpeed_max']        
        portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, system_port_oceanstor_df, 
                                                       join_lst=['PortName'], 
                                                       filled_lst=fillna_wwpn_columns)
    return portshow_aggregated_df





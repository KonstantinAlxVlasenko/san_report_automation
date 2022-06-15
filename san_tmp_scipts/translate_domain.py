# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 13:54:49 2022

@author: vlasenko
"""

import os
script_dir = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop

import numpy as np

# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Rosneft\JAN2022\database_rnc.rosneft"
# db_file = r"rnc.rosneft_collection_database.db"

db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\JAN2022\mts_msc\database_MTS_msk"
db_analysis_file = r"MTS_msk_analysis_database.db"
db_collection_file = r"MTS_msk_collection_database.db"


data_names_analysis = ['switch_params_aggregated', 'portshow_aggregated']
data_names_collection = ['fcrproxydev', 'fcrxlateconfig']


switch_params_aggregated_df, portshow_aggregated_df = dfop.read_database(db_path, db_analysis_file, *data_names_analysis)
fcrproxydev_df, fcrxlateconfig_df = dfop.read_database(db_path, db_collection_file, *data_names_collection)


fcrproxydev_cp_df = fcrproxydev_df.copy()
fcrxlateconfig_cp_df = fcrxlateconfig_df.copy()

fcrproxydev_cp_df['switchWwn'] = fcrproxydev_cp_df['principal_switchWwn']
fcrproxydev_cp_df = dfop.dataframe_fillna(fcrproxydev_cp_df, switch_params_aggregated_df, join_lst=['switchWwn'], filled_lst=['Fabric_name', 'Fabric_label'])
fcrxlateconfig_cp_df = dfop.dataframe_fillna(fcrxlateconfig_cp_df, switch_params_aggregated_df, join_lst=['switchWwn'], filled_lst=['Fabric_name', 'Fabric_label'])



fcrproxydev_cp_df['ImportedFid'] = fcrproxydev_cp_df['Proxy_Created_in_Fabric']
fcrproxydev_cp_df['ExportedFid'] = fcrproxydev_cp_df['Device_Exists_in_Fabric']

for column in ['ImportedFid', 'ExportedFid', 'Domain', 'OwnerDid']:
    fcrxlateconfig_cp_df[column] = fcrxlateconfig_cp_df[column].str.lstrip('0')

# fcrxlateconfig_cp_df['ImportedFid'] = fcrxlateconfig_cp_df['ImportedFid'].str.lstrip('0')
# fcrxlateconfig_cp_df['ExportedFid'] = fcrxlateconfig_cp_df['ExportedFid'].str.lstrip('0')

fcr_xd_proxydev_columns = ['Fabric_name', 'Fabric_label', 'XlateWWN', 'ImportedFid', 'ExportedFid', 'Domain', 'OwnerDid']
fcr_xd_proxydev_df = fcrxlateconfig_cp_df[fcr_xd_proxydev_columns].copy()


fcrproxydev_cp_df.columns.tolist()
fcr_xd_proxydev_df.columns.tolist()


fcr_xd_proxydev_df = dfop.dataframe_fillna(fcr_xd_proxydev_df, fcrproxydev_cp_df, join_lst=['Fabric_name', 'Fabric_label', 'ImportedFid', 'ExportedFid'], 
                                           filled_lst=['Device_portWwn', 'Proxy_PID', 'Physical_PID'], remove_duplicates=False)

fcr_xd_proxydev_df.rename(columns={'Device_portWwn': 'Connected_portWwn'}, inplace=True)
fcr_xd_proxydev_df.drop(columns=['Fabric_name', 'Fabric_label'], inplace=True)


device_columns = ['Fabric_name', 'Fabric_label', 
                  'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn',
                  'portIndex', 'slot', 'port', 'Connected_portId',
                  'speed', 'portType',
                  'Device_Host_Name', 'Device_Port', 'alias',
                  'LSAN', 'deviceType', 'deviceSubtype']

fcr_xd_proxydev_df = dfop.dataframe_fillna(fcr_xd_proxydev_df, portshow_aggregated_df, join_lst=['Connected_portWwn'], filled_lst=device_columns)

device_rename_dct = {column: 'Device_' + column for column in device_columns[:6]}
device_rename_dct['XlateWWN'] = 'switchWwn'
fcr_xd_proxydev_df.rename(columns=device_rename_dct, inplace=True)


switch_columns = ['Fabric_name', 'Fabric_label', 'switchName']
fcr_xd_proxydev_df = dfop.dataframe_fillna(fcr_xd_proxydev_df, switch_params_aggregated_df, join_lst=['switchWwn'], filled_lst=switch_columns)
fcr_xd_proxydev_df = dfop.move_column(fcr_xd_proxydev_df, cols_to_move=switch_columns, ref_col='switchWwn', place='before')

# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 15:53:51 2023

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
db_file = r"DataLine_VC6-VC7_collection_database.db"

 

data_names = ['system_oceanstor', 'port_oceanstor', 'host_oceanstor', 
              'host_id_name_oceanstor', 'host_id_fcinitiator_oceanstor', 
              'hostid_ctrlportid_oceanstor']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
system_oceanstor_df, port_oceanstor_df, host_oceanstor_df, \
    host_id_name_oceanstor_df, host_id_fcinitiator_oceanstor_df, \
        hostid_ctrlportid_oceanstor_df, *_ = data_lst


db_file = r"DataLine_VC6-VC7_analysis_database.db"
data_names = ['portshow_aggregated', 'zoning_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
portshow_aggregated_df, zoning_aggregated_df, *_ = data_lst



# def get_oceanstor_hosts():


hostid_ctrlportid_oceanstor_cp_df = hostid_ctrlportid_oceanstor_df.copy()
hostid_ctrlportid_oceanstor_cp_df['LUN_quantity'] = hostid_ctrlportid_oceanstor_cp_df['LUN_ID_List'].str.count('\w+')

hostid_ctrlportid_oceanstor_cp_df = dfop.explode_columns(hostid_ctrlportid_oceanstor_cp_df, 'Port_ID_List', sep=',')

hostid_ctrlportid_oceanstor_cp_df.drop(columns=['LUN_ID_List', 'Exploded_column'], inplace=True)
hostid_ctrlportid_oceanstor_cp_df.rename(columns={'Exploded_values': 'Storage_Port'}, inplace=True)




# host_id_name_oceanstor_df.columns.to_list()

host_columns = ['configname', 'Host_Id', 'Host_Name', 'Os_Type', 'Host_IP']
storage_host_oceanstor_df = host_id_name_oceanstor_df[host_columns].copy()


# add host fc ports
storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, host_id_fcinitiator_oceanstor_df, 
                                                join_lst=['configname', 'Host_Id'], 
                                                filled_lst=['Host_Wwn'], remove_duplicates=False)
# add controller ports
storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, hostid_ctrlportid_oceanstor_cp_df, 
                                                join_lst=['configname', 'Host_Id'], 
                                                filled_lst=['Storage_Port', 'LUN_quantity'], remove_duplicates=False)
# add controller port Wwpn
port_oceanstor_cp_df = port_oceanstor_df.copy()
port_oceanstor_cp_df['PortName'] = port_oceanstor_df['WWN']
port_oceanstor_cp_df['Storage_Port'] = port_oceanstor_df['ID']
storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, port_oceanstor_cp_df, 
                                                join_lst=['configname', 'Storage_Port'], 
                                                filled_lst=['PortName'])



# for old storages
host__old_columns = ['configname', 'Host_Id', 'Host_Name', 'Os_Type', 'Host_IP', 'Host_Wwn']
storage_host_oceanstor_old_df = host_oceanstor_df[host__old_columns].copy()

# add controllers ports
mask_online_port = port_oceanstor_df['Running_Status'].str.contains('up', case=False, na=None)
port_oceanstor_cp_df = port_oceanstor_df.loc[mask_online_port].copy()
port_oceanstor_cp_df['PortName'] = port_oceanstor_df['WWN']
port_oceanstor_cp_df['Storage_Port'] = port_oceanstor_df['ID']
storage_host_oceanstor_old_df = dfop.dataframe_fillna(storage_host_oceanstor_old_df, port_oceanstor_cp_df, 
                                                join_lst=['configname'], 
                                                filled_lst=['Storage_Port', 'PortName'], remove_duplicates=False)
storage_host_oceanstor_old_df['Host_Wwn'] = storage_host_oceanstor_old_df['Host_Wwn'].str.extract('0x(.+)')


# concatenate old and new
storage_host_oceanstor_df = pd.concat([storage_host_oceanstor_df, storage_host_oceanstor_old_df], ignore_index=True)









# add system_name
storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, system_oceanstor_df, 
                                                join_lst=['configname'], filled_lst=['System_Name'])
# convert wwpn
storage_host_oceanstor_df = dfop.convert_wwn(storage_host_oceanstor_df, ['Host_Wwn', 'PortName'])
# add controllers ports Fabric_name and Fabric_label
storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, portshow_aggregated_df, 
                                                    join_lst=['PortName'], filled_lst=['Fabric_name', 'Fabric_label', 'NodeName'])
# rename
storage_host_oceanstor_df.rename(columns={'Os_Type': 'Persona'}, inplace=True)





# rename controllers NodeName and PortName
rename_columns = {'NodeName': 'Storage_Port_Wwnn', 'PortName': 'Storage_Port_Wwnp'}
storage_host_oceanstor_df.rename(columns=rename_columns, inplace=bool)
# 'clean' Wwn column to have Wwnp only. check Wwnn -> Wwnp correspondance in all fabrics
storage_host_oceanstor_df = dfop.replace_wwnn(storage_host_oceanstor_df, 'Host_Wwn', 
                                            portshow_aggregated_df, ['NodeName', 'PortName'])
# add Host Wwnp zoning device status in fabric of storage port connection
storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, zoning_aggregated_df, 
                                            join_lst=['Fabric_name', 'Fabric_label', 'PortName'], 
                                            filled_lst=['Fabric_device_status'])



# rename controllers Fabric_name and Fabric_label
rename_columns = {'Fabric_name': 'Storage_Fabric_name', 'Fabric_label': 'Storage_Fabric_label', 
                    'Fabric_device_status': 'Fabric_host_status'}
storage_host_oceanstor_df.rename(columns=rename_columns, inplace=bool)
# add host information
host_columns = ['Fabric_name', 'Fabric_label', 'chassis_name', 'switchName', 
                'Index_slot_port', 'portIndex', 'slot', 'port',  'Connected_portId', 
                'Device_Host_Name', 'Device_Port', 'Host_OS', 'Device_Location', 
                'Device_Host_Name_per_fabric_name_and_label',	'Device_Host_Name_per_fabric_label', 
                'Device_Host_Name_per_fabric_name', 'Device_Host_Name_total_fabrics']
storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, portshow_aggregated_df, 
                                                join_lst=['PortName'], filled_lst=host_columns, remove_duplicates=False)
# rename host columns
rename_columns = {'Fabric_name': 'Host_Fabric_name', 'Fabric_label': 'Host_Fabric_label', 'PortName': 'Host_Wwnp'}
storage_host_oceanstor_df.rename(columns=rename_columns, inplace=bool)


mask_fabric_label_notna = storage_host_oceanstor_df[['Host_Fabric_label', 'Storage_Fabric_label']].notna().all(axis=1)
mask_fabric_label_equal = storage_host_oceanstor_df['Host_Fabric_label'] == storage_host_oceanstor_df['Storage_Fabric_label']
storage_host_oceanstor_filtered_df = storage_host_oceanstor_df.loc[(mask_fabric_label_notna & mask_fabric_label_equal) | ~mask_fabric_label_notna].copy()
storage_host_oceanstor_filtered_df.reset_index(drop=True, inplace=True)
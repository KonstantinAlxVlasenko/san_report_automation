# -*- coding: utf-8 -*-
"""
Created on Mon May 30 12:57:41 2022

@author: vlasenko
"""

# -*- coding: utf-8 -*-
"""
Created on Fri May 20 14:37:25 2022

@author: vlasenko
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

# # MTS Moscow
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\JAN2022\mts_msc\database_MTS_msk"
# db_file = r"MTS_msk_analysis_database.db"


# # MTS Tech
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_tech\database_MTS_Techblock"
# db_file = r"MTS_Techblock_analysis_database.db"


# # NOVATEK MAR2022
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Novatek\SAN Assessment\MAR2022\database_Novatek"
# db_file = r"Novatek_analysis_database.db"




# # OTP
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\OTPBank\SAN Assessment DEC2020\database_OTPBank"
# db_file = r"OTPBank_analysis_database.db"


# # Mechel
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Mechel\SAN Assessment FEB21\database_Mechel"
# db_file = r"Mechel_analysis_database.db"


data_names = ['portshow_aggregated', 'npv_ag_connected_devices', 'fcr_xd_proxydev']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)


# data_names = ['fcrxlateconfig']

portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df, *_ = data_lst
# fcrxlateconfig_df, *_ = data_lst


SW_PAIR_NUMBER_PER_FABRIC_THRESHOLD = 8
DEVICE_FABRIC_NAME_SUFFIX = '_dev'
STORAGE_NUMBER_PER_FABRIC_THRESHOLD = 15

# filter portshow_aggregated
mask_portwwn_notna = portshow_aggregated_df['Connected_portWwn'].notna()
mask_device_lib_srv_stor = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
connected_devices_df = portshow_aggregated_df.loc[mask_portwwn_notna & mask_device_lib_srv_stor].copy()

# find pure npiv
mask_npiv = connected_devices_df['Device_type'].str.contains('NPIV', na=False)
connected_devices_df.loc[mask_npiv, 'port_NPIV'] = 'NPIV'
dev_columns = [*npv_ag_connected_devices_df.columns, 'port_NPIV']
connected_devices_df = connected_devices_df[dev_columns].copy()


# fill speed from access gateway ssave in npv_ag_connected_devices_df
mask_ag = portshow_aggregated_df['switchMode'].str.contains('Gateway', na=False)
portshow_ag_df = portshow_aggregated_df.loc[mask_ag]
# copy and clear speed column
npv_ag_connected_devices_cp_df = npv_ag_connected_devices_df.copy()
npv_ag_connected_devices_cp_df['speed_cp'] = npv_ag_connected_devices_cp_df['speed']
npv_ag_connected_devices_cp_df['speed'] = None
# fill speed from 
npv_ag_connected_devices_cp_df = dfop.dataframe_fillna(npv_ag_connected_devices_cp_df, portshow_ag_df, join_lst=['Fabric_name', 'Fabric_label', 'Connected_portWwn'], filled_lst=['speed'])
npv_ag_connected_devices_cp_df['speed'].fillna(npv_ag_connected_devices_cp_df['speed_cp'], inplace=True)
npv_ag_connected_devices_cp_df.drop(columns='speed_cp', inplace=True)




# remove devices from connected_devices_df if they exist in npv_ag_connected_devices_df
wwn_behind_ag_df = npv_ag_connected_devices_cp_df[['Fabric_name', 'Fabric_label', 'Connected_portWwn']].drop_duplicates()
connected_devices_df = pd.merge(connected_devices_df, wwn_behind_ag_df, how='left', indicator='Exist')
mask_unique_row = connected_devices_df['Exist'] == 'left_only'
connected_devices_df = connected_devices_df.loc[mask_unique_row].copy()
connected_devices_df.drop(columns='Exist', inplace=True)

# align fcr_xd_proxydev_df columns
fcr_xd_proxydev_cp_df = fcr_xd_proxydev_df.reindex(columns=dev_columns).copy()
# translate domain device connection speed is na
fcr_xd_proxydev_cp_df['speed'] = None

# concatenate connected devices
connected_devices_df = pd.concat([connected_devices_df, npv_ag_connected_devices_cp_df, fcr_xd_proxydev_cp_df])
connected_devices_df.drop_duplicates(subset=['Fabric_name', 'Fabric_label', 'switchWwn', 'Connected_portWwn'], inplace=True, ignore_index=True)

sw_device_connection_columns = ['Fabric_name', 'Fabric_label', 'switchWwn', 'Device_Host_Name', 'deviceType']

# count number of device links to the switch
connected_devices_df['Physical_link_quantity'] = connected_devices_df.groupby(by=['Fabric_name', 'Fabric_label', 'switchWwn', 'Device_Host_Name', 'deviceType'])['Connected_portWwn'].transform('count')

# switch shape name
create_shape_name(connected_devices_df, "switchName", "switchWwn", "switch_shapeName")

# add switchPair_Id
connected_devices_df = dfop.dataframe_fillna(connected_devices_df, switch_pair_df, join_lst=['switchWwn'], filled_lst=['switchPair_id'])

# modify subtype for srv and lib
connected_devices_df['deviceSubtype_mod'] = connected_devices_df['deviceSubtype']
connected_devices_df.loc[connected_devices_df['deviceType'].str.contains('SRV', na=False), 'deviceSubtype_mod'] = 'SRV'
connected_devices_df.loc[connected_devices_df['deviceType'].str.contains('LIB', na=False), 'deviceSubtype_mod'] = 'LIB'



# extract Enclosure and slot
enclosure_slot_pattern = r'(Enclosure .+?) slot (\d+)'
if connected_devices_df['Device_Location'].notna().any() and connected_devices_df['Device_Location'].str.contains(enclosure_slot_pattern, na=False).any():
    
    connected_devices_df[['Enclosure', 'slot']] = connected_devices_df['Device_Location'].str.extract(enclosure_slot_pattern).values
    connected_devices_df.loc[connected_devices_df['Enclosure'].notna(), 'Enclosure'] = connected_devices_df['Enclosure'] + ':'
    connected_devices_df.loc[connected_devices_df['slot'].notna(), 'slot_int'] = connected_devices_df.loc[connected_devices_df['slot'].notna(), 'slot'].astype('int')
else:
    connected_devices_df[['Enclosure', 'slot', 'slot_int']] = (None, ) * 3




# create link description for each switch->device->speed
device_links_df = connected_devices_df.groupby(by=['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'speed', 'deviceType'], dropna=False)['Connected_portWwn'].agg('count').to_frame()
device_links_df.reset_index(inplace=True)
device_links_df.rename(columns={'Connected_portWwn': 'Link_quantity'}, inplace=True)
device_links_df['Link_quantity'] = device_links_df['Link_quantity'].astype('str')

# link description is number of links 'x' speed and speed mode
device_links_df = dfop.merge_columns(device_links_df, summary_column='Link_description', merge_columns=['Link_quantity', 'speed'], sep='x')
# add 'x' to device XD links
device_links_df.loc[device_links_df['Link_description'].str.contains('^\d+$'), 'Link_description'] = device_links_df['Link_description'] + 'x'


# create link description for each switch->device 
device_links_df = device_links_df.groupby(by=['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'deviceType'])['Link_description'].agg(', '.join).to_frame()
device_links_df.reset_index(inplace=True)
dfop.sort_cell_values(device_links_df, 'Link_description')
# link description to create device groups with the same connections
device_links_df['switchPair_id_str'] = device_links_df['switchPair_id'].astype(int).astype(str)
device_links_df['Link_description_sw_pair'] = device_links_df['switchPair_id_str'] + device_links_df['Fabric_label'] + ': ' + device_links_df['Link_description']
device_links_df.drop(columns='switchPair_id_str', inplace=True)
# drop duplicated link so each row represents device connection to the switch
connected_devices_df.drop(columns=['Connected_portWwn', 'Connected_portId', 'speed'], inplace=True)
connected_devices_df.drop_duplicates(inplace=True, ignore_index=True)
# add link description
sw_device_connection_columns = ['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'deviceType']
connected_devices_df = pd.merge(connected_devices_df, device_links_df, how='left', on=sw_device_connection_columns)

# join link_description for all device connections to find devices with the samelink_description in fabric_name
connected_devices_df['Link_description_fabric_name'] = connected_devices_df.groupby(by=['Fabric_name', 'deviceType', 'Device_Host_Name'])['Link_description_sw_pair'].transform('; '.join)
# sort Link_description_fabric_name
dfop.sort_cell_values(connected_devices_df, 'Link_description_fabric_name', sep='; ')
# sort for future group in the correct order
connected_devices_df.sort_values(by=['Fabric_name', 'deviceType', 'Enclosure', 'slot_int', 'Device_Host_Name', 'switchPair_id', 'Fabric_label'], inplace=True)

# check if storage need to be grouped
mask_storage_lib = connected_devices_df['deviceType'].isin(['STORAGE', 'LIB'])
connected_storages_df = connected_devices_df.loc[mask_storage_lib].copy()
storage_number_df = connected_storages_df.groupby(by=['Fabric_name'])['Device_Host_Name'].nunique().to_frame(name='Storage_lib_number_in_fabric')
connected_devices_df = pd.merge(connected_devices_df, storage_number_df, how='left', on='Fabric_name')
connected_devices_df['Storage_lib_number_in_fabric'].fillna(0, inplace=True)




################
# verify if graph with devices need to be separated
# find fabric with devices
fabric_name_with_dev = connected_devices_df['Fabric_name'].unique()
# count switch_pairs in each fabric
fabric_name_sw_pair_count_df = san_graph_sw_pair_df.groupby(by=['Fabric_name'])['switchName_Wwn'].count().to_frame().reset_index()
fabric_name_sw_pair_count_df.rename(columns={'switchName_Wwn': 'switchPair_quantity'}, inplace=True)
# find fabric with devices with switch_pair number exceeding threshold
mask_fabric_with_dev = fabric_name_sw_pair_count_df['Fabric_name'].isin(fabric_name_with_dev)
mask_sw_pair_threshold = fabric_name_sw_pair_count_df['switchPair_quantity'] > SW_PAIR_NUMBER_PER_FABRIC_THRESHOLD
fabric_name_duplicated_lst = fabric_name_sw_pair_count_df.loc[mask_fabric_with_dev & mask_sw_pair_threshold, 'Fabric_name'].to_list()
fabric_name_dev_lst = [fabric_name + DEVICE_FABRIC_NAME_SUFFIX for fabric_name in fabric_name_duplicated_lst]
# rename fabric_name in connecetd_devices_df
connected_devices_df['Fabric_name_cp'] = connected_devices_df['Fabric_name']
connected_devices_df['Fabric_name'].replace(to_replace=fabric_name_duplicated_lst, value=fabric_name_dev_lst, inplace=True)
# update san_graph_sw_pair
san_graph_sw_pair_duplicated_df = san_graph_sw_pair_df.loc[san_graph_sw_pair_df['Fabric_name'].isin(fabric_name_duplicated_lst)].copy()
san_graph_sw_pair_duplicated_df['Fabric_name'].replace(to_replace=fabric_name_duplicated_lst, value=fabric_name_dev_lst, inplace=True)
san_graph_sw_pair_df = pd.concat([san_graph_sw_pair_df, san_graph_sw_pair_duplicated_df])


# storages links
mask_storage_lib = connected_devices_df['deviceType'].isin(['STORAGE', 'LIB'])
mask_threshold_exceeded = connected_devices_df['Storage_lib_number_in_fabric'] > STORAGE_NUMBER_PER_FABRIC_THRESHOLD

storage_under_threshold_df = connected_devices_df.loc[mask_storage_lib & ~mask_threshold_exceeded].copy()
storage_above_threshold_df = connected_devices_df.loc[mask_storage_lib & mask_threshold_exceeded].copy()
storage_shape_links_df = pd.concat([group_device_on_name(storage_under_threshold_df, san_graph_details_df), group_device_on_link_description(storage_above_threshold_df, san_graph_details_df)])

# server and unknown links
mask_srv_unknown = connected_devices_df['deviceType'].str.contains('SRV|UNKNOWN')
server_df = connected_devices_df.loc[mask_srv_unknown].copy()
server_shape_links_df = group_device_on_link_description(server_df, san_graph_details_df)

####
# create fn for devices groupes on Link_description_fabric_name

def group_device_on_link_description(connected_devices_df, san_graph_details_df):


    # add slot to Device_Host_Name
    connected_devices_df = dfop.merge_columns(connected_devices_df, summary_column='Device_Host_Name_slot', 
                                              merge_columns=['slot', 'Device_Host_Name'], sep='. ', 
                                              drop_merge_columns=False)
    
    
    # slot_Device_Host_Name > Device_Host_Name_slot
    # Device_fabric_group_number > Device_Group_number
    # Device_group_quantity > Device_quantity_in_group
    
    # numbering groups
    connected_devices_df['Device_Group_number'] = connected_devices_df.groupby(by=['Fabric_name', 'deviceType', 'deviceSubtype_mod', 'Enclosure', 'Link_description_fabric_name'], dropna=False).ngroup().add(1)
    # join device name based on group number
    connected_devices_df['Device_quantity_in_group'] = connected_devices_df.groupby(by=['Fabric_name', 'Device_Group_number'])['Device_Host_Name_slot'].transform('nunique')
    
    connected_devices_df['Device_shapeText'] = connected_devices_df.groupby(by=['Fabric_name', 'Fabric_label', 'Device_Group_number'])['Device_Host_Name_slot'].transform(', '.join)
    
    
    # remove duplicate device names if device is connected to multople switshes ain fabric label  and keep order for blade servers and sort for else
    connected_devices_df['Device_shapeText'] = connected_devices_df.apply(lambda series: remove_name_duplicates(series), axis=1)
        
    # add deviceClass ro group number
    connected_devices_df['Device_shapeName'] = connected_devices_df['deviceType'] + " GROUP #" + connected_devices_df['Device_Group_number'].astype(str)
    
    # add enclosure name
    mask_enclosure = connected_devices_df['Enclosure'].notna()
    connected_devices_df.loc[mask_enclosure, 'Device_shapeText'] = connected_devices_df['Enclosure'] + " " + connected_devices_df['Device_shapeText']
    
    # add link shape name
    connected_devices_df = dfop.merge_columns(connected_devices_df, summary_column='Link_shapeName', 
                                          merge_columns=['Device_shapeName', 'switch_shapeName'], sep=' - ', 
                                          drop_merge_columns=False)
    
    # group server dataframe
    device_shape_links_df = connected_devices_df.drop(columns=['Device_Host_Name', 'Device_Location', 'Device_Host_Name_slot', 'slot', 'slot_int']).copy()
    device_shape_links_df.drop_duplicates(inplace=True, ignore_index=True)
    # device_shape_links_df.sort_values(by=['Fabric_name',  'deviceType', 'Device_shapeText', 'Fabric_label', 'switchPair_id'], ignore_index=True, inplace=True)
    
    
    # add san graph details
    device_shape_links_df['switchClass_mode'] = device_shape_links_df['deviceType']
    device_shape_links_df['switchClass_mode'].replace({'UNKNOWN': 'UNKNOWN_DEV'}, inplace=True)
    device_shape_links_df = dfop.dataframe_fillna(device_shape_links_df, san_graph_details_df, join_lst=['switchClass_mode'], filled_lst=['switchClass_weight', 'master_shape', 'y_graph_level', 'x_group_offset'])
    device_shape_links_df.drop(columns='switchClass_mode', inplace=True)

    # sort by device
    device_shape_links_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'deviceSubtype_mod', 'Device_shapeText', 'Fabric_label', 'switchName'], inplace=True, ignore_index=True)

    return device_shape_links_df

def remove_name_duplicates(series):
    """Function to remove duplicated device names from Device_shapeText.
    Multiple names occur if server connected to multiple swithes in the same fabric_label"""
    
    device_lst = series['Device_shapeText'].split(', ')
    unique_device_lst = list(dict.fromkeys(device_lst))
    
    if series['deviceType'] in ['SRV', 'UNKNOWN', 'STORAGE', 'LIB']:
        unique_device_lst = sorted(unique_device_lst)
    return ', '.join(unique_device_lst)



##############
# fn to group device based on Host Name
def group_device_on_name(connected_devices_df, san_graph_details_df):

    device_shape_links_df = connected_devices_df.copy()
    
    # create storage and lib shape name
    warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
    mask_wwn_in_name = device_shape_links_df['Device_Host_Name'].str.contains("([0-9a-f]{2}:){7}[0-9a-f]{2}")
    device_shape_links_df.loc[~mask_wwn_in_name, 'Device_shapeName'] = device_shape_links_df['deviceType'] + " " + device_shape_links_df['Device_Host_Name']
    device_shape_links_df['Device_shapeName'].fillna(device_shape_links_df['Device_Host_Name'], inplace=True)
    
    # add link shape name
    device_shape_links_df = dfop.merge_columns(device_shape_links_df, summary_column='Link_shapeName', 
                                          merge_columns=['Device_shapeName', 'switch_shapeName'], sep=' - ', 
                                          drop_merge_columns=False)
    
    device_shape_links_df['Device_shapeText'] = device_shape_links_df['Device_Host_Name']
    
    
    # add san graph details
    device_shape_links_df['switchClass_mode'] = device_shape_links_df['deviceType']
    device_shape_links_df = dfop.dataframe_fillna(device_shape_links_df, san_graph_details_df, join_lst=['switchClass_mode'], filled_lst=['switchClass_weight', 'master_shape', 'y_graph_level', 'x_group_offset'])
    device_shape_links_df.drop(columns='switchClass_mode', inplace=True)
    
    # sort by device
    # device_shape_links_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'deviceSubtype', 'Device_Host_Name', 'Fabric_label', 'switchName'], inplace=True, ignore_index=True)
    device_shape_links_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'deviceSubtype_mod', 'Device_shapeText', 'Fabric_label', 'switchName'], inplace=True, ignore_index=True)
    
    return device_shape_links_df


#############################################################################
# end





# allocate storage and lib
mask_storage_lib = connected_devices_df['deviceType'].isin(['STORAGE', 'LIB'])
storage_lib_df = connected_devices_df.loc[mask_storage_lib].copy()

# create storage and lib shape name
warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
mask_wwn_in_name = storage_lib_df['Device_Host_Name'].str.contains("([0-9a-f]{2}:){7}[0-9a-f]{2}")
storage_lib_df.loc[~mask_wwn_in_name, 'Device_Shape_Name'] = storage_lib_df['deviceType'] + " " + storage_lib_df['Device_Host_Name']
storage_lib_df['Device_Shape_Name'].fillna(storage_lib_df['Device_Host_Name'], inplace=True)

# # create speed link description
# storage_lib_link_df = storage_lib_df.groupby(by=['Fabric_name', 'Fabric_label', 'switchWwn', 'Device_Host_Name', 'speed', 'deviceType'], dropna=False)['Connected_portWwn'].agg('count').to_frame()
# storage_lib_link_df.reset_index(inplace=True)
# storage_lib_link_df.rename(columns={'Connected_portWwn': 'Link_quantity'}, inplace=True)
# storage_lib_link_df['Link_quantity'] = storage_lib_link_df['Link_quantity'].astype('str')
# # storage_lib_link_df['Link_description'] = storage_lib_link_df['Link_quantity'] + 'x' + storage_lib_link_df['speed']


# storage_lib_link_df = dfop.merge_columns(storage_lib_link_df, summary_column='Link_description', merge_columns=['Link_quantity', 'speed'], sep='x')
# # add 'x' to device XD links
# storage_lib_link_df.loc[storage_lib_link_df['Link_description'].str.contains('^\d+$'), 'Link_description'] = storage_lib_link_df['Link_description'] + 'x'

# # group device links to the same switch and links descriptions
# storage_lib_link_df = storage_lib_link_df.groupby(by=['Fabric_name', 'Fabric_label', 'switchWwn', 'Device_Host_Name', 'deviceType'])['Link_description'].agg(', '.join).to_frame()
# storage_lib_link_df.reset_index(inplace=True)
# # drop duplicated link so each row represents device connection to the switch
# storage_lib_df.drop(columns=['Connected_portWwn', 'Connected_portId', 'speed'], inplace=True)
# storage_lib_df.drop_duplicates(inplace=True, ignore_index=True)
# # add link description
# storage_lib_df = pd.merge(storage_lib_df, storage_lib_link_df, how='left', on=sw_device_connection_columns)



# add link shape name
storage_lib_df = dfop.merge_columns(storage_lib_df, summary_column='Link_shapeName', 
                                      merge_columns=['Device_Shape_Name', 'switch_shapeName'], sep=' - ', 
                                      drop_merge_columns=False)


storage_lib_df['Device_shapeName'] = storage_lib_df['Device_Shape_Name']
storage_lib_df['Device_shapeText'] = storage_lib_df['Device_Host_Name']


# add san graph details
storage_lib_df['switchClass_mode'] = storage_lib_df['deviceType']
storage_lib_df = dfop.dataframe_fillna(storage_lib_df, san_graph_details_df, join_lst=['switchClass_mode'], filled_lst=['switchClass_weight', 'master_shape', 'y_graph_level', 'x_group_offset'])
storage_lib_df.drop(columns='switchClass_mode', inplace=True)



 
# sort by device
storage_lib_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'deviceSubtype', 'Device_Host_Name', 'Fabric_label', 'switchName'], inplace=True, ignore_index=True)



####################### 


#############################
#  allocate srv and unknown
mask_srv_unknown = connected_devices_df['deviceType'].str.contains('SRV|UNKNOWN')
srv_unknown_df = connected_devices_df.loc[mask_srv_unknown].copy()

# extract Enclosure and slot
enclosure_slot_pattern = r'(Enclosure .+?) slot (\d+)'
if srv_unknown_df['Device_Location'].notna().any() and srv_unknown_df['Device_Location'].str.contains(enclosure_slot_pattern, na=False).any():
    
    srv_unknown_df[['Enclosure', 'slot']] = srv_unknown_df['Device_Location'].str.extract(enclosure_slot_pattern).values
    srv_unknown_df.loc[srv_unknown_df['Enclosure'].notna(), 'Enclosure'] = srv_unknown_df['Enclosure'] + ':'
    srv_unknown_df.loc[srv_unknown_df['slot'].notna(), 'slot_int'] = srv_unknown_df.loc[srv_unknown_df['slot'].notna(), 'slot'].astype('int')
else:
    srv_unknown_df[['Enclosure', 'slot', 'slot_int']] = (None, ) * 3

# create link description for each switch-device-speed
srv_unknown_link_df = srv_unknown_df.groupby(by=['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'speed', 'deviceType'])['Connected_portWwn'].agg('count').to_frame()
srv_unknown_link_df.reset_index(inplace=True)
srv_unknown_link_df.rename(columns={'Connected_portWwn': 'Link_quantity'}, inplace=True)
srv_unknown_link_df['Link_quantity'] = srv_unknown_link_df['Link_quantity'].astype('str')
# srv_unknown_link_df['Link_description'] = srv_unknown_link_df['Link_quantity'] + 'x' + srv_unknown_link_df['speed']

srv_unknown_link_df = dfop.merge_columns(srv_unknown_link_df, summary_column='Link_description', merge_columns=['Link_quantity', 'speed'], sep='x')
# add 'x' to device XD links
srv_unknown_link_df.loc[srv_unknown_link_df['Link_description'].str.contains('^\d+$'), 'Link_description'] = srv_unknown_link_df['Link_description'] + 'x'


# create link description for each switch-device 
srv_unknown_link_df = srv_unknown_link_df.groupby(by=['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'deviceType'])['Link_description'].agg(', '.join).to_frame()
srv_unknown_link_df.reset_index(inplace=True)
dfop.sort_cell_values(srv_unknown_link_df, 'Link_description')
# link description to create device groups with the same connections
srv_unknown_link_df['switchPair_id_str'] = srv_unknown_link_df['switchPair_id'].astype(int).astype(str)
srv_unknown_link_df['Link_description_sw_pair'] = srv_unknown_link_df['switchPair_id_str'] + srv_unknown_link_df['Fabric_label'] + ': ' + srv_unknown_link_df['Link_description']
srv_unknown_link_df.drop(columns='switchPair_id_str', inplace=True)
# drop duplicated link so each row represents device connection to the switch
srv_unknown_df.drop(columns=['Connected_portWwn', 'Connected_portId', 'speed'], inplace=True)
srv_unknown_df.drop_duplicates(inplace=True, ignore_index=True)
# add link description
sw_device_connection_columns = ['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'deviceType']
srv_unknown_df = pd.merge(srv_unknown_df, srv_unknown_link_df, how='left', on=sw_device_connection_columns)

# add slot to Device_Host_Name
srv_unknown_df = dfop.merge_columns(srv_unknown_df, summary_column='slot_Device_Host_Name', 
                                          merge_columns=['slot', 'Device_Host_Name'], sep='. ', 
                                          drop_merge_columns=False)

# join link_description for all device connections to find devices with the samelink_description in fabric_name
srv_unknown_df['Link_description_fabric_name'] = srv_unknown_df.groupby(by=['Fabric_name', 'deviceType', 'Device_Host_Name'])['Link_description_sw_pair'].transform('; '.join)
# sort Link_description_fabric_name
dfop.sort_cell_values(srv_unknown_df, 'Link_description_sw_pair', sep='; ')
# sort for future group in the correct order
srv_unknown_df.sort_values(by=['Fabric_name', 'deviceType', 'Enclosure', 'slot_int', 'Device_Host_Name', 'switchPair_id', 'Fabric_label'], inplace=True)


# numbering groups
srv_unknown_df['Device_fabric_group_number'] = srv_unknown_df.groupby(by=['Fabric_name', 'deviceType', 'Enclosure', 'Link_description_fabric_name'], dropna=False).ngroup()
# join device name based on group number
srv_unknown_df['Device_group_quantity'] = srv_unknown_df.groupby(by=['Fabric_name', 'Fabric_label', 'Device_fabric_group_number'])['slot_Device_Host_Name'].transform('count')
srv_unknown_df['Device_group_name'] = srv_unknown_df.groupby(by=['Fabric_name', 'Fabric_label', 'Device_fabric_group_number'])['slot_Device_Host_Name'].transform(', '.join)



def remove_name_duplicates(series):
    """Function to romove duplicated srv names from Device_group_name.
    Multiple names occur if server connected to multiple swithes in the same fabric_label"""
    
    server_lst = series['Device_group_name'].split(', ')
    unique_server_lst = list(dict.fromkeys(server_lst))
    
    if series['Device_group_name'] in ['SRV', 'UNKNOWN']:
        unique_server_lst = sorted(unique_server_lst)
    return ', '.join(unique_server_lst)

srv_unknown_df['Device_group_name'] = srv_unknown_df.apply(lambda series: remove_name_duplicates(series), axis=1)
    

    

srv_unknown_df['Device_Shape_Name'] = srv_unknown_df['deviceType'] + " GROUP #" + srv_unknown_df['Device_fabric_group_number'].astype(str)

# srv_unknown_df['Device_Shape_Name'] = srv_unknown_df.groupby(by=['Fabric_name', 'Fabric_label', 'Device_fabric_group_number'])['Device_Host_Name'].transform(', '.join) 


# add enclosure name
mask_enclosure = srv_unknown_df['Enclosure'].notna()
srv_unknown_df.loc[mask_enclosure, 'Device_group_name'] = srv_unknown_df['Enclosure'] + " " + srv_unknown_df['Device_group_name']
# srv_unknown_df.loc[mask_enclosure, 'Device_Shape_Name'] = srv_unknown_df['Enclosure'] + " " + srv_unknown_df['Device_Shape_Name']

# # add deviceClass 
# warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
# mask_wwn_in_name = srv_unknown_df['Device_Shape_Name'].str.contains("([0-9a-f]{2}:){7}[0-9a-f]{2}")
# mask_enclosure = srv_unknown_df['Enclosure'].isna()
# srv_unknown_df.loc[~mask_wwn_in_name & mask_enclosure, 'Device_Shape_Name'] = srv_unknown_df['deviceType'] + " " + srv_unknown_df['Device_Shape_Name']

# add link shape name
srv_unknown_df = dfop.merge_columns(srv_unknown_df, summary_column='Link_shapeName', 
                                      merge_columns=['Device_Shape_Name', 'switch_shapeName'], sep=' - ', 
                                      drop_merge_columns=False)

# group server dataframe
srv_unknown_grp_df = srv_unknown_df.drop(columns=['Device_Host_Name', 'Device_Location', 'slot_Device_Host_Name', 'slot', 'slot_int', 'deviceSubtype']).copy()
srv_unknown_grp_df.drop_duplicates(inplace=True, ignore_index=True)
srv_unknown_grp_df.sort_values(by=['Fabric_name',  'deviceType', 'Device_group_name', 'Fabric_label', 'switchPair_id'], ignore_index=True, inplace=True)


srv_unknown_grp_df['Device_shapeName'] = srv_unknown_grp_df['Device_Shape_Name']
srv_unknown_grp_df['Device_shapeText'] = srv_unknown_grp_df['Device_group_name']

# add san graph details
srv_unknown_grp_df['switchClass_mode'] = srv_unknown_grp_df['deviceType']
srv_unknown_grp_df['switchClass_mode'].replace({'UNKNOWN': 'UNKNOWN_DEV'}, inplace=True)
srv_unknown_grp_df = dfop.dataframe_fillna(srv_unknown_grp_df, san_graph_details_df, join_lst=['switchClass_mode'], filled_lst=['switchClass_weight', 'master_shape', 'y_graph_level', 'x_group_offset'])
srv_unknown_grp_df.drop(columns='switchClass_mode', inplace=True)



# create storage, library unique df to iterate
srv_unknown_grp_unique_df = srv_unknown_grp_df.drop_duplicates(subset=['Fabric_name', 'Device_group_name', 'deviceType']).copy()
srv_unknown_grp_unique_df = srv_unknown_grp_unique_df[['Fabric_name', 'Device_shapeText', 'Device_shapeName',  'deviceType', 'master_shape', 'y_graph_level', 'x_group_offset']]



##################################################################################################################################



srv_xkawako_df = srv_unknown_df.loc[srv_unknown_df['Device_Host_Name'] == 'xkawako'].copy()
srv_sbldb02_new = srv_unknown_df.loc[srv_unknown_df['Device_Host_Name'] == 'sbldb02_new'].copy()    

str_tst = "Enclosure 0000S2-BCHP-SB-03: 0000s2boxesx35.msk.mts.ru, 0000s2boxesx36.msk.mts.ru"
str_tst2 = re.sub(': |, ', '\n', str_tst)


srv_unknown_df.info()




# find switchPairs to which device is conneceted to
srv_unknown_df['switchPair_id_str'] = srv_unknown_df['switchPair_id'].astype('str')
srv_unknown_df['switchPair_id_fabric_join'] = srv_unknown_df.groupby(by=['Fabric_name', 'switchPair_id', 'Device_Host_Name', 'deviceType'])['Link_description'].transform('; '.join)


# create combined link description for all fabric_labels
srv_unknown_df['Link_descrition_fabric_join'] = srv_unknown_df.groupby(by=['Fabric_name', 'switchPair_id', 'Device_Host_Name', 'deviceType'])['Link_description'].transform('; '.join)



# add link shape name
storage_lib_df = dfop.merge_columns(storage_lib_df, summary_column='Link_shapeName', 
                                      merge_columns=['Device_Shape_Name', 'switch_shapeName'], sep=' - ', 
                                      drop_merge_columns=False)




srv_unknown_df.drop_duplicates(subset=['Fabric_name', 'Fabric_label', 'switchWwn', 'Device_Host_Name', 'deviceType'], inplace=True)


# count device link number in all fabric_labels
srv_unknown_df['Physical_link_quantity_str'] = srv_unknown_df['Physical_link_quantity'].astype('str')
srv_unknown_df['Device_link_quantity_in_fabric_labels'] = srv_unknown_df.groupby(by=['Fabric_name', 'switchPair_id', 'deviceType', 'Device_Host_Name', ])['Physical_link_quantity_str'].transform(', '.join)

srv_unknown_df

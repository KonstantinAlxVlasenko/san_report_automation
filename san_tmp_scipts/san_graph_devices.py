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
# script_dir = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop


data_names = ['portshow_aggregated', 'npv_ag_connected_devices', 'fcr_proxydev']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)


# data_names = ['fcrxlateconfig']

portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df, *_ = data_lst
# fcrxlateconfig_df, *_ = data_lst


SW_PAIR_NUMBER_PER_FABRIC_THRESHOLD = 8
DEVICE_FABRIC_NAME_TAG = '_dev'
STORAGE_NUMBER_PER_FABRIC_THRESHOLD = 15


pattern_dct = {'native_speed': r'Native_(N?\d+G?)', 'speed': r'^(N?\d+G?)$', 'ls_mode': r'Long_Distance_(\w+)', 
               'distance': r'distance += +(\d+ (K|k)m)', 'wwn': '([0-9a-f]{2}:){7}[0-9a-f]{2}', 
               'link_quantity': '(?:ISL|IFL|Link)_\d+', 'enclosure_slot': r'(Enclosure .+?) slot (\d+)'}






def filter_edge_devices(portshow_aggregated_df):
    """Function to filter all devices from portshow_aggregated_df"""
    
    mask_portwwn_notna = portshow_aggregated_df['Connected_portWwn'].notna()
    mask_device_lib_srv_stor = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    connected_devices_df = portshow_aggregated_df.loc[mask_portwwn_notna & mask_device_lib_srv_stor].copy()
    return connected_devices_df



def tag_npiv_devices(connected_devices_df):
    """Function to tag NPIV devices"""

    mask_npiv = connected_devices_df['Device_type'].str.contains('NPIV', na=False)
    connected_devices_df.loc[mask_npiv, 'port_NPIV'] = 'NPIV'
    return connected_devices_df






def specify_npiv_port_speed(npv_ag_connected_devices_df, portshow_aggregated_df):
    """Function to specify npiv port connection speed in npv_ag_connected_devices_df.
    Speed value from supportsave of AG switch has priority over speed in npv_ag_connected_devices_df.
    Speed value copied from AG mode switches in portshow_aggregated_df. Then empty speed values
    filled from original npv_ag_connected_devices_df"""

    # filter ports behind confirmed AG switches
    mask_ag = portshow_aggregated_df['switchMode'].str.contains('Gateway', na=False)
    portshow_ag_df = portshow_aggregated_df.loc[mask_ag]
    # copy and clear speed column
    npv_ag_connected_devices_cp_df = npv_ag_connected_devices_df.copy()
    npv_ag_connected_devices_cp_df['speed_cp'] = npv_ag_connected_devices_cp_df['speed']
    npv_ag_connected_devices_cp_df['speed'] = None
    # fill speed from ssave of AG switches
    npv_ag_connected_devices_cp_df = dfop.dataframe_fillna(npv_ag_connected_devices_cp_df, portshow_ag_df, join_lst=['Fabric_name', 'Fabric_label', 'Connected_portWwn'], filled_lst=['speed'])
    # fill empty values from orignal speed column
    npv_ag_connected_devices_cp_df['speed'].fillna(npv_ag_connected_devices_cp_df['speed_cp'], inplace=True)
    npv_ag_connected_devices_cp_df.drop(columns='speed_cp', inplace=True)
    return npv_ag_connected_devices_cp_df





def remove_wwns_behind_ag_npv_sw(connected_devices_df, npv_ag_connected_devices_cp_df):
    """Function to drop wwns connected to confirmed AG and NPV switches from connected_devices_df
    to avoid wwns duplication after dataframe concatenation. Returnd connected_devices_df
    without wwns directly connected to confirmed AG and NPV switches"""
    
    # tag wwns connected to confirmed AG and NPV switches in connected_devices_df
    wwn_behind_ag_df = npv_ag_connected_devices_cp_df[['Fabric_name', 'Fabric_label', 'Connected_portWwn']].drop_duplicates()
    connected_devices_df = pd.merge(connected_devices_df, wwn_behind_ag_df, how='left', indicator='Exist')
    # drop rows with wwns connected to confirmed AG and NPV switches in connected_devices_df
    mask_unique_row = connected_devices_df['Exist'] == 'left_only'
    connected_devices_df = connected_devices_df.loc[mask_unique_row].copy()
    connected_devices_df.drop(columns='Exist', inplace=True)
    return connected_devices_df





def align_device_type(connected_devices_df):
    """Function to modify 'deviceSubtype' column in connected_devices_df.
    This column might contain different values for the same device (LIB DRV, LIB CTRL) and
    this column is used for switch -> device_name grouping to present unique devices as single row.
    To aviod single switch -> device_name shown in multiple rows column 'deviceSubtype' must be
    aligned for libraries and servers."""
    
    connected_devices_df['deviceSubtype_mod'] = connected_devices_df['deviceSubtype']
    connected_devices_df.loc[connected_devices_df['deviceType'].str.contains('SRV', na=False), 'deviceSubtype_mod'] = 'SRV'
    connected_devices_df.loc[connected_devices_df['deviceType'].str.contains('LIB', na=False), 'deviceSubtype_mod'] = 'LIB'
    connected_devices_df.drop(columns=['deviceSubtype'], inplace=True)
    return connected_devices_df




def extract_enclosure_slot(connected_devices_df, pattern_dct):
    """Function to extract enclosure and slot information from 'Device_Location' column.
    Slot is convetred to int"""
    
    # enclosure_slot_pattern = r'(Enclosure .+?) slot (\d+)'
    if connected_devices_df['Device_Location'].notna().any() and connected_devices_df['Device_Location'].str.contains(pattern_dct['enclosure_slot'], na=False).any():
        connected_devices_df[['Enclosure', 'slot']] = connected_devices_df['Device_Location'].str.extract(pattern_dct['enclosure_slot']).values
        # add ':' to 'Enclosure' and enclosure name
        connected_devices_df.loc[connected_devices_df['Enclosure'].notna(), 'Enclosure'] = connected_devices_df['Enclosure'] + ':'
        # convert slot number to int
        connected_devices_df.loc[connected_devices_df['slot'].notna(), 'slot_int'] = connected_devices_df.loc[connected_devices_df['slot'].notna(), 'slot'].astype('int')
    else:
        connected_devices_df[['Enclosure', 'slot', 'slot_int']] = (None, ) * 3
    return connected_devices_df



def create_sw_device_link_speed_description(connected_devices_df):
    """Function to create speed link description for switch -> device_name level  (2xN16, 2x16G)"""

    # count values in 'speed' columns for fabric_name -> fabric_label -> switch -> device_name level
    link_description_speed_df = count_links_with_values_in_column(connected_devices_df, 'speed')
    # create link description for each switch->device 
    # join and port speed for switch->device (device ports connected to the same switch might have different speed )
    link_description_speed_df = link_description_speed_df.groupby(by=['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'deviceType'])['Link_description_speed'].agg(', '.join).to_frame()
    link_description_speed_df.reset_index(inplace=True)
    dfop.sort_cell_values(link_description_speed_df, 'Link_description_speed')
    return link_description_speed_df


def create_sw_device_link_npiv_description(connected_devices_df):
    """Function to create NPIV link description for switch -> device_name level  (2xNPIV)"""
    
    # count values in port_NPIV' columns for fabric_name -> fabric_label -> switch -> device_name level
    link_description_npiv_df = count_links_with_values_in_column(connected_devices_df, 'port_NPIV')    
    # drop empty npiv rows to avoid duplication if device have normal and npiv connections
    link_description_npiv_df.dropna(subset='Link_description_port_NPIV', inplace=True)
    return link_description_npiv_df
       



def create_sw_device_link_description(connected_devices_df):
    """Function to create speed and NPIV link description for switch -> device_name level
    with switchPair_id and fabric_label. Each row is inique switch -> device_name level coneection
    with 'Link_description' presented as combination of 'switchPair_id', 'Fabric_label', 
    'Link_description_speed', 'Link_description_port_NPIV'  ('2A: 2xN16, 2x16G, 2xNPIV')"""


    # create speed link description for switch -> device_name level  (2xN16, 2x16G)
    link_description_speed_df = create_sw_device_link_speed_description(connected_devices_df)
    # create npiv link description for switch -> device_name level (2xNPIV)
    link_description_npiv_df = create_sw_device_link_npiv_description(connected_devices_df)

    # join speed and npiv details
    device_links_df = pd.merge(link_description_speed_df, link_description_npiv_df, how='left', 
                               on=['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'deviceType'])
    # create Link description column as combination of speed and npiv details
    device_links_df = dfop.merge_columns(device_links_df, summary_column='Link_description', 
                                         merge_columns=['Link_description_speed', 'Link_description_port_NPIV'], sep=', ')
    # link description as combination of switchPair_id, fabric_label and link description
    device_links_df['switchPair_id_str'] = device_links_df['switchPair_id'].astype(int).astype(str)
    device_links_df['Link_description_sw_pair_level'] = \
        device_links_df['switchPair_id_str'] + device_links_df['Fabric_label'] + ': ' + device_links_df['Link_description']
    device_links_df.drop(columns='switchPair_id_str', inplace=True)
    return device_links_df




def create_fabric_swpair_device_link_description(connected_devices_df, device_links_df):
    """Function to create  switch -> device_name connection dataframe with link description on fabric_name level 
    (all switch pairs in faric_name). Dataframe connected_devices_df presented on switch -> device_name level 
    (drop duplicated rows with multiple ports of the same device connected to the same switch).
    Then link description from device_links_df is added to connected_devices_df and all links description 
    from the single device_name in the fabric_name are combined but each row still presents switch -> device_name connection.
    Result each switch -> device_name connection has link description of all links to this device_name in the fabric_name
    ('2A: 2xN16, 2x16G, 2xNPIV', '2B: 2xN16, 2x16G, 2xNPIV', '3A: 2xN16', '3B: 2x16G')."""

    # drop duplicated link so each row represents device connection to the switch
    connected_devices_df.drop(columns=['Connected_portWwn', 'Connected_portId', 'speed', 'port_NPIV'], inplace=True)
    connected_devices_df.drop_duplicates(inplace=True, ignore_index=True)
    # add link description ('2A: 2xN16, 2x16G, 2xNPIV')
    sw_device_connection_columns = ['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'deviceType']
    connected_devices_df = pd.merge(connected_devices_df, device_links_df, how='left', on=sw_device_connection_columns)
    
    # join link_description for all device connections to find devices with the samelink_description in the fabric_name
    connected_devices_df['Link_description_fabric_name_level'] = connected_devices_df.groupby(
        by=['Fabric_name', 'deviceType', 'Device_Host_Name'])['Link_description_sw_pair_level'].transform('; '.join)
    # sort values in Link_description_fabric_name string
    dfop.sort_cell_values(connected_devices_df, 'Link_description_fabric_name_level', sep='; ')
    # sort for future group in the correct order
    connected_devices_df.sort_values(
        by=['Fabric_name', 'deviceType', 'Enclosure', 'slot_int', 'Device_Host_Name', 'switchPair_id', 'Fabric_label'], inplace=True)
    return connected_devices_df



def count_fabric_storage(connected_devices_df):
    """Function to count storages and libraries number in the fabric_name.
    Returns connected_devices_df with 'Storage_lib_number_in_fabric' column."""

    # check if storage need to be grouped
    mask_storage_lib = connected_devices_df['deviceType'].isin(['STORAGE', 'LIB'])
    connected_storages_df = connected_devices_df.loc[mask_storage_lib].copy()
    storage_number_df = connected_storages_df.groupby(
        by=['Fabric_name'])['Device_Host_Name'].nunique().to_frame(name='Storage_lib_number_in_fabric')
    connected_devices_df = pd.merge(connected_devices_df, storage_number_df, how='left', on='Fabric_name')
    connected_devices_df['Storage_lib_number_in_fabric'].fillna(0, inplace=True)
    return connected_devices_df




def create_storage_shape_links(connected_devices_df, pattern_dct):
    """Function to create switch master shape -> storage and library master shape link details.
    If number of storages and libraries in the fabric exceeds STORAGE_NUMBER_PER_FABRIC_THRESHOLD
    then to avoid graph overload storages are grouped based on Link_description so storages connected
    to the same pair(s) nof switches with same link speeds, npiv modes and link number number presented
    with single master shape"""
    
    # storages links
    mask_storage_lib = connected_devices_df['deviceType'].isin(['STORAGE', 'LIB'])
    mask_threshold_exceeded = connected_devices_df['Storage_lib_number_in_fabric'] > STORAGE_NUMBER_PER_FABRIC_THRESHOLD
    
    storage_under_threshold_df = connected_devices_df.loc[mask_storage_lib & ~mask_threshold_exceeded].copy()
    storage_above_threshold_df = connected_devices_df.loc[mask_storage_lib & mask_threshold_exceeded].copy()
    storage_shape_links_df = pd.concat([group_device_on_name(storage_under_threshold_df, san_graph_details_df, pattern_dct), 
                                        group_device_on_link_description(storage_above_threshold_df, san_graph_details_df)])
    return storage_shape_links_df


def create_server_shape_links(connected_devices_df):
    """Function to create switch master shape -> server and unknown master shape link details.
    To avoid graph overload servers are grouped based on Link_description and enclosure so servers connected
    to the same pair(s) of switches with same link speeds, npiv modes, enclosure and link number number presented
    with single master shape"""

    # server and unknown links
    mask_srv_unknown = connected_devices_df['deviceType'].str.contains('SRV|UNKNOWN')
    server_df = connected_devices_df.loc[mask_srv_unknown].copy()
    server_shape_links_df = group_device_on_link_description(server_df, san_graph_details_df)
    return server_shape_links_df


def count_links_with_values_in_column(connected_devices_df, count_column):
    """Function to count values in count_column column for
    fabric_name -> fabric_label -> switch -> device_name level.
    Result column presented as string 'number X value'. Each unique value for the 
    switch -> device connection is on separate rows (2XN16 and 4X16G)."""
    
    # result column name
    link_description_column = 'Link_description_' + count_column
    # count links number with identical parameters (values) for each switch -> device connection
    group_columns = ['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', count_column, 'deviceType']
    device_links_df = connected_devices_df.groupby(by=group_columns, dropna=False)['Connected_portWwn'].agg('count').to_frame()
    device_links_df.reset_index(inplace=True)
    device_links_df.rename(columns={'Connected_portWwn': 'Link_quantity'}, inplace=True)
    
    # present result link_description_column as string 'counted number X value'
    device_links_df['Link_quantity'] = device_links_df['Link_quantity'].astype('str')
    device_links_df = dfop.merge_columns(device_links_df, summary_column=link_description_column, merge_columns=['Link_quantity', count_column], sep='x')
    # add 'x' to device XD links (separator 'x' on prev step was not added for XD links)
    device_links_df.loc[device_links_df[link_description_column].str.contains('^\d+$'), link_description_column] = device_links_df[link_description_column] + 'x'
    # for NPIV link_description_column clean link description if theris no npiv
    if 'npiv' in count_column.lower():
        device_links_df.loc[~device_links_df[link_description_column].str.contains('NPIV', na=False), link_description_column] = None
    return device_links_df



def create_device_fabric_name(connected_devices_df, san_graph_sw_pair_df):
    """Function to verify if Visio page with pure san graph without edge devices will present in Visio document.
    If number of switch pairs in the fabric ecxceeds SW_PAIR_NUMBER_PER_FABRIC_THRESHOLD then devices from it 
    are moved to the fabric with the tag DEVICE_FABRIC_NAME_TAG.
    Retturns connected_devices_df with updated Fabric_name and san_graph_sw_pair_df with added device fabrics
    to perform grouping in Visio page of switch pairs master shapes"""

    # find fabric_names where edge devices are connected
    fabric_name_with_dev = connected_devices_df['Fabric_name'].unique()
    # count switch_pairs in each fabric
    fabric_name_sw_pair_count_df = san_graph_sw_pair_df.groupby(by=['Fabric_name'])['switchName_Wwn'].count().to_frame().reset_index()
    fabric_name_sw_pair_count_df.rename(columns={'switchName_Wwn': 'switchPair_quantity'}, inplace=True)
    # find fabric_names with devices with and where switch_pair number exceeds threshold
    # these fabrics need to be copied
    mask_fabric_with_dev = fabric_name_sw_pair_count_df['Fabric_name'].isin(fabric_name_with_dev)
    mask_sw_pair_threshold = fabric_name_sw_pair_count_df['switchPair_quantity'] > SW_PAIR_NUMBER_PER_FABRIC_THRESHOLD
    fabric_name_duplicated_lst = fabric_name_sw_pair_count_df.loc[mask_fabric_with_dev & mask_sw_pair_threshold, 'Fabric_name'].to_list()
    # create names for device fabrics if they need to be copied
    fabric_name_dev_lst = [fabric_name + DEVICE_FABRIC_NAME_TAG for fabric_name in fabric_name_duplicated_lst]
    # rename fabric_name in connecetd_devices_df
    connected_devices_df['Fabric_name_cp'] = connected_devices_df['Fabric_name']
    connected_devices_df['Fabric_name'].replace(to_replace=fabric_name_duplicated_lst, value=fabric_name_dev_lst, inplace=True)
    
    # update san_graph_sw_pair
    # filter fabirc_names which are need to be copied
    san_graph_sw_pair_duplicated_df = san_graph_sw_pair_df.loc[san_graph_sw_pair_df['Fabric_name'].isin(fabric_name_duplicated_lst)].copy()
    # change fabric_names to the new names
    san_graph_sw_pair_duplicated_df['Fabric_name'].replace(to_replace=fabric_name_duplicated_lst, value=fabric_name_dev_lst, inplace=True)
    # add renamed fabic_names to original one to use later to group master shapes in pure switches and switches + edge devices graphs
    san_graph_sw_pair_group_df = pd.concat([san_graph_sw_pair_df, san_graph_sw_pair_duplicated_df])
    return connected_devices_df, san_graph_sw_pair_group_df, fabric_name_duplicated_lst, fabric_name_dev_lst


####
# create fn for devices groupes on Link_description_fabric_name

def group_device_on_link_description(connected_devices_df, san_graph_details_df):
    """Function to find unique rows on fabic_name - fabric_label - switch - device_group level level ('Link_description_fabric_name_level')
    and fill with visio shape details for each row (switch - device group connection in the fabric) .
    Returns DataFrame with switch shape -> device group shape links details"""    

    if connected_devices_df.empty:
        return pd.DataFrame()

    # add slot to Device_Host_Name (1. srv_blade #1)
    connected_devices_df = dfop.merge_columns(connected_devices_df, summary_column='Device_Host_Name_slot', 
                                                                      merge_columns=['slot', 'Device_Host_Name'], sep='. ', 
                                                                         drop_merge_columns=False)
    # numbering goups within the same fabric_name for the same devce_class and modified device_type 
    # witin the same enclosure (for srv) with identical 'Link_description_fabric_name_level'
    connected_devices_df['Device_Group_number'] = connected_devices_df.groupby(
        by=['Fabric_name', 'deviceType', 'deviceSubtype_mod', 'Enclosure', 'Link_description_fabric_name_level'], 
        dropna=False).ngroup().add(1)
    # count device number within the fabric_name device group
    connected_devices_df['Device_quantity_in_group'] = connected_devices_df.groupby(
        by=['Fabric_name', 'Device_Group_number'])['Device_Host_Name_slot'].transform('nunique')
    # device group visio shape_text is the list of device names with the slot number (slot in case of srv) included in the group
    connected_devices_df['Device_shapeText'] = connected_devices_df.groupby(
        by=['Fabric_name', 'Fabric_label', 'Device_Group_number'])['Device_Host_Name_slot'].transform(', '.join)
    # remove duplicate device names if device is connected to multiple switches in fabric label and keep order for blade servers and sort for others
    connected_devices_df['Device_shapeText'] = connected_devices_df.apply(lambda series: remove_name_duplicates(series), axis=1)
    # shape name for device groups consists of deviceClass to device group number
    connected_devices_df['Device_shapeName'] = connected_devices_df['deviceType'] + " GROUP #" + connected_devices_df['Device_Group_number'].astype(str)
    # in case of blade systems add enclosure name to the device group visio shape_text
    mask_enclosure = connected_devices_df['Enclosure'].notna()
    connected_devices_df.loc[mask_enclosure, 'Device_shapeText'] = connected_devices_df['Enclosure'] + " " + connected_devices_df['Device_shapeText']
    # add link shape name as combination of device group and connected switch shape names
    connected_devices_df = dfop.merge_columns(connected_devices_df, summary_column='Link_shapeName', 
                                          merge_columns=['Device_shapeName', 'switch_shapeName'], sep=' - ', 
                                          drop_merge_columns=False)
    # drop columns containing specific device information
    device_shape_links_df = connected_devices_df.drop(columns=['Device_Host_Name', 'Device_Location', 'Device_Host_Name_slot', 'slot', 'slot_int']).copy()
    # keep unique rows on fabric_name -> fabric_label -> switch -> device_group level
    device_shape_links_df.drop_duplicates(inplace=True, ignore_index=True)
    # device_shape_links_df.sort_values(by=['Fabric_name',  'deviceType', 'Device_shapeText', 'Fabric_label', 'switchPair_id'], ignore_index=True, inplace=True)
    # add visio master shape details for device group device class
    device_shape_links_df = add_device_master_shape_details(device_shape_links_df, san_graph_details_df)
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
def group_device_on_name(connected_devices_df, san_graph_details_df, pattern_dct):
    """Function to find unique rows on fabic_name - fabric_label - switch - device_name level 
    and fill with visio shape details for each row (switch - device connection in the fabric) .
    Returns DataFrame with switch shape -> device name shape links details"""

    if connected_devices_df.empty:
        return pd.DataFrame()    

    # some porst of the same device connected to the same switch might be in NPIV mode while others are in regular mode
    # link descripton already contains all ports (both regular and NPIV)
    # 'deviceSubtype_mod' contains modified and aligned device types (LIB CTRL and LIB DRV -> LIB; QLOGIC, EMULEX etc -> SRV)
    # to drop fabic_name - fabric_label - switch - device_name duplicated rows columns 'deviceSubtype', 'port_NPIV' must be dropped
    # device_shape_links_df = connected_devices_df.drop(columns=['deviceSubtype', 'port_NPIV']).copy()
    device_shape_links_df = connected_devices_df.copy()
    device_shape_links_df.drop_duplicates(inplace=True, ignore_index=True)
    
    # create device shape name
    # wwns for which device_hostname was not found in NS and no alias applied in zoning named as device_type + wwn already
    # to avoid wwn duplication in shape name for these kind of devices filter applied 
    warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
    # mask_wwn_in_name = device_shape_links_df['Device_Host_Name'].str.contains("([0-9a-f]{2}:){7}[0-9a-f]{2}", na=False)
    mask_wwn_in_name = device_shape_links_df['Device_Host_Name'].str.contains(pattern_dct['wwn'], na=False)
    # shape name for devices with a known name (device_name without wwn)
    device_shape_links_df.loc[~mask_wwn_in_name, 'Device_shapeName'] = device_shape_links_df['deviceType'] + " " + device_shape_links_df['Device_Host_Name']
    # shape name for devices with a unknown name (device_name with wwn)
    device_shape_links_df['Device_shapeName'].fillna(device_shape_links_df['Device_Host_Name'], inplace=True)
    
    # add link shape name (shape_name + connected switch shape_name)
    device_shape_links_df = dfop.merge_columns(device_shape_links_df, summary_column='Link_shapeName', 
                                          merge_columns=['Device_shapeName', 'switch_shapeName'], sep=' - ', 
                                          drop_merge_columns=False)
    # device visio shape_text is device_name
    device_shape_links_df['Device_shapeText'] = device_shape_links_df['Device_Host_Name']
    # add visio master shape details for device name device class
    device_shape_links_df = add_device_master_shape_details(device_shape_links_df, san_graph_details_df)
    return device_shape_links_df


def add_device_master_shape_details(device_shape_links_df, san_graph_details_df):
    """Function to add master shape details in device_shape_links_df to create Visio draw"""
    
    # add san graph details (device master shape details for visio draw)
    # 'switchClass_mode' is device tag based on which master shape is assigned
    device_shape_links_df['switchClass_mode'] = device_shape_links_df['deviceType']
    device_shape_links_df['switchClass_mode'].replace({'UNKNOWN': 'UNKNOWN_DEV'}, inplace=True)
    device_shape_links_df = dfop.dataframe_fillna(device_shape_links_df, san_graph_details_df, 
                                                  join_lst=['switchClass_mode'], 
                                                  filled_lst=['switchClass_weight', 'master_shape', 'y_graph_level', 'x_group_offset'])
    device_shape_links_df.drop(columns='switchClass_mode', inplace=True)
    # sort by device
    device_shape_links_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'deviceSubtype_mod', 'Device_shapeText', 'Fabric_label', 'switchName'], 
                                      inplace=True, ignore_index=True)
    return device_shape_links_df



def find_connected_devices(portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df):
    """Function to find unique device ports in each fabric_name as combination of device ports connected
    to the switches, device ports behind VC, NPV, AG mode switches and devices connected to translate domains
    in case of fabric routing and LSAN zones. Returns dataframe witn unique switch -> device wwn rows"""

    # filter all connected devices in fabrics
    connected_devices_df = filter_edge_devices(portshow_aggregated_df)
    # tag NPIV devices with NPIV tag
    connected_devices_df  = tag_npiv_devices(connected_devices_df)
    # align connected_devices_df columns with npv_ag_connected_devices_df columns
    dev_columns = [*npv_ag_connected_devices_df.columns, 'port_NPIV']
    connected_devices_df = connected_devices_df[dev_columns].copy()
    # fill speed from access gateway ssave in npv_ag_connected_devices_df
    npv_ag_connected_devices_cp_df = specify_npiv_port_speed(npv_ag_connected_devices_df, portshow_aggregated_df)
    # remove devices from connected_devices_df if they exist in npv_ag_connected_devices_df
    connected_devices_df = remove_wwns_behind_ag_npv_sw(connected_devices_df, npv_ag_connected_devices_cp_df)
    
    # wwns connected to translate domains
    # align fcr_xd_proxydev_df columns
    fcr_xd_proxydev_cp_df = fcr_xd_proxydev_df.reindex(columns=dev_columns).copy()
    # translate domain device connection speed is na
    fcr_xd_proxydev_cp_df['speed'] = None
    
    # concatenate dataframes wwns connected to all switches except confirmed AG and NPV switches,
    # wwns connected to confirmed AG and NPV switches and wwns connected to translate domains
    connected_devices_df = pd.concat([connected_devices_df, npv_ag_connected_devices_cp_df, fcr_xd_proxydev_cp_df])
    connected_devices_df.drop_duplicates(subset=['Fabric_name', 'Fabric_label', 'switchWwn', 'Connected_portWwn'], inplace=True, ignore_index=True)
    return connected_devices_df



def create_device_link_description(connected_devices_df, switch_pair_df, pattern_dct):
    """Funtion to create link description for switch -> device_name rows.
    Each switch -> device_name row contains link description for fabric_name level
    (all switch pairs -> device name connection in the fabric_name)"""
    
    # count links number of each switch -> device_name connection
    connected_devices_df['Physical_link_quantity'] = connected_devices_df.groupby(
        by=['Fabric_name', 'Fabric_label', 'switchWwn', 'Device_Host_Name', 'deviceType'])['Connected_portWwn'].transform('count') # link_quantity
    # create switch shape name
    create_shape_name(connected_devices_df, "switchName", "switchWwn", "switch_shapeName")
    # add switchPair_Id
    connected_devices_df = dfop.dataframe_fillna(connected_devices_df, switch_pair_df, join_lst=['switchWwn'], filled_lst=['switchPair_id'])
    
    # align subtype for srv and lib
    align_device_type(connected_devices_df)
    
    # extract Enclosure and slot
    connected_devices_df = extract_enclosure_slot(connected_devices_df, pattern_dct)
    # speed and NPIV link description for switch -> device_name level with switchPair_id and fabric_label ('2A: 2xN16, 2x16G, 2xNPIV')
    device_links_df = create_sw_device_link_description(connected_devices_df)
    
    # speed and NPIV link description for switch -> device_name connection in fabric_name level 
    # ('2A: 2xN16, 2x16G, 2xNPIV', '2B: 2xN16, 2x16G, 2xNPIV', '3A: 2xN16', '3B: 2x16G')
    connected_devices_df = create_fabric_swpair_device_link_description(connected_devices_df, device_links_df)
    return connected_devices_df



def create_device_shape_links(connected_devices_df, san_graph_sw_pair_df, pattern_dct):
    """Function to create device shapes and link shapes to the switch shapes.
    Devices shapes for severs and storages if number of storages in the fabric_name
    exceeds STORAGE_NUMBER_PER_FABRIC_THRESHOLD are group of devices with the same
    link description. Resturn dataframe with device shapes and its details
    to build Visio graph"""

    # count storages and libraries number in the fabric_name
    connected_devices_df = count_fabric_storage(connected_devices_df)
    # separate pure switches graph and switches + edge devices graph
    connected_devices_df, san_graph_sw_pair_group_df, fabric_name_duplicated_lst, fabric_name_dev_lst = \
        create_device_fabric_name(connected_devices_df, san_graph_sw_pair_df)
    # create storages and servers visio master shapes details  
    storage_shape_links_df = create_storage_shape_links(connected_devices_df, pattern_dct)
    server_shape_links_df = create_server_shape_links(connected_devices_df)
    return connected_devices_df, storage_shape_links_df, server_shape_links_df, san_graph_sw_pair_group_df, fabric_name_duplicated_lst, fabric_name_dev_lst


# find unique device ports in each fabric_name
connected_devices_df = find_connected_devices(portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df)
# create link description for switch -> device_name rows on fabric_name level
connected_devices_df = create_device_link_description(connected_devices_df, switch_pair_df, pattern_dct)
# create device shapes and link shapes to the switch shapes
connected_devices_df, storage_shape_links_df, server_shape_links_df, san_graph_sw_pair_group_df, fabric_name_duplicated_lst, fabric_name_dev_lst = \
    create_device_shape_links(connected_devices_df, san_graph_sw_pair_df, pattern_dct)

#############################################################################
# end





# connected_devices_df_b = connected_devices_df.copy()
# storage_shape_links_df_b = storage_shape_links_df.copy()
# server_shape_links_df_b = server_shape_links_df.copy()
# san_graph_sw_pair_group_df_b = san_graph_sw_pair_group_df.copy()




# data_before_lst = [connected_devices_df_b, storage_shape_links_df_b, server_shape_links_df_b, san_graph_sw_pair_group_df_b]

# data_after_lst = [connected_devices_df, storage_shape_links_df, server_shape_links_df, san_graph_sw_pair_group_df]

# def find_differences(data_before_lst, data_after_lst, data_names):

#     for df_name, before_df, after_df in zip(data_names, data_before_lst, data_after_lst):
#         df_equality = after_df.equals(before_df)
#         print(f"\n{df_name} equals {df_equality}")
#         if not df_equality:
#             print("   column names are equal: ", before_df.columns.equals(after_df.columns))
#             print("      Unmatched columns:")
#             for column in before_df.columns:
#                 if not before_df[column].equals(after_df[column]):
#                     print("        ", column)  

# find_differences(data_before_lst, data_after_lst, ['connected_devices_df', 'storage_shape_links_df', 'server_shape_links_df', 'san_graph_sw_pair_group_df'])


"""Auxiliary module to group edge devices based on link description (multiple devices -> single shape) or 
devece name (single device -> single shape"""

import warnings

import pandas as pd

import utilities.dataframe_operations as dfop


def group_device_on_link_description(connected_devices_df, san_graph_grid_df):
    """Function to find unique rows on 
    fabic_name - fabric_label - switch - device_group level level ('Link_description_fabric_name_level')
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
    device_shape_links_df = add_device_master_shape_details(device_shape_links_df, san_graph_grid_df)
    return device_shape_links_df


def remove_name_duplicates(series):
    """Function to remove duplicated device names from Device_shapeText.
    Multiple names occur if server connected to multiple swithes in the same fabric_label"""
    
    device_lst = series['Device_shapeText'].split(', ')
    unique_device_lst = list(dict.fromkeys(device_lst))
    
    if series['deviceType'] in ['SRV', 'UNKNOWN', 'STORAGE', 'LIB']:
        unique_device_lst = sorted(unique_device_lst)
    return ', '.join(unique_device_lst)


def group_device_on_name(connected_devices_df, san_graph_grid_df, pattern_dct):
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
    device_shape_links_df = add_device_master_shape_details(device_shape_links_df, san_graph_grid_df)
    return device_shape_links_df


def add_device_master_shape_details(device_shape_links_df, san_graph_grid_df):
    """Function to add master shape details in device_shape_links_df to create Visio draw"""
    
    # add san graph details (device master shape details for visio draw)
    # 'switchClass_mode' is device tag based on which master shape is assigned
    device_shape_links_df['switchClass_mode'] = device_shape_links_df['deviceType']
    device_shape_links_df['switchClass_mode'].replace({'UNKNOWN': 'UNKNOWN_DEV'}, inplace=True)
    device_shape_links_df = dfop.dataframe_fillna(device_shape_links_df, san_graph_grid_df, 
                                                  join_lst=['switchClass_mode'], 
                                                  filled_lst=['switchClass_weight', 'master_shape', 'y_graph_level', 'x_group_offset'])
    device_shape_links_df.drop(columns='switchClass_mode', inplace=True)
    # sort by device
    device_shape_links_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'deviceSubtype_mod', 'Device_shapeText', 'Fabric_label', 'switchName'], 
                                      inplace=True, ignore_index=True)
    return device_shape_links_df
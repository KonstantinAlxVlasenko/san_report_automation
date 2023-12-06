
"""Module to create link description (speed, quantity, npiv) for switch -> device_name connection on fabric_name level"""

import pandas as pd

import utilities.dataframe_operations as dfop

from ..shape_details import create_shape_name

CLEAR_NPIV_LINKS = True


def create_device_link_description(connected_devices_df, switch_pair_df, pattern_dct):
    """Funtion to create link description for switch -> device_name rows.
    Each switch -> device_name row contains link description for fabric_name level
    (all switch pairs -> device name connection in the fabric_name)"""
    
    
    connected_devices_df['Connected_portWwn_npiv_free'] = connected_devices_df['Connected_portWwn']
    if CLEAR_NPIV_LINKS and connected_devices_df['port_NPIV'].notna().any():
        mask_npiv = connected_devices_df['port_NPIV'].str.lower().str.contains('npiv', na=False)
        phys_link_count_column = 'Connected_portWwn_npiv_free'
        connected_devices_df.loc[mask_npiv, 'Connected_portWwn_npiv_free'] = None
    else:
        phys_link_count_column = 'Connected_portWwn'

    # count links number of each switch -> device_name connection
    connected_devices_df['Physical_link_quantity'] = connected_devices_df.groupby(
        by=['Fabric_name', 'Fabric_label', 'switchWwn', 
            'Device_Host_Name', 'deviceType'])[phys_link_count_column].transform('count') # link_quantity
    
    # connected_devices_df.drop(columns='Connected_portWwn_npiv_free', inplace=True)
    
    
    # create switch shape name
    create_shape_name(connected_devices_df, "switchName", "switchWwn", "switch_shapeName")
    # add switchPair_Id
    connected_devices_df = dfop.dataframe_fillna(connected_devices_df, switch_pair_df, join_lst=['switchWwn'], filled_lst=['switchPair_id'])
    
    # align subtype for srv and lib
    align_device_type(connected_devices_df)
    
    # extract Enclosure and slot
    connected_devices_df = extract_enclosure_slot(connected_devices_df, pattern_dct)
    # speed and NPIV link description for switch -> device_name level with switchPair_id and fabric_label ('2A: 2xN16, 2x16G, 2xNPIV')
    device_links_df = create_sw_device_link_description(connected_devices_df, phys_link_count_column)
    
    # speed and NPIV link description for switch -> device_name connection in fabric_name level 
    # ('2A: 2xN16, 2x16G, 2xNPIV', '2B: 2xN16, 2x16G, 2xNPIV', '3A: 2xN16', '3B: 2x16G')
    connected_devices_df = create_fabric_swpair_device_link_description(connected_devices_df, device_links_df)
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


def create_sw_device_link_description(connected_devices_df, phys_link_count_column):
    """Function to create speed and NPIV link description for switch -> device_name level
    with switchPair_id and fabric_label. Each row is inique switch -> device_name level coneection
    with 'Link_description' presented as combination of 'switchPair_id', 'Fabric_label', 
    'Link_description_speed', 'Link_description_port_NPIV'  ('2A: 2xN16, 2x16G, 2xNPIV')"""


    # create speed link description for switch -> device_name level  (2xN16, 2x16G)
    link_description_speed_df = create_sw_device_link_speed_description(connected_devices_df, phys_link_count_column)
    # create npiv link description for switch -> device_name level (2xNPIV)
    link_description_npiv_df = create_sw_device_link_npiv_description(connected_devices_df)

    # join speed and npiv details
    device_links_df = pd.merge(link_description_speed_df, link_description_npiv_df, how='left', 
                               on=['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', 'deviceType'])
    # create Link description column as combination of speed and npiv details
    sep = ' + ' if CLEAR_NPIV_LINKS else ', '
    device_links_df = dfop.merge_columns(device_links_df, summary_column='Link_description', 
                                         merge_columns=['Link_description_speed', 'Link_description_port_NPIV'], sep=sep)
    # link description as combination of switchPair_id, fabric_label and link description
    device_links_df['switchPair_id_str'] = device_links_df['switchPair_id'].astype(int).astype(str)
    device_links_df['Link_description_sw_pair_level'] = \
        device_links_df['switchPair_id_str'] + device_links_df['Fabric_label'] + ': ' + device_links_df['Link_description']
    device_links_df.drop(columns='switchPair_id_str', inplace=True)
    return device_links_df


def create_sw_device_link_speed_description(connected_devices_df, phys_link_count_column):
    """Function to create speed link description for switch -> device_name level  (2xN16, 2x16G)"""

    if CLEAR_NPIV_LINKS and connected_devices_df['port_NPIV'].notna().any():
        mask_npiv = connected_devices_df['port_NPIV'].str.lower().str.contains('npiv', na=False)
        connected_devices_df.loc[mask_npiv, 'speed'] = None

    # count values in 'speed' columns for fabric_name -> fabric_label -> switch -> device_name level
    link_description_speed_df = count_links_with_values_in_column(connected_devices_df, count_column='speed', count_value=phys_link_count_column)
    link_description_speed_df.drop(columns=['Connected_portWwn_npiv_free', 'Connected_portWwn'], errors='ignore', inplace=True)

    # print('\n')
    # print(link_description_speed_df)
    # print(link_description_speed_df['Link_description_speed'].unique())
    # mask_zero = link_description_speed_df['Link_description_speed'] == '0x'
    # print(link_description_speed_df.loc[mask_zero])
    # mask_netapp = link_description_speed_df['Device_Host_Name'].str.lower().str.contains('netapp25')
    # print(link_description_speed_df.loc[mask_netapp])
    # exit()

    # mask_zero = link_description_speed_df['Link_description_speed'].str.contains('0x', na=False)
    # link_description_speed_df.loc[mask_zero, 'Link_description_speed'] = None
    # link_description_speed_df.dropna(subset='Link_description_speed', inplace=True)

    # create link description for each switch->device 
    # join and port speed for switch->device (device ports connected to the same switch might have different speed )
    link_description_speed_df = link_description_speed_df.groupby(
        by=['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 
            'Device_Host_Name', 'deviceType'])['Link_description_speed'].agg(', '.join).to_frame()
    link_description_speed_df.reset_index(inplace=True)

    dfop.sort_cell_values(link_description_speed_df, 'Link_description_speed')
    return link_description_speed_df


def create_sw_device_link_npiv_description(connected_devices_df):
    """Function to create NPIV link description for switch -> device_name level  (2xNPIV)"""
    
    connected_devices_df['Connected_portWwn_npiv_free'] = None
    if connected_devices_df['port_NPIV'].notna().any():
        mask_npiv = connected_devices_df['port_NPIV'].str.lower().str.contains('npiv', na=False)
        connected_devices_df.loc[mask_npiv, 'Connected_portWwn_npiv'] = connected_devices_df.loc[mask_npiv, 'Connected_portWwn']


    
    # count values in port_NPIV' columns for fabric_name -> fabric_label -> switch -> device_name level
    link_description_npiv_df = count_links_with_values_in_column(connected_devices_df, count_column='port_NPIV', count_value='Connected_portWwn_npiv')
    link_description_npiv_df.drop(columns=['Connected_portWwn_npiv', 'Connected_portWwn'], errors='ignore', inplace=True)


    # print('\n')
    # print(link_description_npiv_df)
    # print(link_description_npiv_df['Link_description_port_NPIV'].unique())
    # mask_zero = link_description_npiv_df['Link_description_port_NPIV'] == '0x'
    # print(link_description_npiv_df.loc[mask_zero])

    # mask_zero = link_description_npiv_df['Link_description_port_NPIV'].str.contains('0x', na=False)
    # link_description_npiv_df.loc[mask_zero, 'Link_description_port_NPIV'] = None
    # # drop empty npiv rows to avoid duplication if device have normal and npiv connections
    # link_description_npiv_df.dropna(subset='Link_description_port_NPIV', inplace=True)
    
    return link_description_npiv_df
       

def count_links_with_values_in_column(connected_devices_df, count_column, count_value):
    """Function to count values in count_column column for
    fabric_name -> fabric_label -> switch -> device_name level.
    Result column presented as string 'number X value'. Each unique value for the 
    switch -> device connection is on separate rows (2XN16 and 4X16G)."""
    
    # result column name
    link_description_column = 'Link_description_' + count_column
    # count links number with identical parameters (values) for each switch -> device connection
    group_columns = ['Fabric_name', 'Fabric_label', 'switchWwn', 'switchPair_id', 'Device_Host_Name', count_column, 'deviceType']


    # device_links_df = connected_devices_df.groupby(by=group_columns, dropna=False)['Connected_portWwn'].agg('count').to_frame()

    device_links_df = connected_devices_df.groupby(by=group_columns, dropna=False)[count_value].agg('count').to_frame()

    device_links_df.reset_index(inplace=True)
    # device_links_df.rename(columns={'Connected_portWwn': 'Link_quantity'}, inplace=True)
    device_links_df.rename(columns={count_value: 'Link_quantity'}, inplace=True)
    

    # present result link_description_column as string 'counted number X value'
    device_links_df['Link_quantity'] = device_links_df['Link_quantity'].astype('str')
    device_links_df = dfop.merge_columns(device_links_df, summary_column=link_description_column, merge_columns=['Link_quantity', count_column], sep='x')
    # add 'x' to device XD links (separator 'x' on prev step was not added for XD links)
    device_links_df.loc[device_links_df[link_description_column].str.contains('^\d+$'), link_description_column] = device_links_df[link_description_column] + 'x'
    
    mask_zero = device_links_df[link_description_column].str.contains('0x', na=False)
    device_links_df.loc[mask_zero, link_description_column] = None
    device_links_df.dropna(subset=link_description_column, inplace=True)



    
    # for NPIV link_description_column clean link description if theris no npiv
    # if 'npiv' in count_column.lower():
    #     device_links_df.loc[~device_links_df[link_description_column].str.contains('NPIV', na=False), link_description_column] = None


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
    for column in ['Connected_portWwn', 'Connected_portWwn_npiv_free', 'Connected_portWwn_npiv', 
                   'Connected_portId', 'speed', 'port_NPIV']:
        if column in connected_devices_df.columns:
            connected_devices_df.drop(columns=column, inplace=True)
    # connected_devices_df.drop(columns=['Connected_portWwn', 'Connected_portId', 'speed', 'port_NPIV'], inplace=True)
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
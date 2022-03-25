"""Module to count device connection statistics (port numbers, port speeds, virtual cahnnel usage)
and add notes if some criteria are not fullfilled"""


import pandas as pd
import numpy as np
from .portcmd_device_connection_statistics_notes import add_notes

unique_vc_str ='Unique_VC_quantity_'
bandwidth_str = 'Bandwidth_'
device_port_quantity_str = 'Device_port_quantity_'
unique_slot_str = 'Unique_switch_slot_quantity_'
unique_port_speed_str = 'Unique_port_speed_quantity_'
director_str = 'director_quantity'
switch_str = 'switch_quantity'

columns_str = [unique_vc_str, unique_slot_str, unique_port_speed_str, bandwidth_str, director_str]

def device_connection_statistics(portshow_aggregated_df):
    """Main function to count device connection statistics"""
    
    portshow_aggregated_modified_df, fabric_labels_lst = prior_preparation(portshow_aggregated_df)
    portshow_vc_unique_df, portshow_switch_unique_df, portshow_slot_unique_df, portshow_port_speed_unique_df = unique_values(portshow_aggregated_modified_df)
    # count statistcics for ports quantity, ports speed, ports vc and device unique vc statistics for each device
    device_connection_statistics_df = count_device_connection_statistics(portshow_aggregated_modified_df, portshow_vc_unique_df, portshow_switch_unique_df, 
                                                                            portshow_slot_unique_df, portshow_port_speed_unique_df)
    # add device bandwidth in each Fabric_label
    device_connection_statistics_df = count_device_bandwidth(device_connection_statistics_df, portshow_aggregated_modified_df)
    # add notes
    device_connection_statistics_df = add_notes(device_connection_statistics_df, fabric_labels_lst, columns_str)
    
    # drop fabric_labels columns and remove 'Unknown' tag from 'Device_Location'
    device_connection_statistics_df.drop(columns=fabric_labels_lst, inplace=True)
    device_connection_statistics_df['Device_Location'] = device_connection_statistics_df['Device_Location'].replace(['Unknown'], np.nan)
    
    # change columns order
    stat_columns = device_connection_statistics_df.columns.tolist()
    device_columns = ['Fabric_name', 'Device_Host_Name', 'Device_Location', 'deviceType', 'deviceSubtype']	
    reorder_stat_columns = [*device_columns, *stat_columns[5:]]
    device_connection_statistics_df = device_connection_statistics_df[reorder_stat_columns]
    device_connection_statistics_df.fillna(np.nan, inplace=True)
    
    return device_connection_statistics_df


def prior_preparation(portshow_aggregated_df):
    """Function to filter required data from portshow_aggregated_df DataFrame
    to count statistics"""

    # AG mode switches dropped to avoid duplicate  connection information
    mask_switch_native = portshow_aggregated_df['switchMode'] == 'Native'
    # drop Switches and Virtual connect modules
    mask_not_switch_vc = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    # device class has to be defined
    mask_devicetype = portshow_aggregated_df['deviceType'].notna()
    # drop fabrics which are not part of assessment
    mask_fabric_name_label = portshow_aggregated_df[['Fabric_name', 'Fabric_label']].notna().all(axis=1)
    # drop fabrics which are out of assessment scope
    mask_valid_fabric = ~portshow_aggregated_df['Fabric_name'].isin(['x', '-'])
    
    mask_complete = mask_fabric_name_label & mask_switch_native & mask_not_switch_vc & mask_devicetype & mask_valid_fabric
    columns_lst = ['Fabric_name', 'deviceType',	'Device_Location', 'deviceSubtype', 'Device_Host_Name', 
                   'Fabric_label', 'speed', 'Virtual_Channel', 'switchWwn', 'switchType' , 'switchClass', 'slot']
    portshow_aggregated_modified_df = portshow_aggregated_df.loc[mask_complete, columns_lst].copy()
    
    # assign 'unknown' tag for na Device_Location for grouping and crosstab operations
    portshow_aggregated_modified_df['Device_Location'].fillna('Unknown', inplace=True)
    # set 'Virtual_Channel', 'speed' with Fabric_label tag
    portshow_aggregated_modified_df['Virtual_Channel'] = \
        portshow_aggregated_modified_df['Fabric_label'] + '_' + portshow_aggregated_modified_df['Virtual_Channel']
    portshow_aggregated_modified_df['speed'] = \
        portshow_aggregated_modified_df['Fabric_label'] + '_' + portshow_aggregated_modified_df['speed']
    # set column name tag for Fabric_labels for later port number count
    portshow_aggregated_modified_df['Fabric_connection'] = \
        device_port_quantity_str + portshow_aggregated_modified_df['Fabric_label']
    
    # director or switch connection tag
    
    # TO_REMOVE mask director implemente throught the switch Class
    # portshow_aggregated_modified_df['switchType'] = portshow_aggregated_modified_df['switchType'].astype('float64').astype('int64')
    # director_type = [42, 62, 77, 120, 121, 165, 166, 179, 180]
    # mask_director = portshow_aggregated_modified_df['switchType'].isin(director_type)
    
    mask_director = portshow_aggregated_modified_df['switchClass'] == 'DIR'
    portshow_aggregated_modified_df['switchType'] = np.where(mask_director, director_str , switch_str)
    portshow_aggregated_modified_df['switchType'] = portshow_aggregated_modified_df['Fabric_label'] + '_' + portshow_aggregated_modified_df['switchType']
    
    # extract speed values
    portshow_aggregated_modified_df['speed_extract'] = portshow_aggregated_modified_df['speed'].str.extract(r'(\d+)')

    # fabric_labels used in Fabric
    fabric_labels_lst = portshow_aggregated_modified_df['Fabric_label'].unique()
    fabric_labels_lst.sort()
    
    return portshow_aggregated_modified_df, fabric_labels_lst


def unique_values(portshow_modified_df):
    """Function to create DataFrames to count for each device unique VC quantity, 
    unique switch type connected quantity (combination of switchWwn and swicthType),
    unique switch slot connection quantity (combination of switchWwn and slot number),
    unique portspeed quantity"""    

    # create dataFrames count to count quantity of unique VC, switcName, slot number
    columns = ['Fabric_name', 'deviceType', 'Device_Location', 'deviceSubtype','Device_Host_Name', 'Fabric_label']
    
    # create DataFrame with unique VC for each device only to verify if Virtual channel divercitu present for each device
    unique_vc_columns = columns + ['Virtual_Channel']
    portshow_vc_unique_df = portshow_modified_df[unique_vc_columns].copy()
    portshow_vc_unique_df.drop_duplicates(inplace=True)
    # portshow_vc_unique_df = portshow_modified_df.drop_duplicates().copy()
    portshow_vc_unique_df['Unique_Virtual_Channel'] = unique_vc_str +  portshow_vc_unique_df['Fabric_label']
    
    # create DataFrame with unique switchWwn and director, swicth tags
    # to verify how many switches divice connected to and its type (direcotr, switch)
    unique_switch_columns = columns + ['switchWwn', 'switchType']
    portshow_switch_unique_df = portshow_modified_df[unique_switch_columns].copy()
    portshow_switch_unique_df.drop_duplicates(inplace=True)
    
    # create DataFrame with unique slot number to verify if slot diversity present for each device
    unique_slot_columns = columns + ['switchWwn', 'slot']
    portshow_slot_unique_df = portshow_modified_df[unique_slot_columns].copy()
    portshow_slot_unique_df.drop_duplicates(inplace=True)
    portshow_slot_unique_df['Unique_switch_slot'] = unique_slot_str +  portshow_slot_unique_df['Fabric_label']
    
    # create DataFrame with unique port speed to verify if speed diversity present for each device
    unique_slot_columns = columns + ['speed_extract']
    portshow_port_speed_unique_df = portshow_modified_df[unique_slot_columns].copy()
    portshow_port_speed_unique_df.drop_duplicates(inplace=True)
    portshow_port_speed_unique_df['Unique_port_speed'] = unique_port_speed_str +  portshow_port_speed_unique_df['Fabric_label']
    
    return portshow_vc_unique_df, portshow_switch_unique_df, portshow_slot_unique_df, portshow_port_speed_unique_df


def count_device_connection_statistics(portshow_aggregated_modified_df, portshow_vc_unique_df, 
                                        portshow_switch_unique_df, portshow_slot_unique_df, portshow_port_speed_unique_df):
    """Function to count ports quantity, ports speed, ports vc and device unique vc statistics for each device"""

    device_connection_statistics_df = pd.DataFrame()
    statistics_lst =  [('Fabric_connection', portshow_aggregated_modified_df),
                       ('Fabric_label', portshow_aggregated_modified_df),
                       ('speed', portshow_aggregated_modified_df),
                       ('switchType', portshow_switch_unique_df),
                       ('Virtual_Channel', portshow_aggregated_modified_df),
                       ('Unique_Virtual_Channel', portshow_vc_unique_df),
                       ('Unique_switch_slot', portshow_slot_unique_df),
                       ('Unique_port_speed', portshow_port_speed_unique_df)]
    
    # count statistcics for each column, DataFRame pair in statistics_lst
    for column, df in statistics_lst:
        margins_flag = True if column == 'Fabric_connection' else False
        # groupby columns
        index_lst = [df.Fabric_name,
                     df.deviceType,
                     df.Device_Location,
                     df.deviceSubtype,
                     df.Device_Host_Name]
        # count statistics for current column
        current_statistics_df = pd.crosstab(index = index_lst,
                                columns = df[column],
                                margins=margins_flag)
        # add current_statistics_df DataFrame to aggregated device_connection_statistics_df DataFRame
        if device_connection_statistics_df.empty:
            device_connection_statistics_df = current_statistics_df.copy()
        else:
            device_connection_statistics_df = device_connection_statistics_df.merge(current_statistics_df, how='left', 
                                                                                    left_index=True, right_index=True)
    # rename 'All' column
    device_connection_statistics_df.rename(columns={'All': device_port_quantity_str + 'Total'}, inplace=True)                

    return device_connection_statistics_df


def count_device_bandwidth(device_connection_statistics_df, portshow_aggregated_modified_df):
    """Function to add total bandwidth for each device in device_connection_statistics_df DataFrame"""

    portshow_speed_df = portshow_aggregated_modified_df.copy()
    # extract speed value from 'speed' column
    portshow_speed_df['speed_extract'] = portshow_speed_df['speed'].str.extract(r'(\d+)')
    portshow_speed_df['speed_extract'] = portshow_speed_df['speed_extract'].astype('int64', errors='ignore')
    # add 'Bandwidth' tag to Fabric_label to create columns later
    portshow_speed_df['Bandwidth_label'] = bandwidth_str + portshow_speed_df['Fabric_label']
    
    # groupby ports for each device in Fabric_label and summarize port speeds to get device bandwidth
    grp_lst = ['Fabric_name', 'deviceType',	'Device_Location', 'deviceSubtype', 'Device_Host_Name', 'Bandwidth_label']
    bandwidth_df = portshow_speed_df.groupby(grp_lst, as_index = False)['speed_extract'].sum()
    # move 'Bandwidth_label' column values as independent column names with values from 'speed_extract' column
    bandwidth_pivot_df = bandwidth_df.pivot_table(values='speed_extract', index=grp_lst[:-1], columns='Bandwidth_label', aggfunc='first')
    # if device has no connection to Fabric_label than bandwidth is zero
    bandwidth_pivot_df.fillna(0, inplace=True)
    
    # add bandwidth info to device_connection_statistics_df DataFrame
    device_connection_statistics_df = device_connection_statistics_df.merge(bandwidth_pivot_df, how='left', left_index=True, right_index=True)
    device_connection_statistics_df.reset_index(inplace=True)

    return device_connection_statistics_df
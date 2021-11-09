"""Module to identify MAPS Dashboard port information"""

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
# import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop

# from common_operations_dataframe import dataframe_fillna, explode_columns


switch_columns = ['Fabric_name', 'Fabric_label', 
                  'configname', 'chassis_name', 'chassis_wwn',
                  'switchWwn', 'switchName',
                  'Configured_Notifications', 'Fabric_Vision_license']

exploded_columns = ['Exploded_column', 'Exploded_values']
slot_port_columns = ['slot', 'port']


def maps_db_ports(portshow_sfp_aggregated_df, switch_params_aggregated_df, comp_dct):
    """Function to verify Quarantined_Ports, Decommissioned_Ports, Fenced_Ports
    and Top_Zoned_PIDs in MAPS Dashboard"""

    # # regular expression patterns
    # *_, comp_dct = re_pattern_lst

    portshow_cp_df = portshow_sfp_aggregated_df.copy()
    switch_params_cp_df = switch_params_aggregated_df.copy()
    switch_params_cp_df['switchName'].fillna(switch_params_cp_df['SwitchName'], inplace=True)
    # remove uninfomative values from switch DataFrame
    switch_params_cp_df.replace(to_replace={comp_dct['maps_clean']: np.nan} , regex=True, inplace=True)
    # explode ports so that each port presented as separate row
    maps_ports_df, top_zoned_ports_df = explode_maps_ports(switch_params_cp_df)
    # extract slot, port, pid and it-flows
    maps_ports_df, top_zoned_ports_df = extract_exploded_ports(maps_ports_df, top_zoned_ports_df, comp_dct)
    # find port information in portshow_sfp_aggregated_df
    maps_ports_df, top_zoned_ports_df = fillna_port_information(portshow_cp_df, maps_ports_df, top_zoned_ports_df)
    # concatenate maps_ports_df and top_zoned_ports_df
    maps_db_ports_df = pd.concat([maps_ports_df, top_zoned_ports_df], ignore_index=True)
    return maps_db_ports_df


def explode_maps_ports(switch_params_cp_df):
    """Function to explode MAPS Dashboard ports"""

    maps_ports_df = dfop.explode_columns(switch_params_cp_df, 
                                    'Quarantined_Ports', 'Decommissioned_Ports', 
                                    'Fenced_Ports', 'Fenced_circuits', sep=',')
    top_zoned_ports_df = dfop.explode_columns(switch_params_cp_df, 'Top_Zoned_PIDs', sep=' ')
    # drop excessive columns
    if not maps_ports_df.empty:
        maps_ports_df = maps_ports_df[[*switch_columns, *exploded_columns]].copy()
    if not top_zoned_ports_df.empty:
        top_zoned_ports_df = top_zoned_ports_df[[*switch_columns, *exploded_columns]].copy()
    return maps_ports_df, top_zoned_ports_df


def extract_exploded_ports(maps_ports_df, top_zoned_ports_df, comp_dct):
    """Function to extract slot, port and portid, it-flows from exploded column"""

    # slot_port_pattern = '(?:(\d+)/)?(\d+)'
    slot_port_pattern = comp_dct['slot_port']
    if not maps_ports_df.empty:
        maps_ports_df[slot_port_columns] = maps_ports_df['Exploded_values'].str.extract(slot_port_pattern)
        maps_ports_df['slot'].fillna('0', inplace=True)
    
    # pid_flow_pattern = '0x([0-9a-f]{6})\((\d+)\)'
    pid_flow_pattern = comp_dct['pid_flow']
    if not top_zoned_ports_df.empty:
        top_zoned_ports_df[['Connected_portId', 'it-flows']] = top_zoned_ports_df['Exploded_values'].str.extract(pid_flow_pattern)
    return maps_ports_df, top_zoned_ports_df


def fillna_port_information(portshow_cp_df, maps_ports_df, top_zoned_ports_df):
    """Function to add port and connected device information based on 
    slot, port or portID"""

    # fillna port information
    filled_columns = ['portIndex', 'portState', 'Connected_portWwn', 
                      'Device_Host_Name', 'Device_Port', 'alias',
                      'speed',  'deviceType', 'deviceSubtype',
                      'Slow_Drain_Device', 'Connected_through_AG',  
                      'stat_ftx', 'stat_frx', 'tim_txcrd_z']
    
    sort_columns = ['Fabric_name', 'Fabric_label', 
                    'chassis_name', 'switchName', 'Exploded_column', 
                    'slot', 'port', 'Connected_portId']
    
    if not maps_ports_df.empty:
        # convert slot, port to int
        for column in slot_port_columns:
            portshow_cp_df[column] = portshow_cp_df[column].astype('int64')
            maps_ports_df[column] = maps_ports_df[column].astype('int64')
    
        maps_ports_df = dfop.dataframe_fillna(maps_ports_df, portshow_cp_df, 
                                         join_lst=[*switch_columns[:-2], *slot_port_columns], 
                                         filled_lst=['Connected_portId', *filled_columns], 
                                         remove_duplicates=False)
        maps_ports_df.sort_values(by=sort_columns, inplace=True)
    
    if not top_zoned_ports_df.empty:
        top_zoned_ports_df = dfop.dataframe_fillna(top_zoned_ports_df, portshow_cp_df, 
                                              join_lst=[*switch_columns[:-2], 'Connected_portId'],
                                              filled_lst=[*slot_port_columns, *filled_columns])
        top_zoned_ports_df.sort_values(by=sort_columns, inplace=True)        
    return maps_ports_df, top_zoned_ports_df


"Module with auxiliary functions to combine storage host DaraFrame"

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop


def get_ctrl_ports_fabric(portshow_aggregated_df, zoning_aggregated_df, storage_host_df):

    # add controllers ports Fabric_name and Fabric_label
    storage_host_df = dfop.dataframe_fillna(storage_host_df, portshow_aggregated_df, 
                                                    join_lst=['PortName'], filled_lst=['Fabric_name', 'Fabric_label'])
    # rename controllers NodeName and PortName
    rename_columns = {'NodeName': 'Storage_Port_Wwnn', 'PortName': 'Storage_Port_Wwnp'}
    storage_host_df.rename(columns=rename_columns, inplace=bool)

    # 'clean' Wwn column to have Wwnp only. check Wwnn -> Wwnp correspondance in all fabrics
    storage_host_df = dfop.replace_wwnn(storage_host_df, 'Host_Wwn', 
                                                    portshow_aggregated_df, ['NodeName', 'PortName'])
    # add Host Wwnp zoning device status in fabric of storage port connection
    storage_host_df = dfop.dataframe_fillna(storage_host_df, zoning_aggregated_df, 
                                                        join_lst=['Fabric_name', 'Fabric_label', 'PortName'], 
                                                        filled_lst=['Fabric_device_status'])
    # rename controllers Fabric_name and Fabric_label
    rename_columns = {'Fabric_name': 'Storage_Fabric_name', 'Fabric_label': 'Storage_Fabric_label', 
                        'Fabric_device_status': 'Fabric_host_status'}
    storage_host_df.rename(columns=rename_columns, inplace=bool)
    return storage_host_df



def get_host_ports_fabric(portshow_aggregated_df, storage_host_df):

    # add host information
    host_columns = ['Fabric_name', 'Fabric_label', 'chassis_name', 'switchName', 
                    'Index_slot_port', 'portIndex', 'slot', 'port',  'Connected_portId', 
                    'Device_Host_Name', 'Device_Port', 'Host_OS', 'Device_Location', 
                    'Device_Host_Name_per_fabric_name_and_label',   'Device_Host_Name_per_fabric_label', 
                    'Device_Host_Name_per_fabric_name', 'Device_Host_Name_total_fabrics']
    storage_host_df = dfop.dataframe_fillna(storage_host_df, portshow_aggregated_df, 
                                                    join_lst=['PortName'], filled_lst=host_columns, remove_duplicates=False)
    # rename host columns
    rename_columns = {'Fabric_name': 'Host_Fabric_name', 'Fabric_label': 'Host_Fabric_label', 'PortName': 'Host_Wwnp'}
    storage_host_df.rename(columns=rename_columns, inplace=bool)
    return storage_host_df


def drop_unequal_fabrics_ports(storage_host_df):
    """Function to drop rows where host and controller ports are in the different fabric labels.
    Ports which are not connected to any fabric are not fitered off"""

    mask_fabric_label_notna = storage_host_df[['Host_Fabric_label', 'Storage_Fabric_label']].notna().all(axis=1)
    mask_fabric_label_equal = storage_host_df['Host_Fabric_label'] == storage_host_df['Storage_Fabric_label']
    storage_host_filtered_df = storage_host_df.loc[(mask_fabric_label_notna & mask_fabric_label_equal) | ~mask_fabric_label_notna].copy()
    storage_host_filtered_df.reset_index(drop=True, inplace=True)
    return storage_host_filtered_df



def verify_host_mode(storage_host_aggregated_df):
    """Function to verify if persona (storage host mode) is defined in correspondence with host os"""

    # os_lst = ['vmware', 'windows', 'linux']
    # # cumulative host mode mask
    # mask_persona_correct = None
    # for os_type in os_lst:
    #     # host mode matches os name except for linux
    #     os_mode = os_type if os_type != 'linux' else 'generic'
    #     # mask for current os
    #     mask_os = (storage_host_aggregated_df['Persona'].str.lower().str.contains(os_mode) & \
    #                 storage_host_aggregated_df['Host_OS'].str.lower().str.contains(os_type))
    #     # add current mask to cumulative mask
    #     if mask_persona_correct is None:
    #         mask_persona_correct = mask_os
    #     else:
    #         mask_persona_correct = mask_persona_correct | mask_os

    os_type_lst = ['vmware', 'windows', 'linux', 'linux']
    host_mode_lst = ['vmware', 'windows', 'linux', 'generic']
    # cumulative host mode mask
    mask_persona_correct = None
    for os_type, host_mode in zip(os_type_lst, host_mode_lst):
        # mask for current os
        mask_os = (storage_host_aggregated_df['Persona'].str.lower().str.contains(host_mode) & \
                    storage_host_aggregated_df['Host_OS'].str.lower().str.contains(os_type))
        # add current mask to cumulative mask
        if mask_persona_correct is None:
            mask_persona_correct = mask_os
        else:
            mask_persona_correct = mask_persona_correct | mask_os

    # perform checking for rows with existing data only both in Persona and Host_OS columns
    mask_os_notna = storage_host_aggregated_df[['Persona', 'Host_OS']].notna().all(axis=1)
    storage_host_aggregated_df['Persona_correct'] = \
        np.select([mask_os_notna & mask_persona_correct, mask_os_notna & ~mask_persona_correct], ['Yes', 'No'], default=pd.NA)
    # replace pd.NA values
    storage_host_aggregated_df.fillna(np.nan, inplace=True)
    return storage_host_aggregated_df


def verify_storage_host_zoning(storage_host_aggregated_df, zoning_aggregated_df):
    """Function to verify if storage port and host port are zoned"""
    
    # prepare zoning (slice effective zoning and local or imported ports only)
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'remote_imported'])
    mask_effective = zoning_aggregated_df['cfg_type'] == 'effective'
    zoning_valid_df = zoning_aggregated_df.loc[mask_effective & mask_connected].copy()
    # find zones with 3PAR storages only to reduce search time
    group_columns = ['Fabric_name', 'Fabric_label', 'zone']
    zone_3par_dorado_df = \
        zoning_valid_df.groupby(by=group_columns).filter(lambda zone: zone['deviceSubtype'].str.lower().isin(['3par', 'huawei']).any())

    storage_host_aggregated_df['zone'] = \
        storage_host_aggregated_df.apply(lambda series: find_zones(series, zone_3par_dorado_df), axis=1)

    return storage_host_aggregated_df


def find_zones(series, zoning_valid_df):
    """Auxiliary function for verify_storage_host_zoning fn 
    to find zones in effective configuration with storage port and server"""
    
    # verify rows where storage port and server are in same fabric only
    if series['Fabric_host_status'] in  ['local', 'remote_imported']: 
        group_columns = ['Fabric_name', 'Fabric_label', 'zone']
        storage_host_sr = series[['Storage_Port_Wwnp', 'Host_Wwnp']]

        # excessive step zoning_valid_df is already filtered TO_REMOVE 
        mask_same_fabic = (zoning_valid_df['Fabric_name'] == series['Storage_Fabric_name']) & \
                            (zoning_valid_df['Fabric_label'] == series['Storage_Fabric_label'])
        zoning_valid_fabric_df = zoning_valid_df.loc[mask_same_fabic].copy()
        
        # find zones with storage port wwnp and host wwnp
        storage_host_zone_df = \
            zoning_valid_fabric_df.groupby(by=group_columns).filter(lambda zone: storage_host_sr.isin(zone['PortName']).all())
        # get zones defined in the same fabric as storage port connection fabric
        mask_same_fabic = (storage_host_zone_df['Fabric_name'] == series['Storage_Fabric_name']) & \
                            (storage_host_zone_df['Fabric_label'] == series['Storage_Fabric_label'])
        zoning_valid_df = storage_host_zone_df.loc[mask_same_fabic].copy()
        # if zones are found return string of zones separated by commas
        if not zoning_valid_df.empty:
            zone_sr = zoning_valid_df['zone'].drop_duplicates()
            zones_str = ', '.join(zone_sr.to_list())
            return zones_str

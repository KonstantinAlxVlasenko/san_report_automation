
"""Module to filter unique device ports"""

import pandas as pd

import utilities.dataframe_operations as dfop


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
    connected_devices_df = dfop.concatenate_dataframes_vertically(connected_devices_df, npv_ag_connected_devices_cp_df, fcr_xd_proxydev_cp_df)
    # connected_devices_df = pd.concat([connected_devices_df, npv_ag_connected_devices_cp_df, fcr_xd_proxydev_cp_df]) # depricated method
    
    connected_devices_df.drop_duplicates(subset=['Fabric_name', 'Fabric_label', 'switchWwn', 'Connected_portWwn'], 
                                            inplace=True, ignore_index=True)
    return connected_devices_df


def filter_edge_devices(portshow_aggregated_df):
    """Function to filter all devices from portshow_aggregated_df"""
    
    mask_portwwn_notna = portshow_aggregated_df['Connected_portWwn'].notna()
    mask_device_lib_srv_stor = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    connected_devices_df = portshow_aggregated_df.loc[mask_portwwn_notna & mask_device_lib_srv_stor].copy()
    return connected_devices_df


def tag_npiv_devices(connected_devices_df):
    """Function to tag NPIV devices"""

    mask_npiv = connected_devices_df['Device_type'].str.contains('NPIV', na=False)
    dfop.column_to_object(connected_devices_df, 'port_NPIV')
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
    npv_ag_connected_devices_cp_df = dfop.dataframe_fillna(npv_ag_connected_devices_cp_df, portshow_ag_df, 
                                                            join_lst=['Fabric_name', 'Fabric_label', 'Connected_portWwn'], 
                                                            filled_lst=['speed'])
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
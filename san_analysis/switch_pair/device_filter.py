"""Module to filter devices connected to the switches"""

import utilities.dataframe_operations as dfop


def find_sw_brocade_connected_devices(portshow_aggregated_df):
    """Function to find devices connected to Brocade switches. 
    Devices connected to Native mode switches with direct or through NPIV port connection.
    Devices connecetd directly to AG mode switches"""

    mask_devicetype_notna = portshow_aggregated_df['deviceType'].notna()
    mask_device_lib_srv_stor = ~portshow_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    brocade_connected_devices_df = portshow_aggregated_df.loc[mask_devicetype_notna & mask_device_lib_srv_stor]
    return brocade_connected_devices_df


def find_sw_npv_ag_connected_devices(switch_pair_df, portshow_aggregated_df, merge_column):
    """Function to find deviced connected behind NPIV switch ports (AG mode switches or VC module and Cisco switches).
    AG switchWwn identified based on AG switchPort Wwpn except first two symbols pairs(merge_column='oui_board_sn).
    VC and Cisco switchWwn retrived directly from device Wwnn (merge_column='NodeName').
    Parameter merge_column idetifies column in switch_pair_df which values used to filter 
    AG switch, Cisco switch or VC module ports in in portshow_aggregated_df."""
    
    mask_native = portshow_aggregated_df['switchMode'] == 'Native'
    portshow_cp_df = portshow_aggregated_df.loc[mask_native].copy()
    # extract Domain ID and Area ID from FCID to identify all devices connected throught the same NPIV port
    portshow_cp_df['Connected_Domain_Area_Id'] = portshow_cp_df['Connected_portId'].str.extract(r'^(.{4})(?:.{2})')
    
    # create dataframe containing oui, NodeName correspondance with switchWwn
    switch_wwn_oui_df = switch_pair_df[['Fabric_name', 'Fabric_label', 'switchWwn']].copy()
    switch_wwn_oui_df['oui_board_sn'] = switch_pair_df['switchWwn'].str.extract(r'^[\da-f:]{6}(.+)')
    switch_wwn_oui_df['NodeName'] = switch_pair_df['switchWwn']
    # switchWwn is already present in portshow_cp_df thus Connected_switchWwn column is used
    switch_wwn_oui_df['Connected_switchWwn'] = switch_wwn_oui_df['switchWwn']
    
    # add connected AG, VC or NPV switch Wwnn to Native switch port information
    portshow_cp_df = dfop.dataframe_fillna(portshow_cp_df, switch_wwn_oui_df, 
                                            join_lst=['Fabric_name', 'Fabric_label', merge_column], 
                                            filled_lst=['Connected_switchWwn'])
    # filter AG, VC or NPV switch ports connected to Native switches based on merge_column
    mask_connected_wwn_oui = portshow_cp_df[merge_column].isin(switch_wwn_oui_df[merge_column])
    portshow_sw_df = portshow_cp_df.loc[mask_connected_wwn_oui].copy()

    # add switchWwn information to all devices within the same domain ID, Area ID (adding Wwnn of the switch where all devices directly connected)
    portshow_cp_df = dfop.dataframe_fillna(portshow_cp_df, portshow_sw_df, join_lst=['Fabric_name', 'Fabric_label', 'Connected_Domain_Area_Id'], 
                                                                           filled_lst=['Connected_NPIV', 'Connected_switchWwn'])
    # filter device ports only connecetd to the AG, VC and NPV switches from switch_pair_df
    mask_devicetype_notna = portshow_cp_df['deviceType'].notna()
    mask_device_lib_srv_stor = ~portshow_cp_df['deviceType'].isin(['SWITCH', 'VC'])
    mask_connected_switchwwn_notna = portshow_cp_df['Connected_switchWwn'].notna()
    device_columns = ['Fabric_name', 'Fabric_label', 'Connected_switchWwn', 'Device_Host_Name']
    npv_ag_connected_devices_df = portshow_cp_df.loc[mask_devicetype_notna & mask_device_lib_srv_stor & mask_connected_switchwwn_notna, device_columns]
    # switchWwn, switchType, switchMode columns used to run function to find pair switch (pair candidate must have same switcType and switchMode)
    npv_ag_connected_devices_df.rename(columns={'Connected_switchWwn': 'switchWwn'}, inplace=True)
    npv_ag_connected_devices_df = dfop.dataframe_fillna(npv_ag_connected_devices_df, switch_pair_df, 
                                                        join_lst=['switchWwn'], 
                                                        filled_lst=['switchType', 'switchMode'])
    return npv_ag_connected_devices_df
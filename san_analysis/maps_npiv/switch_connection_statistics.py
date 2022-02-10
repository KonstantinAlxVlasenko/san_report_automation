"""Module to count connection statistics for each Native mode switch and find core switch"""


import numpy as np
import pandas as pd
import utilities.dataframe_operations as dfop

sw_columns = ['Fabric_name', 'Fabric_label', 
               'chassis_name', 'SwitchName', 'switchWwn', 'switchPair_id']

# sw_columns = ['Fabric_name', 'Fabric_label', 
#                'chassis_name', 'SwitchName', 'switchWwn']


def switch_connection_statistics_aggregated(switch_params_aggregated_df, isl_statistics_df, npiv_statistics_df, pattern_dct):
    """Main function to count switch connection statistics"""

    sw_connection_statistics_df = native_ag_connection_statistics(isl_statistics_df, npiv_statistics_df)
    if not sw_connection_statistics_df.empty:
        core_sw_df = find_core_switch(sw_connection_statistics_df)
        # add switch role and fos
        sw_connection_statistics_df = dfop.dataframe_fillna(sw_connection_statistics_df, switch_params_aggregated_df, join_lst=sw_columns, 
                                        filled_lst=['switchRole', 'FOS_version', 'switch.edgeHoldTime'])
        # find FOS most recent version in each fabric
        sw_connection_statistics_df = find_max_fabric_fos(sw_connection_statistics_df, pattern_dct)
        # add core switch tag
        sw_connection_statistics_df = dfop.dataframe_fillna(sw_connection_statistics_df, core_sw_df, join_lst=sw_columns, filled_lst=['Core_switch_note'])
        # add notes if principal not in core or if principal fos version is not recent in the fabric
        sw_connection_statistics_df = add_notes(sw_connection_statistics_df)

        # asymmetry_note_columns = ['Native_Asymmetry_note', 'AG_Asymmetry_note']
        # sw_connection_statistics_df = move_column(sw_connection_statistics_df, asymmetry_note_columns, ref_col='')    
    return sw_connection_statistics_df


def connection_statistics(df, connected_dev_columns, tag):
    """Function to count Native or AG switch connection statistics.
    Connections quantity of the each switch to other switches (Native and AG)"""
    
    # statistics DataFrame columns
    stat_columns = [*sw_columns, *connected_dev_columns]
    # 'chassis_name', 'SwitchName' and 'switchWwn' are nan for the summary rows
    mask_fabric_summary = df[sw_columns[2:]].isna().all(axis=1)
    # drop summary rows (switch to switch connection statistics)
    switch_conn_df = df.loc[~mask_fabric_summary, stat_columns].copy()
    # convert connection quantity to numeric type 
    switch_conn_df[connected_dev_columns[2:]] = switch_conn_df[connected_dev_columns[2:]].apply(pd.to_numeric, errors='ignore')


    # summary rows are not changed and concatenated with connection statistics after calculation. 
    # Conneceted device (switch) name and it's wwn are dropped since connection statistics counted for entire switch
    summary_columns = [*sw_columns, *connected_dev_columns[2:]]
    switch_conn_fabric_summary_df = df.loc[mask_fabric_summary, summary_columns].copy()
    switch_conn_fabric_summary_df[connected_dev_columns[2:]] = switch_conn_fabric_summary_df[connected_dev_columns[2:]].apply(pd.to_numeric, errors='ignore')
    # verify if fabric A and B are symmetrical from Native ang AG connection point of view
    switch_conn_fabric_summary_df = dfop.verify_group_symmetry(switch_conn_fabric_summary_df, symmetry_grp=['Fabric_name'], symmetry_columns=connected_dev_columns[2:])
    # sum up connections, links, ports and bandwidth for each switch
    switch_conn_total_df = switch_conn_df.groupby(sw_columns).agg('sum').reset_index()
    # check if swicth have similar connections in all fabric labels
    switch_conn_total_df = dfop.verify_group_symmetry(switch_conn_total_df, symmetry_grp=['Fabric_name', 'switchPair_id'], symmetry_columns=connected_dev_columns[2:])
    # check if switch pair is present
    mask_connection_pair_absent = switch_conn_total_df.groupby(by=['Fabric_name', 'switchPair_id'])['switchWwn'].transform('count') < 2
    switch_conn_total_df.loc[mask_connection_pair_absent , 'Connection_pair_absence_note'] = 'connection_pair_absent'
    switch_conn_total_df['Asymmetry_note'].fillna(switch_conn_total_df['Connection_pair_absence_note'], inplace=True)
    switch_conn_total_df.drop(columns=['Connection_pair_absence_note'], inplace=True)

    # switch quantity are not summed up and each row represents single switch
    if 'Switch_quantity' in switch_conn_total_df.columns:
        switch_conn_total_df['Switch_quantity'] = 1
    
    # add unchanged summary to connection statistics DataFrame
    switch_conn_df = pd.concat([switch_conn_total_df, switch_conn_fabric_summary_df])
    # rename columns with the Native or AG tag
    connected_dev_rename_dct = {k: tag + '_' + k for k in [*connected_dev_columns, 'Asymmetry_note'] if k != 'Switch_quantity'}
    switch_conn_df.rename(columns=connected_dev_rename_dct, inplace=True)
    
    return switch_conn_df
    

def native_ag_connection_statistics(isl_statistics_df, npiv_statistics_df):
    """Function to aggregate stastics for connected switches in Native and AG mode"""
    
    # native connections
    if not isl_statistics_df.empty:
        connected_native_sw_columns = ['Connected_SwitchName', 'Connected_switchWwn', 
                                'Switch_quantity',	'Switch_connection_quantity', 
                                'Logical_link_quantity', 'Physical_link_quantity', 
                                'Port_quantity', 'Bandwidth_Gbps']
        native_switch_conn_df = connection_statistics(isl_statistics_df, connected_native_sw_columns, 'Native')

    #  AG sw connection
    if not npiv_statistics_df.empty:
        npiv_statistics_cp_df = npiv_statistics_df.copy()
        # rename columns to correspond in column name in native_switch_conn_df 
        npiv_statistics_cp_df.rename(columns={'switchName': 'SwitchName', 
                                        'Device_quantity': 'Device_connection_quantity'}, inplace=True)
        connected_ag_dev_columns = [ 
                            'Device_Host_Name', 'NodeName', 
                            'Device_connection_quantity',
                            'Logical_link_quantity', 'Physical_link_quantity', 
                            'Port_quantity', 'Bandwidth_Gbps']
        ag_dev_conn_df = connection_statistics(npiv_statistics_cp_df, connected_ag_dev_columns, 'AG')

    # merge native and ag switches
    if not isl_statistics_df.empty and not npiv_statistics_df.empty:
        sw_connection_statistics_df = native_switch_conn_df.merge(ag_dev_conn_df, how='left', on=sw_columns)
    elif not isl_statistics_df.empty and npiv_statistics_df.empty:
        sw_connection_statistics_df = native_switch_conn_df.copy()
    elif isl_statistics_df.empty and not npiv_statistics_df.empty:
        sw_connection_statistics_df = ag_dev_conn_df.copy()
    else:
        return pd.DataFrame()

    sw_connection_statistics_df.sort_values(by=sw_columns, inplace=True)
    sw_connection_statistics_df = dfop.move_all_down(sw_connection_statistics_df)
    return sw_connection_statistics_df


def find_core_switch(sw_connection_statistics_df):
    """Function to identify core switch in each fabric.
    Core switch is the switch with the largest quantity of connections, port, bandwidth to other native switches.
    If AG switches present in the fabric then it's quantity of connections, port, bandwidth are also taken into account.
    At each step, the maximum value of the parameter in each fabric is determined. Switches for which this parameter is less
    then a value are dropped. Switches with all maximum values are core switches
    """

    # drop summry rows
    mask_fabric_summary = sw_connection_statistics_df[sw_columns[2:]].isna().all(axis=1)
    core_sw_df = sw_connection_statistics_df.loc[~mask_fabric_summary].copy()
    core_sw_df.fillna(0, inplace=True)

    """
    possible parameters columns  
    'Native_Switch_connection_quantity', 
    'Native_Logical_link_quantity', 'Native_Physical_link_quantity', 
    'Native_Port_quantity', 'Native_Bandwidth_Gbps,
    'AG_AG_Device_connection_quantity', 
    'AG_Logical_link_quantity', 'AG_Physical_link_quantity', 
    'AG_Port_quantity', 'AG_Bandwidth_Gbps'
    """
    
    # the maximum value is determined sequentially for the following columns
    connection_columns = ['Native_Switch_connection_quantity', 'Native_Port_quantity', 'Native_Bandwidth_Gbps',
                    'AG_AG_Device_connection_quantity', 'AG_Port_quantity', 'AG_Bandwidth_Gbps']
    connection_columns = [column for column in connection_columns if column in core_sw_df.columns]
    
    for column in connection_columns:
        # determine maximum value for the fabric
        core_sw_df['max'] = core_sw_df.groupby(by=['Fabric_name', 'Fabric_label'])[column].transform('max')
        # drop rows for which parameter is less then maximum value
        mask_max = core_sw_df[column] == core_sw_df['max']
        core_sw_df = core_sw_df.loc[mask_max].copy()
    # tag core switches
    core_sw_df['Core_switch_note'] = 'core_switch'
    return core_sw_df


def find_max_fabric_fos(sw_connection_statistics_df, pattern_dct):
    """Function to find the most recent FOS version in the Fabric"""

    # remove excessive symbols from FOS version
    # sw_connection_statistics_df['FOS_version_clean'] = sw_connection_statistics_df['FOS_version'].str.extract('v?(\d+\.\d+\.\d+[a-z]?\d?)')
    sw_connection_statistics_df['FOS_version_clean'] = sw_connection_statistics_df['FOS_version'].str.extract(pattern_dct['fos'])
    # fill nan values with char 0 to find maximum
    sw_connection_statistics_df['FOS_version_clean'].fillna('0', inplace=True)
    # find most recent FOS veriosn for each fabric
    sw_connection_statistics_df['FOS_fabric_max'] = \
        sw_connection_statistics_df.groupby(by=['Fabric_name', 'Fabric_label'])['FOS_version_clean'].transform('max')
    # remove max FOS version from fabric summary rows
    mask_fos_na = sw_connection_statistics_df['FOS_version'].isna()
    sw_connection_statistics_df.loc[mask_fos_na, 'FOS_fabric_max'] = np.nan
    return sw_connection_statistics_df


def add_notes(sw_connection_statistics_df):
    """Function to add notes if Principal switch is not in core and
    if Principal FOS version is not recent in the fabic"""

    # note if principal switch is not core switch
    mask_principal = sw_connection_statistics_df['switchRole'] == 'Principal'
    mask_not_core = sw_connection_statistics_df['Core_switch_note'].isna()
    sw_connection_statistics_df.loc[mask_principal & mask_not_core, 'Principal_core_note'] = 'principal_not_in_core'
    
    # note if principal FOS version is not recent in the fabric
    mask_fos_not_max = sw_connection_statistics_df['FOS_version_clean'] != sw_connection_statistics_df['FOS_fabric_max']
    sw_connection_statistics_df.loc[mask_principal & mask_fos_not_max, 'Principal_fos_note'] = 'principal_fos_not_recent'
    return sw_connection_statistics_df

"""Module to get list of switches and VC modulues to find it pairs later"""

import numpy as np
import utilities.dataframe_operations as dfop


def create_sw_brocade_dataframe(switch_params_aggregated_df):
    """Function to filter Brocade swithes for which pair switch need to be found"""
    
    mask_valid_fabric = ~switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['x', '-']).any(axis=1)
    switch_columns = ['configname', 'Fabric_name', 'Fabric_label', 'Device_Location', 
                       'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 
                       'switchType', 'ModelName', 'switchMode', 'LS_type_report']
    switch_pair_brocade_df = switch_params_aggregated_df.loc[mask_valid_fabric,switch_columns].copy()
    return switch_pair_brocade_df


def create_sw_npv_dataframe(portshow_aggregated_df):
    """Function to filter NPV connected switches except Brocade (VC, CISCO) for which pair VC module or switch need to be found"""
    
    mask_valid_fabric = ~portshow_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['x', '-']).any(axis=1)
    mask_npiv = portshow_aggregated_df['Connected_NPIV'] == 'yes'
    mask_vc_cisco = portshow_aggregated_df['deviceSubtype'].isin(['VC FC', 'VC FLEX', 'CISCO'])
    vc_cisco_columns = ['Fabric_name', 'Fabric_label', 'Device_Host_Name', 'NodeName', 'deviceType', 'deviceSubtype']
    vc_cisco_pair_df = portshow_aggregated_df.loc[mask_valid_fabric & mask_npiv & mask_vc_cisco, vc_cisco_columns].drop_duplicates().copy()
    # if aliases used for VC ports names are grouped
    vc_cisco_pair_df['Device_Host_Name'] = vc_cisco_pair_df.groupby(
        by=['Fabric_name', 'Fabric_label', 'NodeName'])['Device_Host_Name'].transform(
            lambda names: ', '.join(sorted(names))
        )
    vc_cisco_pair_df.drop_duplicates(inplace=True)

    vc_cisco_pair_df.rename(columns={'Device_Host_Name': 'switchName', 'NodeName': 'switchWwn'}, inplace=True)
    
    # add switchType, switchMode, configname columns for run function to find pair switch (pair candidate must have same switcType and switchMode)
    vc_cisco_pair_df['switchType'] = vc_cisco_pair_df['deviceSubtype']
    vc_cisco_pair_df['switchType'].replace(to_replace={'VC FC': 501, 'VC FLEX': 502, 'CISCO': 503, 'HUAWEI': 504}, inplace=True)
    
    vc_cisco_pair_df['switchMode'] = vc_cisco_pair_df['deviceSubtype']
    vc_cisco_pair_df['switchMode'].replace(to_replace={'VC FC': 'Access Gateway Mode', 'VC FLEX': 'Access Gateway Mode', 'CISCO': 'NPV', 'HUAWEI': 'NPV'}, inplace=True)
    
    vc_cisco_pair_df['configname'] = np.nan
    vc_cisco_pair_df['switchWwn_pair'] = np.nan
    return vc_cisco_pair_df




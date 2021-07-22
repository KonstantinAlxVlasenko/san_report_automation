"""Module with auxiliary functions to get zoning configuration statistics"""

import numpy as np
import pandas as pd
from common_operations_dataframe import count_frequency


def active_vs_configured_ports(portshow_zoned_aggregated_df, configuration_type):
    """Function to count total active device ports vs number of unzoned ports or
    ports without aliases depending from configuration_type parameter. 
    Function used in alias and effective zoning cfg statistics calculations"""

    # switch and vc ports should not be included zoning configuration 
    mask_device_ports = ~portshow_zoned_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    mask_online = portshow_zoned_aggregated_df['portState'] == 'Online'
    # to avoid device port duplication Native mode switches should be taken into account
    mask_native_mode = portshow_zoned_aggregated_df['switchMode'] == 'Native'
    # to drop unused fabrics
    mask_valid_fabric = portshow_zoned_aggregated_df['Fabric_name'] != 'x'
    
    portshow_zoned_aggregated_cp_df = \
        portshow_zoned_aggregated_df.loc[mask_online & mask_device_ports & mask_native_mode & mask_valid_fabric].copy()
    # mask shows devices not in effective configuration
    mask_not_effective = portshow_zoned_aggregated_cp_df['cfg_type'] != 'effective'
    # mask shows device ports without alias
    mask_no_alias = portshow_zoned_aggregated_cp_df['alias'].isna()
    # mask shows device connected to the port
    mask_device_type = portshow_zoned_aggregated_cp_df['deviceType'].notna()
    
    # create new column filled with 'Total_device_ports' value for all device ports
    portshow_zoned_aggregated_cp_df['Total_device_ports'] = \
        portshow_zoned_aggregated_cp_df['deviceType'].where(portshow_zoned_aggregated_cp_df['deviceType'].isna(), 'Total_device_ports')
    # create new column filled with 'Total_unzoned_ports' value for ports which are not part of effective zoning configuration
    portshow_zoned_aggregated_cp_df['Total_unzoned_ports'] = \
        np.where(mask_not_effective & mask_device_type, 'Total_unzoned_ports', pd.NA)
    # create column filled with 'Total_noalias_ports' for ports which have no alias
    portshow_zoned_aggregated_cp_df['Total_no_alias_ports'] = \
        np.where(mask_no_alias & mask_device_type, 'Total_no_alias_ports', pd.NA)
        
    portshow_zoned_aggregated_cp_df.fillna(np.nan, inplace=True)
    
    if configuration_type == 'cfg_effective':
       summary_df = \
           count_frequency(portshow_zoned_aggregated_cp_df, count_columns=['Total_unzoned_ports', 'Total_device_ports'])
    elif configuration_type == 'alias':
        summary_df = count_frequency(portshow_zoned_aggregated_cp_df, count_columns=['Total_no_alias_ports', 'Total_device_ports'])
    
    return summary_df


def merge_df(df_lst, fillna_index, merge_on=['Fabric_name', 'Fabric_label']):
    """Function to join DataFrames from df_lst. And fill nan values with zeroes in all
    DataFrames till fillna_index (including). fillna_index is index of last DataFrame 
    in df_lst required to fill nan values"""

    merged_df = df_lst[0].copy()
    for i, df in enumerate(df_lst):
        if i != 0 and not df.empty:
            merged_df = merged_df.merge(df, how='outer', on=merge_on)
            if i == fillna_index:
                merged_df.fillna(0, inplace=True)

    return merged_df
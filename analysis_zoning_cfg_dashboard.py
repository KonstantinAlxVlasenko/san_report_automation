"""Module to create Effective zoning configuration summary statistics"""

import numpy as np
import pandas as pd


def cfg_dashborad(zonemember_statistics_df, portshow_zoned_aggregated_df, zoning_aggregated_df, alias_aggregated_df):
    """Main function to count statistics. Count statistics from DataFrames passed as parameters and
    merge all summary DataFrames into single DataFrame"""

    # filter out effective zones only 
    mask_zone_effective = zonemember_statistics_df['cfg_type'] == 'effective'
    mask_cfg_summary = zonemember_statistics_df['zone'].isna()
    # DataFrame with information for each zone (zonelevel)
    zonelevel_statistics_effective_df = zonemember_statistics_df.loc[mask_zone_effective & ~mask_cfg_summary].copy()
    # DataFrame with summary for all zones in Fabric (cfg level)
    cfglevel_statistics_effective_df = zonemember_statistics_df.loc[mask_zone_effective & mask_cfg_summary].copy()

    # count quantity of each zone type (all zones, lsan and peer zones)
    zone_type_columns = ['Fabric_name', 'Fabric_label', 'zone_tag', 'lsan_tag', 'property']
    zone_type_columns = [column for column in zone_type_columns if column in cfglevel_statistics_effective_df.columns]
    zone_type_summary_df = cfglevel_statistics_effective_df.loc[:, zone_type_columns].copy()
    zone_type_summary_df.set_index(['Fabric_name', 'Fabric_label'], inplace=True)
    # summarize all zone types
    zone_type_summary_df.loc[('All', ''), :] = zone_type_summary_df.sum(numeric_only=True, axis=0)
    zone_type_summary_df.reset_index(inplace=True)

    # count qunatity of zones with each type of note (no_target, no_initiator, wwnn_zones, etc)
    zonelevel_statistics_effective_df['zone_Wwnn_tag'] = \
        np.where(zonelevel_statistics_effective_df['Wwnn'] > 0, 'zone_Wwnn_tag', pd.NA)
    zone_notes_summary_df = \
        count_summary(zonelevel_statistics_effective_df, count_columns=['Target_Initiator_note', 'Target_model_note', 'zone_Wwnn_tag'])

    # count device ports in zoning configuration for each ports state 
    # (local, remote_imported, remote_na, remote_initializing, absent, remote_configured)
    port_status_columns = ['Fabric_name', 'Fabric_label', 'alias_member', 'PortName', 'Fabric_device_status']
    mask_effective = zoning_aggregated_df['cfg_type'] =='effective'
    zoned_ports_status_df = zoning_aggregated_df.loc[mask_effective, port_status_columns].copy()
    # each PortWwnn (device port) is counted only once
    zoned_ports_status_df.drop_duplicates(subset=port_status_columns, inplace=True)
    zoned_ports_status_summary_df = count_summary(zoned_ports_status_df, count_columns=['Fabric_device_status'])

    # count active device ports in Fabric
    portshow_zoned_aggregated_cp_df = portshow_zoned_aggregated_df.copy()
    # switch and vc ports should not be included zoning configuration 
    mask_device_ports = ~portshow_zoned_aggregated_df['deviceType'].isin(['SWITCH', 'VC'])
    mask_online = portshow_zoned_aggregated_df['portState'] == 'Online'
    # to avoid device port duplication Native mode switches should be taken into account
    mask_native_mode = portshow_zoned_aggregated_df['switchMode'] == 'Native'
    portshow_zoned_aggregated_cp_df = portshow_zoned_aggregated_df.loc[mask_online & mask_device_ports & mask_native_mode].copy()
    # create new column filled with 'Total_device_ports' value for all device ports
    portshow_zoned_aggregated_cp_df['Total_device_ports'] = \
        portshow_zoned_aggregated_cp_df['deviceType'].where(portshow_zoned_aggregated_cp_df['deviceType'].isna(), 'Total_device_ports')
    zoned_vs_total_ports_summary_df = count_summary(portshow_zoned_aggregated_cp_df, count_columns=['Total_device_ports'])

    # count mean, max, min values for alias qunatity and active ports quantity in zones for each Fabric
    # zonemember might be presented as alias, Wwnp or Domain Index
    alias_count_columns = ['zonemember_alias', 'zonemember_wwn', 'zonemember_domain_portindex']
    alias_count_columns = [column for column in alias_count_columns if column in zonelevel_statistics_effective_df.columns]
    # sum all "aliases" for each zone
    zonelevel_statistics_effective_df['Total_alias'] = zonelevel_statistics_effective_df[alias_count_columns].sum(axis=1)
    # count statistics for each Fabric
    count_columns_dct = {'Total_alias': 'alias_per_zone', 'Total_zonemembers_active': 'active_ports_per_zone'}
    alias_vs_active_ports_per_zone_df = find_mean_max_min(zonelevel_statistics_effective_df, count_columns = count_columns_dct)

    # count mean, max, min values for defined ports, active ports number and number zones alias applied in in aliases for each Fabric
    mask_alias_effective = alias_aggregated_df['cfg_type'] == 'effective'
    alias_effective_df = alias_aggregated_df.loc[mask_alias_effective].copy()
    # defined and active ports number for each zonemember (alias) were calculated in advance
    # thus each alias should be counted only once
    alias_effective_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'zone_member'], inplace=True)
    count_columns_dct = {'ports_per_alias': 'ports_per_alias', 'active_ports_per_alias': 'active_ports_per_alias', 
                            'zone_number_alias_used_in': 'zone_number_alias_used_in'}
    alias_ports_vs_zone_usage_df = find_mean_max_min(alias_effective_df, count_columns = count_columns_dct)

    # merge all summary DataFrames into one
    active_cfg_statistics_df = zone_type_summary_df 
    summary_lst = [zone_notes_summary_df, zoned_ports_status_summary_df, zoned_vs_total_ports_summary_df, alias_vs_active_ports_per_zone_df, alias_ports_vs_zone_usage_df]
    for df in summary_lst:
        if not df.empty:
            active_cfg_statistics_df = active_cfg_statistics_df.merge(df, how='left', on=['Fabric_name', 'Fabric_label'])

    return active_cfg_statistics_df


def count_summary(df, count_columns: list, group_columns = ['Fabric_name', 'Fabric_label']):
    """Auxiliary function to count values in groups for columns in count_columns"""
        
    index_lst = [df[column] for column in group_columns if column in df.columns]
    summary_df = pd.DataFrame()
    for column in count_columns:
        if column in df.columns and df[column].notna().any():
            current_df = pd.crosstab(index=index_lst, columns=df[column], margins=True)
            current_df.drop(columns=['All'], inplace=True)
            if summary_df.empty:
                summary_df = current_df.copy()
            else:
                summary_df = summary_df.merge(current_df, how='left', on=group_columns)
                
    summary_df.reset_index(inplace=True)
                
    return summary_df


def find_mean_max_min(df, count_columns: dict, group_columns = ['Fabric_name', 'Fabric_label']):
    """Auxiliary function to find mean, max and min values in groups for columns in count_columns
    and rename columns with corresponding keys from count_columns"""
    
    summary_df = pd.DataFrame()
    for count_column, rename_column in count_columns.items():
        current_df = df.groupby(by = group_columns)[count_column].agg( ['mean', 'max', 'min'])
        current_df['mean'] = current_df['mean'].round(1)
        rename_dct = {}
        for column in current_df.columns:
            rename_dct[column] = rename_column + '_' + column
        current_df.rename(columns=rename_dct, inplace=True)
        current_df.reset_index(inplace=True)
        if summary_df.empty:
            summary_df = current_df.copy()
        else:
            summary_df = summary_df.merge(current_df, how='left', on=group_columns)
            
    return summary_df


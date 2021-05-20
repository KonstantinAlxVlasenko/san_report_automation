"""Module to create zoning statistics related DataFrames"""


import numpy as np
import pandas as pd

from analysis_zoning_statistics_modify import modify_zoning
from analysis_zoning_statistics_notes import note_zonemember_statistics
from common_operations_dataframe import dataframe_fillna

# from common_operations_filesystem import load_data, save_data, save_xlsx_file

invalid_zone_tags = ['no_initiator', 'no_target', 'no_target, no_initiator', 'no_target, several_initiators']

def zonemember_statistics(zoning_aggregated_df, report_data_lst):
    """Main function to create zonemembers statistics"""

    zoning_modified_df, zoning_duplicated_df, zoning_pairs_df = modify_zoning(zoning_aggregated_df)

    # get statistics DataFrames for zone and cfgtype level statistics
    zonemember_zonelevel_stat_df = count_zonemember_statistics(zoning_modified_df)
    zonemember_cfgtypelevel_stat_df = count_zonemember_statistics(zoning_modified_df, zone=False)
    zonemember_zonelevel_stat_df.reset_index(inplace=True)
    # drop duplicated All row
    zonemember_zonelevel_stat_df.drop(zonemember_zonelevel_stat_df.index[zonemember_zonelevel_stat_df['Fabric_name'] == 'All'], inplace = True)
    zonemember_cfgtypelevel_stat_df.reset_index(inplace=True)
    # add defined and actual wwn number for each zone
    zone_wwn_number_df = defined_actual_wwn_number(zoning_aggregated_df, df_type='zone')
    
    zonemember_zonelevel_stat_df = zonemember_zonelevel_stat_df.merge(zone_wwn_number_df, how='left', 
                                                                        on=['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone'])

    # if zone is empty (no active devices) fill device_type columns (target/initiator) with zeroes
    device_type_columns = [column for column in zonemember_zonelevel_stat_df.columns if ('initiator' in column.lower() or 'target' in column.lower())]

    mask_empty_zone = zonemember_zonelevel_stat_df['Total_zonemembers_active'] == 0
    zonemember_zonelevel_stat_df.loc[mask_empty_zone, device_type_columns] = \
        zonemember_zonelevel_stat_df.loc[mask_empty_zone, device_type_columns].fillna(0)

    # add list of zone pairs to each zone in statistics
    zoning_paired_columns = ['Fabric_name', 'Fabric_label',  'cfg_type',  'zone',
                                'Zone_name_device_names_ratio', 'Zone_name_device_names_related', 
                                'All_devices_multiple_fabric_label_connection', 'zone_paired',
                                'Zone_and_Pairzone_names_ratio', 'Zone_and_Pairzone_names_related']
    zonemember_zonelevel_stat_df = dataframe_fillna(zonemember_zonelevel_stat_df, zoning_pairs_df, 
                                                        join_lst=zoning_paired_columns[:4], filled_lst=zoning_paired_columns[4:])

    # add list of identical (duplicated) zones to each zone in statistics
    zoning_duplicated_columns = ['Fabric_name', 'Fabric_label',  'cfg',  'cfg_type',  'zone', 'zone_duplicated']
    zonemember_zonelevel_stat_df = dataframe_fillna(zonemember_zonelevel_stat_df, zoning_duplicated_df, 
                                                        join_lst=zoning_duplicated_columns[:-1], filled_lst=[zoning_duplicated_columns[-1]])


    # add 'Target_Initiator'and 'Target_model' notes to zonemember_zonelevel_stat_df DataFrame
    zonemember_zonelevel_stat_df = note_zonemember_statistics(zonemember_zonelevel_stat_df)
    # add note if zone is not used in effective configuration
    grp_columns = ['Fabric_name', 'Fabric_label', 'zone']
    zonemember_zonelevel_stat_df['Effective_cfg_usage_note'] = zonemember_zonelevel_stat_df.groupby(by=grp_columns)['cfg_type'].transform(lambda x: ', '.join(set(x)))
    mask_non_effective = ~zonemember_zonelevel_stat_df['Effective_cfg_usage_note'].str.contains('effective')
    zonemember_zonelevel_stat_df['Effective_cfg_usage_note'] = np.where(mask_non_effective, 'unused_zone', pd.NA)
    zonemember_zonelevel_stat_df['Effective_cfg_usage_note'].fillna(np.nan, inplace=True)


    # TO_REMOVE rellocated up
    # # add list of identical (duplicated) zones to each zone in statistics
    # zoning_duplicated_columns = ['Fabric_name', 'Fabric_label',  'cfg',  'cfg_type',  'zone', 'zone_duplicated']
    # zonemember_zonelevel_stat_df = dataframe_fillna(zonemember_zonelevel_stat_df, zoning_duplicated_df, 
    #                                                     join_lst=zoning_duplicated_columns[:-1], filled_lst=[zoning_duplicated_columns[-1]])
    # # add list of zone pairs to each zone in statistics
    # zoning_paired_columns = ['Fabric_name', 'Fabric_label',  'cfg_type',  'zone', 
    #                             'All_devices_multiple_fabric_label_connection', 'zone_paired']
    # zonemember_zonelevel_stat_df = dataframe_fillna(zonemember_zonelevel_stat_df, zoning_pairs_df, 
    #                                                     join_lst=zoning_paired_columns[:4], filled_lst=zoning_paired_columns[4:])


    # remove duplicated and paired zones list if current zone is non-working zone (duplication of working zones only required)
    # list of duplicated zones is removed but duplication tag remains  
    mask_valid_zone = ~zonemember_zonelevel_stat_df['Target_Initiator_note'].isin(['no_target', 'no_initiator', 'no_target, no_initiator', 'no_target, several_initiators'])
    columns = ['zone_duplicated', 'zone_paired', 
                'Zone_name_device_names_ratio', 'Zone_name_device_names_related',
                'Zone_and_Pairzone_names_ratio', 'Zone_and_Pairzone_names_related']
    zonemember_zonelevel_stat_df[columns] = zonemember_zonelevel_stat_df[columns].where(mask_valid_zone)

    # TO_REMOVE 
    # zonemember_zonelevel_stat_df['zone_duplicated'] = zonemember_zonelevel_stat_df['zone_duplicated'].where(mask_valid_zone)
    # zonemember_zonelevel_stat_df['zone_paired'] = zonemember_zonelevel_stat_df['zone_paired'].where(mask_valid_zone)

    # sort values
    zonemember_zonelevel_stat_df.sort_values(by=['Fabric_name', 'Fabric_label', 'cfg_type', 'cfg', 'zone'],
                                                ascending=[True, True, False, True, True], inplace=True, ignore_index=True)
    # concatenate both statistics
    zonemember_statistics_df = pd.concat([zonemember_zonelevel_stat_df, zonemember_cfgtypelevel_stat_df], ignore_index=True)

    return zonemember_statistics_df, zonemember_zonelevel_stat_df


def count_zonemember_statistics(zoning_modified_deafult_df, zone=True):
    """
    Auxiliary function to count statistics for list of columns of 
    modified zoning_modified_df DataFrame.
    Zone True - Zone level statistics, Zone False - Cfg type level statistics
    """

    # column names for which statistics is counted for
    columns_lst = ['zone_tag', 'qos_tag', 'lsan_tag', 'tdz_tag', 'zone_duplicated_tag', 'zone_paired_tag', 
                    'Fabric_device_status', 'peerzone_member_type',
                    'deviceType', 'deviceSubtype',
                    'Device_type', 'Wwn_type', 'Wwnp_duplicated', 'zone_member_type']

    wwnn_duplicates_columns = ['Fabric_name', 'Fabric_label', 
                                'cfg', 'cfg_type', 'zone', 
                                'zone_member', 'alias_member']

    wwnp_duplicated_columns = ['Fabric_name', 'Fabric_label', 
                                'cfg', 'cfg_type', 'zone', 'PortName']

    columns_lst = [column for column in columns_lst 
                    if column in zoning_modified_deafult_df.columns 
                    and zoning_modified_deafult_df[column].notna().any()]

    # list to merge diffrenet parameters statistics into single DataFrame
    merge_lst = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone']

    # aggregated zoning_statistics DataFrame is initially empty
    zone_aggregated_statistics_df = pd.DataFrame()
    for column in columns_lst:
        zoning_modified_df = zoning_modified_deafult_df.copy()
        if column == 'Wwn_type':
            zoning_modified_df.drop_duplicates(subset=wwnn_duplicates_columns, inplace=True)
        if column == 'zone_member_type':
            zoning_modified_df.drop_duplicates(subset=wwnn_duplicates_columns[:-1], inplace=True)
            mask_property_member = zoning_modified_df['peerzone_member_type'] == 'property'
            zoning_modified_df = zoning_modified_df.loc[~mask_property_member]
        if column == 'Wwnp_duplicated':
            zoning_modified_df.drop_duplicates(subset=wwnp_duplicated_columns, inplace=True)
        # list of series(columns) grouping performed on
        index_lst = [zoning_modified_df.Fabric_name, zoning_modified_df.Fabric_label,
                    zoning_modified_df.cfg, zoning_modified_df.cfg_type,
                    zoning_modified_df.zone]
        # list to merge diffrenet parameters statistics into single DataFrame
        merge_lst = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone']
        
        # for cfg type level statistics drop zone
        # from lists grouping and merging on
        if not zone:
            index_lst = index_lst[:-1]
            merge_lst = merge_lst[:-1]

        # count statistics for each column from columns_lst
        column_statistics_df = pd.crosstab(index = index_lst,
                                    columns = zoning_modified_df[column],
                                    margins=True)
        # drop All column for all statistics except for device class
        if column == 'deviceType':
            column_statistics_df.rename(columns={'All': 'Total_zonemembers_active'}, inplace=True)
        elif column == 'Fabric_device_status':
            column_statistics_df.rename(columns={'All': 'Total_zonemembers'}, inplace=True)
        elif column == 'Wwn_type' and 'Wwnn' not in column_statistics_df.columns:
            column_statistics_df.drop(columns=['All'], inplace=True)
            # if now wwnn used during zone configuration then add Wwnn column with zeroes
            column_statistics_df['Wwnn'] = 0
        else:
            column_statistics_df.drop(columns=['All'], inplace=True)

        # add Initiator total column  (sum for all initiators) for Device_type column
        if column == 'Device_type':
            initiator_columns = ['Physical Initiator', 'NPIV Initiator']
            initiator_columns = [column for column in initiator_columns if column in column_statistics_df.columns]
            column_statistics_df['Total Initiators'] = column_statistics_df[initiator_columns].sum(axis=1)

        # for the first iteration (aggregated DataFrame is empty)
        if zone_aggregated_statistics_df.empty:
            # just take Fabric_device_status statistics
            zone_aggregated_statistics_df = column_statistics_df.copy()
        else:                
            # for the rest statistics DataFrames perform merge operation with zonememeber_statistic aggregated DataFrame
            zone_aggregated_statistics_df = zone_aggregated_statistics_df.merge(column_statistics_df, how='left', on=merge_lst)

    # fill all columns with None values except for port type columns with 0
    # port type might be empty if nscamshow is empty (fabric includes single switch only)
    df_columns = zone_aggregated_statistics_df.columns.to_list()
    fillna_columns = [column for column in df_columns if not (('initiator' in column.lower()) or ('target' in column.lower()))]
    zone_aggregated_statistics_df[fillna_columns] = zone_aggregated_statistics_df[fillna_columns].fillna(0)

    return zone_aggregated_statistics_df


def defined_actual_wwn_number(aggregated_df, df_type='alias'):
    """
    Function to count defined vs actual wwn number in zone or alias.
    Checks if Wwnn is 'unpacked' into more then one Wwnp
    """
    
    group_columns = ['Fabric_name',	'Fabric_label', 'zone_member', 'alias_member']
    if df_type == 'zone':
        group_columns = [*group_columns[:2], *['cfg', 'cfg_type', 'zone'], *group_columns[3:]]
        
    # count defined and actual wwnp numbers in each zone or alias
    duplicates_free_df = aggregated_df.drop_duplicates(subset=group_columns).copy()
    wwn_number_defined_sr = duplicates_free_df.groupby(group_columns[:-1]).alias_member.count()
    wwn_number_actual_sr = aggregated_df.groupby(group_columns[:-1]).alias_member.count()
    wwn_unpack_sr = wwn_number_actual_sr - wwn_number_defined_sr
    
    # add defined and actual wwnp numbers to main DataFrame
    wwn_unpack_df = pd.DataFrame(wwn_unpack_sr)
    wwn_unpack_df.rename(columns={'alias_member': 'Wwnn_to_Wwnp_number_unpacked'}, inplace=True)
    
    return wwn_unpack_df #wwn_number_df















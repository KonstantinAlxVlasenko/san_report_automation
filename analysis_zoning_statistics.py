"""Module to create zoning statistics related DataFrames"""

import numpy as np
import pandas as pd


def zonemember_statistics(zoning_aggregated_df):
    """Main function to create zonemembers statistics"""

    statistics_columns_lst = ['deviceType', 'deviceSubtype', 'Device_type', 'Wwn_type'] 

    # to count zonemeber stitistics it is required to make
    # changes in zoning_aggregated_df DataFrame
    zoning_modified_df = zoning_aggregated_df.copy()
    # All classes of servers are considered to be SRV class
    zoning_modified_df.deviceType.replace(to_replace={'BLADE_SRV': 'SRV'}, inplace=True)
    # deviceType transformed to be combination if device class and device type
    zoning_modified_df.deviceSubtype = zoning_modified_df['deviceType'] + ' ' + zoning_modified_df['deviceSubtype']
    # servers device type is not important for zonemember analysys
    mask_srv = zoning_modified_df.deviceType.str.contains('SRV', na=False)
    zoning_modified_df.deviceSubtype = np.where(mask_srv, np.nan, zoning_modified_df.deviceSubtype)
    """
    We are interested to count connected devices statistic only.
    Connected devices are in the same fabric with the switch which 
    zoning configurutaion defined in (local) or imported to that fabric
    in case of LSAN zones (imported).
    Ports with status remote_na, initializing and configured considered to be
    not connected (np.nan) and thus not taking into acccount.
    But device status for not connected ports is reflected in zonemember statistics.
    """  
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'imported'])
    zoning_modified_df[statistics_columns_lst] = \
        zoning_modified_df[statistics_columns_lst].where(mask_connected, pd.Series((np.nan, np.nan)), axis=1)

    # get statistice DataFrames for zone and cfgtype level statistics
    zonemember_zonelevel_stat_df = count_zonemember_statistics(zoning_modified_df)
    zonemember_cfgtypelevel_stat_df = count_zonemember_statistics(zoning_modified_df, zone=False)
    zonemember_zonelevel_stat_df.reset_index(inplace=True)
    # drop duplicated All row
    zonemember_zonelevel_stat_df.drop(zonemember_zonelevel_stat_df.index[zonemember_zonelevel_stat_df['Fabric_name'] == 'All'], inplace = True)
    zonemember_cfgtypelevel_stat_df.reset_index(inplace=True)
    # add defined and actual wwn number for each zone
    zone_wwn_number_df = defined_actual_wwn_number(zoning_aggregated_df, df_type='zone')
    zonemember_zonelevel_stat_df = zonemember_zonelevel_stat_df.merge(zone_wwn_number_df, how='left', on=['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone'])
    # add 'Target_Initiator'and 'Target_model' notes to zonemember_zonelevel_stat_df DataFrame
    zonemember_zonelevel_stat_df = note_zonemember_statistics(zonemember_zonelevel_stat_df)
    # concatenate both statistics
    zonemember_statistics_df = pd.concat([zonemember_zonelevel_stat_df, zonemember_cfgtypelevel_stat_df], ignore_index=True)
        
    return zonemember_statistics_df, zonemember_zonelevel_stat_df


def count_zonemember_statistics(zoning_modified_df, zone=True):
    """
    Auxiliary function to count statistics for list of columns of 
    modified zoning_modified_df DataFrame.
    Zone True - Zone level statistics, Zone False - Cfg type level statistics
    """

    # column names for which statistics is counted for
    columns_lst = ['Fabric_device_status',
                'deviceType',
                'deviceSubtype',
                'Device_type',
                'Wwn_type'] 
    # list to merge diffrenet parameters statistics into single DataFrame
    merge_lst = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type','zone']
    # list of series(columns) grouping performed on
    index_lst = [zoning_modified_df.Fabric_name, zoning_modified_df.Fabric_label,
                zoning_modified_df.cfg, zoning_modified_df.cfg_type,
                zoning_modified_df.zone]
    # for cfg type level statistics drop zone
    # from lists grouping and merging on
    if not zone:
        index_lst = index_lst[:-1]
        merge_lst = merge_lst[:-1]
    # aggregated zoning_statistics DataFrame is initially empty
    zone_aggregated_statistics_df = pd.DataFrame()
    for column in columns_lst:
        # count statistics for each column from columns_lst
        column_statistics_df = pd.crosstab(index = index_lst,
                                    columns = zoning_modified_df[column],
                                    margins=True)
        # drop All column for all statistics except for device class
        if column == 'deviceType':
            column_statistics_df.rename(columns={'All': 'Total_connected_zonemembers'}, inplace=True)
        elif column == 'Fabric_device_status':
            column_statistics_df.rename(columns={'All': 'Total_zonemembers'}, inplace=True)
        else:
            column_statistics_df.drop(columns=['All'], inplace=True)

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
    Checks if Wwnn is 'unpacked' into more then onle Wwnp
    """
    
    group_columns = ['Fabric_name',	'Fabric_label', 'zone_member', 'alias_member']
    if df_type == 'zone':
        group_columns = [*group_columns[:2], *['cfg', 'cfg_type', 'zone'], *group_columns[3:]]
        
    # count defined and actual wwnp numbers in each zone or alias
    duplicates_free_df = aggregated_df.drop_duplicates(subset=group_columns).copy()
    wwn_number_defined_sr = duplicates_free_df.groupby(group_columns[:-1]).alias_member.count()
    wwn_number_actual_sr = aggregated_df.groupby(group_columns[:-1]).alias_member.count()
    
    # add dined and actual wwnp numbers to main DataFrame
    wwn_number_defined_df = pd.DataFrame(wwn_number_defined_sr)
    wwn_number_actual_df = pd.DataFrame(wwn_number_actual_sr)
    
    wwn_number_defined_df.rename(columns={'alias_member': 'Wwn_number_defined'}, inplace=True)
    wwn_number_actual_df.rename(columns={'alias_member': 'Wwn_number_actual'}, inplace=True)
    wwn_number_df = wwn_number_defined_df.merge(wwn_number_actual_df, how='left', on=group_columns[:-1])
    
    return wwn_number_df


def note_zonemember_statistics(zonemember_zonelevel_stat_df):
    """
    Function to verify zone content from target_initiator number (no targets, no initiators, 
    neither target nor initiator (empty zone), zone contains more than one initiator) and
    target models, class (libraries and storages or different storage models in one zone)
    point of view.
    """

    zonemember_stat_notes_df =  zonemember_zonelevel_stat_df.copy()
    # add device classes to the statistics DataFrame if some of them are missing
    # and fill columns with zeroes
    columns_lst = zonemember_stat_notes_df.columns.to_list()
    target_initiators_lst = ['SRV', 'STORAGE', 'LIB']
    add_columns = [column for column in target_initiators_lst if column not in columns_lst]
    if add_columns:
        zonemember_stat_notes_df = zonemember_stat_notes_df.reindex(columns=[*columns_lst, *add_columns])
        zonemember_stat_notes_df[add_columns] = zonemember_stat_notes_df[add_columns].fillna(0)
    # create target number summary column with quantity for each zone
    zonemember_stat_notes_df['STORAGE_LIB'] = zonemember_stat_notes_df['STORAGE'] + zonemember_stat_notes_df['LIB']
    # target_initiator zone check
    zonemember_stat_notes_df['Target_Initiator_note'] =\
        zonemember_stat_notes_df.apply(lambda series: target_initiator_note(series), axis=1)
    zonemember_stat_notes_df.drop(columns=['STORAGE_LIB'], inplace=True)

    # find storage models columns if they exist (should be at least one storage in fabric)
    storage_model_columns = [column for column in columns_lst if 'storage' in column.lower()]
    if len(storage_model_columns) > 1:
        storage_model_columns.remove('STORAGE')

    """
    Explicitly exclude replication zones (considered to be correct and presence of different storage models
    is permitted by zone purpose) and zones without initiator (condsidered to be incorrect).
    No target and empty zones are excluded by defenition (target ports) and considered to be incorrect.
    All incorrect zones are out of scope of verification if different storage models or 
    library and storage presence in a single zone
    """
    mask_exclude_zone = ~zonemember_stat_notes_df['Target_Initiator_note'].isin(['replication_zone', 'no_initiator'])
    # check if zone contains storages of different models
    if len(storage_model_columns) > 1:
        # zonemember_stat_notes_df['Storage_model_note'] = np.nan
        mask_different_storages = (zonemember_stat_notes_df[storage_model_columns] != 0).all(axis=1)
        zonemember_stat_notes_df['Storage_model_note'] = np.where(mask_exclude_zone & mask_different_storages, 'different_storages', pd.NA)
    else:
        zonemember_stat_notes_df['Storage_model_note'] = np.nan

    # zonemember_stat_notes_df['Storage_library_note'] = np.nan

    # check if zone contains storage and library in a single zone
    mask_storage_lib = (zonemember_stat_notes_df[['STORAGE', 'LIB']] != 0).all(axis=1)
    zonemember_stat_notes_df['Storage_library_note'] = np.where(mask_exclude_zone & mask_storage_lib, 'storage_library', pd.NA)
    # join both columns in a single column
    zonemember_stat_notes_df['Target_model_note'] = \
        zonemember_stat_notes_df[['Storage_model_note', 'Storage_library_note']].apply(lambda x: x.str.cat(sep=', ') \
            if x.notna().any() else np.nan, axis=1)
    zonemember_stat_notes_df.drop(columns=['Storage_model_note', 'Storage_library_note'], inplace=True)
    # drop columns if all values are NA
    zonemember_stat_notes_df.dropna(how='all', axis='columns', inplace=True)
    # check if there are SRV, STORAGE and LIB devices classes in zones
    # if none of the zones contain any of device class then drop this class from statistcics DataFRame
    for column in target_initiators_lst:
        if (zonemember_stat_notes_df[column] == 0).all():
            zonemember_stat_notes_df.drop(columns=column, inplace=True)

    return zonemember_stat_notes_df


def target_initiator_note(series):
    """
    Auxiliary function for 'note_zonemember_statistic' function 
    to verify zone content from target_initiator number point of view.
    """
    
    # if there are no local or imported zonemembers in fabric of zoning config switch
    # current zone is empty (neither actual initiators nor targets are present)
    if series['Total_connected_zonemembers'] == 0:
        return 'empty_zone'
    # if all zonememebrs are storages with local or imported device status 
    # and no asent devices then zone considered to be replication zone 
    if series['STORAGE'] == series['Total_zonemembers'] and series['STORAGE']>1:
        return 'replication_zone'
    """
    If there are no actual server in the zone and number of defined zonemembers exceeds
    local or imported zonemebers (some devices are absent or not in the fabric of
    zoning configuration switch) then it's not a replication zone and considered to be
    initiator's less zone
    """
    if series['SRV'] == 0 and series['Total_zonemembers'] > series['Total_connected_zonemembers']:
        if series['STORAGE_LIB'] > 0:
            return 'no_initiator'
    # if zone contains initiator(s) but not targets then zone considered to be target's less zone
    if series['SRV'] >= 1 and series['STORAGE_LIB'] == 0:
            return 'no_target'
    # if zone contains more then one initiator and target(s) then initiator number exceeds threshold
    if series['SRV'] > 1:
        return 'several_initiators'
    
    return np.nan
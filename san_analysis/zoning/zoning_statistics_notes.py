"""Module to add notes to zoning statistics DataFrame"""


import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop


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
    unique_names_lst = [device_type + ' Unique name' for device_type in target_initiators_lst]
    add_columns_lst = [*target_initiators_lst, *unique_names_lst]
    add_columns = [column for column in add_columns_lst if column not in columns_lst]
    if add_columns:
        zonemember_stat_notes_df = zonemember_stat_notes_df.reindex(columns=[*columns_lst, *add_columns])
        zonemember_stat_notes_df[add_columns] = zonemember_stat_notes_df[add_columns].fillna(0)
    # create target number summary column with quantity for each zone
    zonemember_stat_notes_df['STORAGE_LIB'] = zonemember_stat_notes_df['STORAGE'] + zonemember_stat_notes_df['LIB']
    # target_initiator zone check
    zonemember_stat_notes_df['Target_Initiator_note'] =\
        zonemember_stat_notes_df.apply(lambda series: target_initiator_note(series), axis=1)
    zonemember_stat_notes_df.drop(columns=['STORAGE_LIB'], inplace=True)
    # add note if different storage models are in the same zone (except peer zones)
    zonemember_stat_notes_df = storage_model_note(zonemember_stat_notes_df)
    # check if zone contains storage and library in a single zone
    mask_storage_lib = (zonemember_stat_notes_df[['STORAGE', 'LIB']] != 0).all(axis=1)
    zonemember_stat_notes_df['Storage_library_note'] = np.where(mask_storage_lib, 'storage_library', pd.NA)
    # join both columns in a single column
    zonemember_stat_notes_df['Target_model_note'] = \
        zonemember_stat_notes_df[['Storage_model_note', 'Storage_library_note']].apply(lambda x: x.str.cat(sep=', ') \
            if x.notna().any() else np.nan, axis=1)
    zonemember_stat_notes_df.drop(columns=['Storage_model_note', 'Storage_library_note'], inplace=True)
    # drop columns if all values are NA
    zonemember_stat_notes_df.dropna(how='all', axis='columns', inplace=True)
    # check if there are SRV, STORAGE and LIB devices classes in zones
    # if none of the zones contain any of device class then drop this class from statistcics DataFrame
    for column in target_initiators_lst:
        if (zonemember_stat_notes_df[column] == 0).all():
            zonemember_stat_notes_df.drop(columns=column, inplace=True)

    if not 'Target_Initiator_note' in zonemember_stat_notes_df.columns:
        zonemember_stat_notes_df['Target_Initiator_note'] = np.nan

    # add mixed zone note
    if {'aliasmember_domain_portindex', 'aliasmember_wwn'}.issubset(zonemember_stat_notes_df.columns):
        mask_mixed_zone = (zonemember_stat_notes_df[['aliasmember_domain_portindex', 'aliasmember_wwn']] > 0).all(axis=1)
        zonemember_stat_notes_df.loc[mask_mixed_zone, 'Mixed_zone_note'] = 'mixed_wwn_di_zone'
    else:
        zonemember_stat_notes_df['Mixed_zone_note'] = np.nan

    # add pair_zone_note
    mask_device_connection = zonemember_stat_notes_df['All_devices_multiple_fabric_label_connection'] == 'Yes'
    
    if not 'zone_paired' in zonemember_stat_notes_df.columns:
        zonemember_stat_notes_df['zone_paired'] = np.nan
    
    mask_no_pair_zone = zonemember_stat_notes_df['zone_paired'].isna()
    # valid zones
    invalid_zone_tags = ['no_target', 'no_initiator', 'no_target, no_initiator', 'no_target, several_initiators']
    mask_valid_zone = ~zonemember_stat_notes_df['Target_Initiator_note'].isin(invalid_zone_tags)
    dfop.column_to_object(zonemember_stat_notes_df, 'Pair_zone_note')
    zonemember_stat_notes_df.loc[mask_valid_zone & mask_device_connection & mask_no_pair_zone, 'Pair_zone_note'] = 'pair_zone_not_found'
    return zonemember_stat_notes_df



def storage_model_note(zonemember_stat_notes_df):
    """Function to add note if different storage models are in the same zone (except peer zones).
    Explicitly exclude replication zones (presence of different storage models in the same zone
    is allowed due to replication purpose) and zones without initiator (condsidered to be incorrect).
    No target and empty zones are excluded by defenition (target ports) and considered to be incorrect.
    All incorrect zones are out of scope of verification if different storage models or 
    library and storage presence in a single zone"""


    # find storage models columns if they exist (should be at least one storage in fabric)
    storage_model_columns = [column for column in zonemember_stat_notes_df.columns 
                                if 'storage' in column.lower() and not column.lower() in ['storage', 'storage unique name']]

    mask_exclude_zone = ~zonemember_stat_notes_df['Target_Initiator_note'].isin(['replication_zone', 'no_initiator'])
    # check if zone contains storages of different models
    if len(storage_model_columns) > 1:
        mask_different_storages = (zonemember_stat_notes_df[storage_model_columns] != 0).sum(axis=1).gt(1)
        # peer zone can containt diffrent storage models
        if 'peer' in zonemember_stat_notes_df.columns:
            mask_not_peer = zonemember_stat_notes_df['peer'] == 0
            mask_different_storages_note = mask_exclude_zone & mask_different_storages & mask_not_peer
        else:
            mask_different_storages_note = mask_exclude_zone & mask_different_storages
        zonemember_stat_notes_df['Storage_model_note'] = np.where(mask_different_storages_note, 'different_storages', pd.NA)
    else:
        zonemember_stat_notes_df['Storage_model_note'] = np.nan
    # count storage models in zones
    zonemember_stat_notes_df['Storage_model_quantity'] = (zonemember_stat_notes_df[storage_model_columns] != 0).sum(axis=1)
    return zonemember_stat_notes_df


def target_initiator_note(series):
    """
    Auxiliary function for 'note_zonemember_statistic' function 
    to verify zone content from target_initiator number point of view.
    """

    # if there are no local or imported zonemembers in fabric of zoning config switch
    # current zone is empty (neither actual initiators nor targets are present)
    if series['Total_zonemembers_active'] == 0:
        return 'no_target, no_initiator'
    # if all zonememebrs are storages with local or imported device status, 
    # no absent devices and more then one storage system in zone then zone considered to be replication zone 
    if series['STORAGE'] == series['Total_zonemembers'] and series['STORAGE Unique name'] > 1:
        return 'replication_zone'
    """
    If there are no actual server in the zone and number of defined zonemembers exceeds
    local or imported zonemebers (some devices are absent or not in the fabric of
    zoning configuration switch) then it's not a replication zone and considered to be
    initiator's less zone
    """
    if series['SRV'] == 0 and series['Total Initiators'] == 0: # and series['Total_zonemembers'] > series['Total_zonemembers_active']:
        if series['STORAGE_LIB'] > 0:
            return 'no_initiator'
    # if zone contains initiator(s) but not targets then zone considered to be targetless zone
    if series['SRV'] == 1 and series['STORAGE_LIB'] == 0:
            return 'no_target'
    # if zone contains more then one initiator and no targets
    # and it's not a peerzone  then 'no_target, several_initiators' tag
    # if it's a peer zone then 'no_target' tag
    if (series['SRV'] > 1 or series['Total Initiators'] > 1) and series['STORAGE_LIB'] == 0:
        if 'peer' in series.index and 'property' in series.index:
            if series['peer'] == 0 and series['property'] == 0:
                return 'no_target, several_initiators'
            elif series['peer'] != 0 or series['property'] != 0:
                return 'no_target' 
        else:
            return 'no_target, several_initiators'
    # if zone contains more then one initiator and it's not a peerzone 
    # then initiator number exceeds threshold
    if series['SRV'] > 1 or series['Total Initiators'] > 1:
        if 'peer' in series.index and 'property' in series.index:
            if series['peer'] == 0 and series['peer'] == 0:
                return 'several_initiators'
        else:
            return 'several_initiators'
    return np.nan
"""Module to create zoning configuration related DataFrames"""

import pandas as pd
import numpy as np

from analysis_zoning_aggregation import zoning_aggregated
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_dataframe import dataframe_segmentation
from common_operations_servicefile import dct_from_columns


def zoning_analysis_main(switch_params_aggregated_df, portshow_aggregated_df, 
                            cfg_df, zone_df, alias_df, cfg_effective_df, fcrfabric_df, lsan_df,
                            report_columns_usage_dct, report_data_lst):
    """Main function to analyze zoning configuration"""
        
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['zoning_aggregated', 'alias_aggregated', 'zonemember_statistics', 'Зонирование', 'Псевдонимы', 'Зонирование_A&B']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, zoning_report_df, alias_report_df, zoning_compare_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['cfg', 'cfg_effective', 'zone', 'alias',
                           'switch_params_aggregated', 'switch_parameters', 'switchshow_ports', 'chassis_parameters', 
                            'portshow_aggregated', 'portcmd', 'fdmi', 'nscamshow', 'nsshow', 'blade_servers', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating zoning table'
        print(info, end =" ") 

        # aggregated tables
        zoning_aggregated_df, alias_aggregated_df \
            = zoning_aggregated(switch_params_aggregated_df, portshow_aggregated_df, 
                                    cfg_df, zone_df, alias_df, cfg_effective_df, fcrfabric_df, lsan_df, report_data_lst)

        zonemember_statistics_df, zonemember_zonelevel_stat_df = zonemember_statistics(zoning_aggregated_df)
        zoning_aggregated_df = statistics_to_agggregated_zoning(zoning_aggregated_df, zonemember_zonelevel_stat_df)
        # after finish display status
        status_info('ok', max_title, len(info))
        # report tables
        zoning_report_df = create_report(zoning_aggregated_df, data_names[3:4], report_columns_usage_dct, max_title)
        alias_report_df = create_report(alias_aggregated_df, data_names[4:5], report_columns_usage_dct, max_title)
        zoning_compare_report_df = compare_zone_config(zoning_report_df)
        # create list with partitioned DataFrames
        data_lst = [zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, zoning_report_df, alias_report_df, zoning_compare_report_df]
        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)

    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, zoning_report_df, alias_report_df \
            = verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [zoning_aggregated_df, alias_aggregated_df, zonemember_statistics_df, zoning_report_df, alias_report_df, zoning_compare_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return zoning_aggregated_df, alias_aggregated_df


def compare_zone_config(zoning_report_df):
    """
    Function to create comparision table (side by side) of A and B fabrics
    with valid zones of effective configuration only.
    """

    # list of invalid zones to remove from compare report
    invalid_zone_lst = ['нет инициатора', 'нет таргета', 'нет инициатора, нет таргета']
    mask_valid_zone = ~zoning_report_df['Примечание. Количество таргетов и инициаторов в зоне'].isin(invalid_zone_lst)
    # take effective configuration only if defined configuration(s) exist
    if 'Тип конфигурации' in zoning_report_df.columns:
        mask_effective = zoning_report_df['Тип конфигурации'] == 'effective'
        mask_valid_zone = mask_effective & mask_valid_zone

    zoning_valid_df = zoning_report_df.loc[mask_valid_zone].copy()

    """Clean zoning_valid_df DataFrame from excessive columns if values are not informative"""
    # drop device status column if all ports are available
    if (zoning_valid_df['Статус устройства в сети конфигурации'] == 'доступно').all():
        zoning_valid_df.drop(columns=['Статус устройства в сети конфигурации'], inplace=True)
    # drop device fabric label columns if they are equal with the switch defined conffig
    if 'Подсеть устройства' in zoning_valid_df.columns:
        # if Fabric name and Fabric labels are equal for config defined switch and zonemembers
        # then zonemember Fabric name and label columns are excessive
        check_df = zoning_valid_df.copy()
        zonemember_fabric_columns = ['Фабрика устройства', 'Подсеть устройства']
        cfg_fabric_columns = ['Фабрика', 'Подсеть']
        zonemember_fabric_columns = [column for column in zonemember_fabric_columns if column in check_df.columns]
        cfg_fabric_columns = [column for column in cfg_fabric_columns if column in check_df.columns]
        check_df.dropna(subset = zonemember_fabric_columns, inplace=True)
        zonemember_check_df = check_df.loc[:, zonemember_fabric_columns].copy()
        zonemember_check_df.rename(columns= dict(zip(zonemember_fabric_columns, cfg_fabric_columns)), inplace=True)
        if check_df[cfg_fabric_columns].equals(zonemember_check_df[cfg_fabric_columns]):
            zoning_valid_df.drop(columns=zonemember_fabric_columns, inplace=True)
    # drop alias wwn type if all zonemembers are Wwnp type
    if 'Тип Wwn псевдонима' in zoning_valid_df.columns:
        check_df = zoning_valid_df.copy()
        check_df.dropna(subset = ['Тип Wwn псевдонима'], inplace=True)
        if (zoning_valid_df['Тип Wwn псевдонима'] == 'Wwnp').all():
            zoning_valid_df.drop(columns=['Тип Wwn псевдонима'], inplace=True)

    # separate A and B fabrics for side by side compare
    mask_A = zoning_valid_df['Подсеть'] == 'A'
    mask_B = zoning_valid_df['Подсеть'] == 'B'
    zoning_report_A_df = zoning_valid_df.loc[mask_A].copy()
    zoning_report_B_df = zoning_valid_df.loc[mask_B].copy()
    # reset index for both A and B fabrics to concatenate DataFrames horizontally
    zoning_report_A_df.reset_index(inplace=True, drop=True)
    zoning_report_B_df.reset_index(inplace=True, drop=True)

    # create side by side comparision report table
    zoning_compare_report_df = pd.concat([zoning_report_A_df, zoning_report_B_df], axis=1)

    return zoning_compare_report_df


def create_report(aggregated_df, data_name, report_columns_usage_dct, max_title):
    """
    Auxiliary function to remove unnecessary columns from aggregated DataFrame and
    extract required columns and create report dataframe
    """

    # columns which values need to be translated
    translate_columns = ['Fabric_device_status', 'Target_Initiator_note', 'Target_model_note']
    # loading values to translate
    translate_dct = dct_from_columns('customer_report', max_title, 'Зонирование_перевод_eng', 
                                        'Зонирование_перевод_ru', init_file = 'san_automation_info.xlsx')

    # pylint: disable=unbalanced-tuple-unpacking
    cleaned_df = drop_columns(aggregated_df, report_columns_usage_dct)
    # translate values in column if column in DataFrame
    for column in translate_columns:
        if column in cleaned_df.columns:
            cleaned_df[column] = cleaned_df[column].replace(to_replace=translate_dct)
    # take required data from aggregated DataFrame to create report
    report_df, = dataframe_segmentation(cleaned_df, data_name, report_columns_usage_dct, max_title)

    return report_df


def drop_columns(aggregated_df, report_columns_usage_dct):
    """Auxiliary function to drop unnecessary columns from aggreagated DataFrame"""
    
    fabric_name_usage = report_columns_usage_dct['fabric_name_usage']

    cleaned_df = aggregated_df.copy()
    # if Fabric name and Fabric labels are equal for config defined switch and zonemembers
    # then zonemember Fabric name and label columns are excessive
    check_df = cleaned_df.copy()
    check_df.dropna(subset = ['zonemember_Fabric_name', 'zonemember_Fabric_label'], inplace=True)
    mask_fabricname = (check_df.Fabric_name == check_df.zonemember_Fabric_name).all()
    mask_fabriclabel = (check_df.Fabric_label == check_df.zonemember_Fabric_label).all()
    if mask_fabricname and mask_fabriclabel:
        cleaned_df.drop(columns=['zonemember_Fabric_name', 'zonemember_Fabric_label'], inplace=True)
    # if zonemembers or aliases are not in the same fabric with the switch of zoning config definition
    # but there is only one fabric then zonemember_Fabric_name column is excessive
    if not fabric_name_usage and ('zonemember_Fabric_name' in cleaned_df.columns):
        cleaned_df.drop(columns=['zonemember_Fabric_name'], inplace=True)
    # if there is no LSAN devices in fabric then LSAN_device_state column should be dropped
    if cleaned_df.LSAN_device_state.isna().all():
        cleaned_df.drop(columns=['LSAN_device_state'], inplace=True)
    # if all zonemembers are defined through WWNp then Wwn_type column not informative
    check_df.dropna(subset = ['Wwn_type'], inplace=True)
    mask_wwnp = (check_df.Wwn_type == 'Wwnp').all()
    if mask_wwnp:
        cleaned_df.drop(columns=['Wwn_type'], inplace=True)
    # if only effective configuration present drop cfg_type column
    if cleaned_df['cfg_type'].str.contains('effective', na=False).all():
        cleaned_df.drop(columns=['cfg_type'], inplace=True)
    # if all zonemembers except not connected are in the same fabric 
    # with the cfg switch drop  Member_in_cfg_Fabric column
    if (cleaned_df['Member_in_cfg_Fabric'].dropna() == 'Да').all():
        cleaned_df.drop(columns=['Member_in_cfg_Fabric'], inplace=True)
    # if all ports are local
    if (cleaned_df['Fabric_device_status'] == 'local').all():
        cleaned_df.drop(columns=['Fabric_device_status'], inplace=True)
    # if all ports are F-ports
    if (cleaned_df['portType'].dropna() == 'F-Port').all():
        cleaned_df.drop(columns=['portType'], inplace=True)

    return cleaned_df


def zonemember_statistics(zoning_aggregated_df):
    """Function to create zonemembers statistics"""

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
    We are interested to count coonected devices statistic only.
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

    # get statistice DataFrames for zone anf cfgtype level statistics
    zonemember_zonelevel_stat_df = aggregataed_statistics(zoning_modified_df)
    zonemember_cfgtypelevel_stat_df = aggregataed_statistics(zoning_modified_df, zone=False)

    zonemember_zonelevel_stat_df.reset_index(inplace=True)
    # drop duplicated All row
    zonemember_zonelevel_stat_df.drop(zonemember_zonelevel_stat_df.index[zonemember_zonelevel_stat_df['Fabric_name'] == 'All'], inplace = True)
    zonemember_cfgtypelevel_stat_df.reset_index(inplace=True)
    # add 'Target_Initiator'and 'Target_model' notes to zonemember_zonelevel_stat_df DataFrame
    zonemember_zonelevel_stat_df = statistics_notes(zonemember_zonelevel_stat_df)
    # concatenate both statistics
    zonemember_statistics_df = pd.concat([zonemember_zonelevel_stat_df, zonemember_cfgtypelevel_stat_df], ignore_index=True)
        
    return zonemember_statistics_df, zonemember_zonelevel_stat_df


def statistics_to_agggregated_zoning(zoning_aggregated_df, zonemember_zonelevel_stat_df):
    """
    Function to compliment zoning_aggregated_df DataFrame with 'Target_Initiator'and 'Target_model' notes 
    obtained from zone statistics DataFrame analysis
    """
    
    # create DataFrame with note columns only
    zone_columns = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone']
    note_columns = ['Target_Initiator_note', 'Target_model_note']
    note_columns = [column for column in note_columns if column in zonemember_zonelevel_stat_df.columns]
    zonenote_df = zonemember_zonelevel_stat_df.reindex(columns = [*zone_columns, *note_columns])
    # compliment aggregated DataFrame 
    zoning_aggregated_df = zoning_aggregated_df.merge(zonenote_df, how='left', on=zone_columns)

    return zoning_aggregated_df


def statistics_notes(zonemember_zonelevel_stat_df):
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

    def target_initiator_note(series):
        """
        Auxiliary function to verify zone content from target_initiator
        number point of view.
        """
        
        # if pd.isna(series['zone']):
        #     return np.nan
        
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
        
        # TO REMOVE
        # # if zone contains initiator but not targets and there are zonemembers 
        # # with no connection to fabric of zoning configuration switch
        # # then zone considered to be target's less zone
        # if series['Total_zonemembers'] > series['Total_connected_zonemembers'] and series['SRV'] > 0:
        #     if series['STORAGE_LIB'] == 0:
        #         return 'no_target'

        # if zone contains initiator(s) but not targets then zone considered to be target's less zone
        if series['SRV'] >= 1 and series['STORAGE_LIB'] == 0:
                return 'no_target'
        # if zone contains more then one initiator and target(s) then initiator number exceeds threshold
        if series['SRV'] > 1:
            return 'several_initiators'
        
        return np.nan

    # zonemember_stat_notes_df['Target_Initiator_note'] = np.nan

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


# def zone_notes(zoning_aggregated_df, zonemember_statistics_df):



def aggregataed_statistics(zoning_modified_df, zone=True):
    """
    Auxiliary function to count statistics for 
    modified zoning_modified_df DataFrame.
    Zone True - Zone level statistics,
    Zone False - Cfg type level statistics
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











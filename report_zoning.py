"""Module to create zoning configuration related reports"""

import pandas as pd
import numpy as np


from common_operations_dataframe import dataframe_segmentation
from common_operations_servicefile import dct_from_columns

def zoning_report_main(zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df,
                        zonemember_statistics_df, effective_cfg_statistics_df, data_names, report_columns_usage_dct, max_title):
    """Main function to create zoning report tables"""

    # report tables
    # loading values to translate
    translate_dct = dct_from_columns('customer_report', max_title, 'Зонирование_перевод_eng', 
                                        'Зонирование_перевод_ru', init_file = 'san_automation_info.xlsx')
    zoning_report_df = create_report(zoning_aggregated_df, data_names[5:6], translate_dct, report_columns_usage_dct, max_title)
    alias_report_df = create_report(alias_aggregated_df, data_names[6:7], translate_dct, report_columns_usage_dct, max_title)
    zoning_compare_report_df = compare_zone_config(zoning_report_df)
    unzoned_device_report_df, no_alias_device_report_df = unzoned_device_report(portshow_zoned_aggregated_df, data_names[8:10], report_columns_usage_dct, max_title)
    zoning_absent_device_report_df = absent_device(zoning_aggregated_df, data_names[10], translate_dct, report_columns_usage_dct, max_title)
    zonemember_statistics_report_df = zonemember_statistics_report(zonemember_statistics_df, translate_dct, report_columns_usage_dct, max_title)
    effective_cfg_statistics_report_df = cfg_statistics_report(effective_cfg_statistics_df, translate_dct, report_columns_usage_dct, max_title)

    return zoning_report_df, alias_report_df, zoning_compare_report_df, unzoned_device_report_df, \
            no_alias_device_report_df, zoning_absent_device_report_df, zonemember_statistics_report_df, \
                effective_cfg_statistics_report_df


def create_report(aggregated_df, data_name, translate_dct, report_columns_usage_dct, max_title):
    """
    Auxiliary function to remove unnecessary columns from aggregated DataFrame and
    extract required columns and create report dataframe
    """

    # pylint: disable=unbalanced-tuple-unpacking
    cleaned_df = drop_columns(aggregated_df, report_columns_usage_dct)
    translate_columns = ['Fabric_device_status', 'Target_Initiator_note', 'Target_model_note']
    cleaned_df = translate_values(cleaned_df, translate_dct, translate_columns)
    # take required data from aggregated DataFrame to create report
    report_df, = dataframe_segmentation(cleaned_df, data_name, report_columns_usage_dct, max_title)

    return report_df


def drop_columns(aggregated_df, report_columns_usage_dct):
    """Auxiliary function to drop unnecessary columns from aggreagated DataFrame"""
    
    fabric_name_usage = report_columns_usage_dct['fabric_name_usage']
    # list of columns to check if all values in column are NA
    possible_allna_values = ['LSAN_device_state', 'alias_duplicated', 'Wwnn_unpack', 'peerzone_member_type']
    # dictionary of items to check if all values in column (dict key) are equal to certain value (dict value)
    possible_identical_values = {'Wwn_type': 'Wwnp', 'cfg_type': 'effective', 'Member_in_cfg_Fabric': 'Да', 
                                            'Fabric_device_status': 'local', 'portType': 'F-Port', }
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

    # if all aliases contain one wwn only
    if 'zone_member' in cleaned_df and 'zonemember_duplicates_free' in cleaned_df:
        if all(cleaned_df['zone_member'] == cleaned_df['zonemember_duplicates_free']):
            cleaned_df.drop(columns=['zonemember_duplicates_free'], inplace=True)
    # if all device connected to one fabric_label only
    if 'Device_Host_Name_per_fabric_name_and_label' in cleaned_df.columns and \
        'Device_Host_Name_per_fabric_label' in cleaned_df.columns and \
            cleaned_df['Device_Host_Name_per_fabric_name_and_label'].equals(cleaned_df['Device_Host_Name_per_fabric_label']):
                cleaned_df.drop(columns=['Device_Host_Name_per_fabric_label'], inplace=True)
    # drop columns where all values are NA
    for column in possible_allna_values:
        if column in cleaned_df.columns and cleaned_df[column].isna().all():
            cleaned_df.drop(columns=[column], inplace=True)
    # drop columns where all values after dropping NA are equal to certian value
    for column, value in possible_identical_values.items():
        if column in cleaned_df.columns and (cleaned_df[column].dropna() == value).all():
            cleaned_df.drop(columns=[column], inplace=True)

    return cleaned_df


def translate_values(translated_df, translate_dct, translate_columns = None):
    """Function to translate values in corresponding columns"""

    if not translate_columns:
        translate_columns = translated_df.columns

    # columns which values need to be translated
    # translate values in column if column in DataFrame
    for column in translate_columns:
        if column in translated_df.columns:
            translated_df[column] = translated_df[column].replace(to_replace=translate_dct)

    return translated_df


def compare_zone_config(zoning_report_df):
    """
    Function to create comparision table (side by side) of A and B fabrics
    with valid zones of effective configuration only.
    """

    mask_applied = False

    # list of invalid zones to remove from compare report
    if 'Примечание. Количество таргетов и инициаторов в зоне' in zoning_report_df.columns:
        invalid_zone_lst = ['нет инициатора', 'нет таргета', 'нет инициатора, нет таргета', 'нет таргета, несколько инициаторов']
        mask_valid_zone = ~zoning_report_df['Примечание. Количество таргетов и инициаторов в зоне'].isin(invalid_zone_lst)
        mask_applied = True
    # take effective configuration only if defined configuration(s) exist
    if 'Тип конфигурации' in zoning_report_df.columns:
        mask_effective = zoning_report_df['Тип конфигурации'] == 'effective'
        mask_valid_zone = mask_effective & mask_valid_zone if mask_applied else mask_effective
        # if mask_applied:
        #     mask_valid_zone = mask_effective & mask_valid_zone
        # else:

    if mask_applied:    
        zoning_valid_df = zoning_report_df.loc[mask_valid_zone].copy()
    else:
        zoning_valid_df = zoning_report_df.copy()

    """Clean zoning_valid_df DataFrame from excessive columns if values are not informative"""
    # drop device status column if all ports are available
    if 'Статус устройства в сети конфигурации' in zoning_valid_df.columns and (zoning_valid_df['Статус устройства в сети конфигурации'] == 'доступно').all():
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
    # drop column if each device connected to single fabric_name only
    if 'Количество портов устройства в подсети' in zoning_valid_df.columns and \
        'Количество портов устройства в фабрике' in zoning_valid_df.columns and \
            zoning_valid_df['Количество портов устройства в подсети'].equals(zoning_valid_df['Количество портов устройства в фабрике']):
                zoning_valid_df.drop(columns=['Количество портов устройства в фабрике'], inplace=True)


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


def unzoned_device_report(portshow_cfg_aggregated_df, data_names, report_columns_usage_dct, max_title):
    """
    Function to check all fabric devices for usage in zoning configuration and
    check if all fabric devices have aliases.
    Create unzoned devices and no aliases reports
    """

    # switche and virtual connect ports are not part of zoning configuration by defenition
    mask_not_switch_vc = ~portshow_cfg_aggregated_df.deviceType.isin(['SWITCH', 'VC'])
    # show online ports only
    mask_online = portshow_cfg_aggregated_df['portState'] == 'Online'
    # Access gateway switch connection information is excessive
    mask_native = portshow_cfg_aggregated_df['switchMode'] == 'Native'
    # show ports which are not part of any configuration
    mask_not_zoned = portshow_cfg_aggregated_df['cfg_type'].isna()
    # show_devices that have no aliases
    mask_no_alias = portshow_cfg_aggregated_df['alias'].isna()

    unzoned_device_df = portshow_cfg_aggregated_df.loc[mask_native & mask_online & mask_not_switch_vc & mask_not_zoned]
    unzoned_device_df.dropna(axis='columns', how='all')

    no_alias_device_df = portshow_cfg_aggregated_df.loc[mask_native & mask_online & mask_not_switch_vc & mask_no_alias]
    # no_alias_devices_df.dropna(axis='columns', how='all')
    # create report DataFeame
    # pylint: disable=unbalanced-tuple-unpacking
    unzoned_device_report_df, = dataframe_segmentation(unzoned_device_df, data_names[0], report_columns_usage_dct, max_title)
    no_alias_device_report_df, = dataframe_segmentation(no_alias_device_df, data_names[1], report_columns_usage_dct, max_title)


    return unzoned_device_report_df, no_alias_device_report_df


def absent_device(zoning_aggregated_df, data_name, translate_dct, report_columns_usage_dct, max_title):
    """Function to create table with absent and unavailable remote devices in zoning configuration"""
    
    mask_absent = zoning_aggregated_df.Fabric_device_status.isin(['absent', 'remote_na'])
    absent_columns = ['Fabric_name', 'Fabric_label', 'cfg',	'cfg_type',	'zone_member', 'alias_member', 'Fabric_device_status', 'zone']
    absent_device_df = zoning_aggregated_df.loc[mask_absent, absent_columns]
    absent_device_df = absent_device_df.groupby(absent_columns[:-1], as_index = False).agg({'zone': ', '.join})
    absent_device_df = translate_values(absent_device_df, translate_dct, ['Fabric_device_status'])
    zoning_absent_device_report_df, = dataframe_segmentation(absent_device_df, data_name, report_columns_usage_dct, max_title)

    return zoning_absent_device_report_df


def zonemember_statistics_report(zonemember_statistics_df, translate_dct, report_columns_usage_dct, max_title):
    """Function to create report table out of statistics_df DataFrame"""

    fabric_name_usage = report_columns_usage_dct['fabric_name_usage']

    # create statitics report DataFrame
    zonemember_statistics_report_df = zonemember_statistics_df.copy()
    # drop 'Wwnn_to_Wwnp_number_unpacked' column if all values are zero
    if (zonemember_statistics_report_df['Wwnn_to_Wwnp_number_unpacked'].dropna() == 0).all():
        zonemember_statistics_report_df.drop(columns=['Wwnn_to_Wwnp_number_unpacked'], inplace=True)        

    # # drop column 'chassis_name' if it is not required
    # if not fabric_name_usage:
    #     zonemember_statistics_report_df.drop(columns = ['Fabric_name'], inplace=True)
    # rename values in columns
    translate_columns = ['Fabric_name', 'Fabric_device_status', 'Target_Initiator_note', 'Target_model_note']
    zonemember_statistics_report_df = translate_values(zonemember_statistics_report_df, translate_dct, translate_columns)
    # column titles used to create dictionary to traslate column names
    statistic_columns_lst = ['Статистика_зон_eng', 'Статистика_зон_ru']
    # dictionary used to translate column names
    statistic_columns_dct = dct_from_columns('customer_report', max_title, *statistic_columns_lst, \
        init_file = 'san_automation_info.xlsx')
    # translate columns in fabric_statistics_report and statistics_subtotal_df DataFrames
    zonemember_statistics_report_df.rename(columns = statistic_columns_dct, inplace = True)

    # statistics_report_df.sort_values(by=['Фабрика', 'Подсеть', 'Имя коммутатора'], inplace=True)

    return zonemember_statistics_report_df

def cfg_statistics_report(effective_cfg_statistics_df, translate_dct, report_columns_usage_dct, max_title):

    # create statitics report DataFrame
    effective_cfg_statistics_report_df = effective_cfg_statistics_df.copy()
    # # drop column 'chassis_name' if it is not required
    # if not fabric_name_usage:
    #     zonemember_statistics_report_df.drop(columns = ['Fabric_name'], inplace=True)
    # rename values in columns
    translate_columns = ['Fabric_name']
    effective_cfg_statistics_report_df = \
        translate_values(effective_cfg_statistics_report_df, translate_dct, translate_columns)

    # column titles used to create dictionary to traslate column names
    statistic_columns_lst = ['Статистика_конфигурации_eng', 'Статистика_конфигурации_ru']
    # dictionary used to translate column names
    statistic_columns_dct = dct_from_columns('customer_report', max_title, *statistic_columns_lst, \
        init_file = 'san_automation_info.xlsx')
    # translate columns in fabric_statistics_report and statistics_subtotal_df DataFrames
    effective_cfg_statistics_report_df.rename(columns = statistic_columns_dct, inplace = True)

    return effective_cfg_statistics_report_df
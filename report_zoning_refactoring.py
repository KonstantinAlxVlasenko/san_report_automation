"""Module to create zoning configuration related reports"""

import pandas as pd

# from common_operations_dataframe import dataframe_segmentation
from common_operations_dataframe_presentation import (
    dataframe_segmentation, dataframe_slice_concatenate, drop_all_identical,
    drop_all_na, drop_equal_columns, drop_equal_columns_pairs, translate_values)
from common_operations_servicefile import dct_from_columns


def zoning_report_main(zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df,
                        zonemember_statistics_df, alias_statistics_df, effective_cfg_statistics_df, 
                            data_names, report_columns_usage_dct, max_title):
    """Main function to create zoning report tables"""

    # report tables
    # loading values to translate
    translate_dct = dct_from_columns('customer_report', max_title, 'Зонирование_перевод_eng', 
                                        'Зонирование_перевод_ru', init_file = 'san_automation_info.xlsx')
    zoning_report_df = create_report(zoning_aggregated_df, data_names[6:7], translate_dct, report_columns_usage_dct, max_title)
    zoning_valid_df = valid_zoning(zoning_aggregated_df)
    zoning_valid_report_df = create_report(zoning_valid_df, data_names[6:7], translate_dct, report_columns_usage_dct, max_title)
    zoning_compare_report_df = dataframe_slice_concatenate(zoning_valid_report_df, column='Подсеть')
    alias_report_df = create_report(alias_aggregated_df, data_names[7:8], translate_dct, report_columns_usage_dct, max_title)
    
    unzoned_device_report_df, no_alias_device_report_df = \
        unzoned_device_report(portshow_zoned_aggregated_df, data_names[9:11], report_columns_usage_dct, max_title)
    zoning_absent_device_report_df = \
        absent_device(zoning_aggregated_df, data_names[11], translate_dct, report_columns_usage_dct, max_title)
    zonemember_statistics_report_df = statistics_report(zonemember_statistics_df, data_names[12], translate_dct, max_title)
    alias_statistics_report_df = statistics_report(alias_statistics_df, data_names[13], translate_dct, max_title)
    effective_cfg_statistics_report_df = statistics_report(effective_cfg_statistics_df, data_names[14], translate_dct, max_title)

    return zoning_report_df, alias_report_df, zoning_compare_report_df, unzoned_device_report_df, \
            no_alias_device_report_df, zoning_absent_device_report_df, zonemember_statistics_report_df, \
                alias_statistics_report_df, effective_cfg_statistics_report_df


def create_report(aggregated_df, data_name, translate_dct, report_columns_usage_dct, max_title):
    """
    Auxiliary function to remove unnecessary columns from aggregated DataFrame and
    extract required columns and create report dataframe
    """

    # pylint: disable=unbalanced-tuple-unpacking
    cleaned_df = drop_excessive_columns(aggregated_df, report_columns_usage_dct)
    translate_columns = ['Fabric_device_status', 'Target_Initiator_note', 'Target_model_note', 
                            'Effective_cfg_usage_note', 'Pair_zone_note', 'Multiple_fabric_label_connection',
                            'Zone_and_Pairzone_names_related', 'Zone_name_device_names_related']
    cleaned_df = translate_values(cleaned_df, translate_dct, translate_columns)
    # take required data from aggregated DataFrame to create report
    report_df, = dataframe_segmentation(cleaned_df, data_name, report_columns_usage_dct, max_title)
    return report_df


def drop_excessive_columns(df, report_columns_usage_dct):
    """Auxiliary function to drop unnecessary columns from aggreagated DataFrame"""

    fabric_name_usage = report_columns_usage_dct['fabric_name_usage']
    # list of columns to check if all values in column are NA
    possible_allna_values = ['LSAN_device_state', 'alias_duplicated', 'Wwnn_unpack', 
                                'peerzone_member_type', 'zone_duplicated', 
                                'Target_Initiator_note', 'Effective_cfg_usage_note', 'Pair_zone_note',
                                'Device_Port', 'Storage_Port_Type']
    # dictionary of items to check if all values in column (dict key) are equal to certain value (dict value)

    cleaned_df = df.copy()

    # if Fabric name and Fabric labels are equal for config defined switch and zonemembers
    # then zonemember Fabric name and label columns are excessive 
    cleaned_df = drop_equal_columns_pairs(cleaned_df, columns_main=['Fabric_name', 'Fabric_label'], 
                                        columns_droped=['zonemember_Fabric_name', 'zonemember_Fabric_label'], dropna=True)

    # if all aliases contain one member only
    # if all devices connected to one fabric_label only
    cleaned_df = drop_equal_columns(cleaned_df, columns_pairs=[('zone_member', 'zonemember_duplicates_free'),
                                                                ('Device_Host_Name_per_fabric_name_and_label', 'Device_Host_Name_per_fabric_label'),
                                                                ('Device_Host_Name_total_fabrics', 'Device_Host_Name_per_fabric_name')])
    # drop columns where all values are NA
    cleaned_df = drop_all_na(cleaned_df, possible_allna_values)
    # drop columns where all values after dropping NA are equal to certian value
    possible_identical_values = {'Wwn_type': 'Wwnp', 'Member_in_cfg_Fabric': 'Да', 
                                'Fabric_device_status': 'local', 'portType': 'F-Port', 
                                'Storage_Port_Type': 'host'}
    cleaned_df = drop_all_identical(cleaned_df, possible_identical_values, dropna=True)
    # drop columns where all values without dropping NA are equal to certian value
    possible_identical_values = {'cfg_type': 'effective'}
    cleaned_df = drop_all_identical(cleaned_df, possible_identical_values, dropna=False)
    return cleaned_df


def valid_zoning(zoning_aggregated_df):
    """Function to filter effective and valid zones from zoning aggregated DataFrame
    to match zoning configuration in different fabric labels"""

    # effective configuration
    mask_effective = zoning_aggregated_df['cfg_type'] == 'effective'
    # valid zones
    invalid_zone_tags = ['no_target', 'no_initiator', 'no_target, no_initiator', 'no_target, several_initiators']
    mask_valid_zone = ~zoning_aggregated_df['Target_Initiator_note'].isin(invalid_zone_tags)

    zoning_valid_df = zoning_aggregated_df.loc[mask_effective & mask_valid_zone].copy()
    return zoning_valid_df


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
    absent_columns = ['Fabric_name', 'Fabric_label', 'cfg',	'cfg_type',	'zone_member', 'alias_member', 
                        'Fabric_device_status', 'zonemember_Fabric_name', 'zonemember_Fabric_label', 'zone']
    absent_device_df = zoning_aggregated_df.loc[mask_absent, absent_columns]
    absent_device_df = absent_device_df.groupby(absent_columns[:-1], as_index = False, dropna=False).agg({'zone': ', '.join})
    absent_device_df = translate_values(absent_device_df, translate_dct, ['Fabric_device_status'])
    zoning_absent_device_report_df, = dataframe_segmentation(absent_device_df, data_name, report_columns_usage_dct, max_title)
    return zoning_absent_device_report_df


def statistics_report(statistics_df, data_name, translate_dct, max_title):
    """Function to convert zonemember_statistics_df, alias_statistics_df and cfg_statistics_df 
    to report DataFrames (traslate column names and All)"""
    
    # create statitics report DataFrame
    statistics_report_df = statistics_df.copy()

    if data_name == 'Статистика_зон':
        possible_allna_columns = ['zone_duplicated', 'Target_Initiator_note', 'Effective_cfg_usage_note']
        statistics_report_df = drop_all_na(statistics_report_df, possible_allna_columns)

        # drop 'Wwnn_to_Wwnp_number_unpacked' column if all values are zero
        statistics_report_df = drop_all_identical(statistics_report_df, 
                                                    columns_values={'Wwnn_to_Wwnp_number_unpacked': 0}, dropna=True)

    # rename values in columns
    if data_name == 'Статистика_зон':
        translate_columns = ['Fabric_name', 'Fabric_device_status', 
                                'Target_Initiator_note', 'Target_model_note', 
                                'Effective_cfg_usage_note', 'Pair_zone_note',
                                'All_devices_multiple_fabric_label_connection',
                                'Zone_and_Pairzone_names_related', 'Zone_name_device_names_related']
    else:
        translate_columns = ['Fabric_name']
    statistics_report_df = \
        translate_values(statistics_report_df, translate_dct, translate_columns)

    # column titles used to create dictionary to traslate column names
    statistic_columns_lst = [data_name + '_eng', data_name + '_ru']
    # dictionary used to translate column names
    statistic_columns_dct = dct_from_columns('customer_report', max_title, *statistic_columns_lst, \
        init_file = 'san_automation_info.xlsx')
    # translate columns in fabric_statistics_report and statistics_subtotal_df DataFrames
    statistics_report_df.rename(columns = statistic_columns_dct, inplace = True)
    return statistics_report_df

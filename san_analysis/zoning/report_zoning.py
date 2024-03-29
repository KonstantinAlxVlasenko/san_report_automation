"""Module to create zoning configuration related reports"""


import pandas as pd

import utilities.dataframe_operations as dfop
import utilities.report_operations as report


def zoning_report_main(zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df,
                        zonemember_statistics_df, alias_statistics_df, effective_cfg_statistics_df, 
                            data_names, report_headers_df, report_columns_usage_sr):
    """Main function to create zoning report tables"""

    if zoning_aggregated_df.empty:
        return [pd.DataFrame()] * 9
    
    zoning_report_df = create_report(zoning_aggregated_df, report_headers_df, report_columns_usage_sr, data_names[6])
    zoning_valid_df = valid_zoning(zoning_aggregated_df)
    zoning_valid_report_df = create_report(zoning_valid_df, report_headers_df, report_columns_usage_sr, data_names[6])
    zoning_compare_report_df = dfop.dataframe_slice_concatenate(zoning_valid_report_df, column='Подсеть')
    alias_report_df = create_report(alias_aggregated_df, report_headers_df, report_columns_usage_sr, data_names[7])
    
    unzoned_device_report_df, no_alias_device_report_df = \
        unzoned_device_report(portshow_zoned_aggregated_df, report_headers_df, report_columns_usage_sr, data_names[9:11])
    zoning_absent_device_report_df = \
        absent_device(zoning_aggregated_df, report_headers_df, report_columns_usage_sr, data_names[11])

    zonemember_statistics_report_df = statistics_report(zonemember_statistics_df, report_headers_df, data_names[12])
    dfop.drop_zero(zonemember_statistics_report_df)
    alias_statistics_report_df = statistics_report(alias_statistics_df, report_headers_df, data_names[13])
    effective_cfg_statistics_report_df = statistics_report(effective_cfg_statistics_df, report_headers_df, data_names[14])

    return zoning_report_df, alias_report_df, zoning_compare_report_df, unzoned_device_report_df, \
            no_alias_device_report_df, zoning_absent_device_report_df, zonemember_statistics_report_df, \
                alias_statistics_report_df, effective_cfg_statistics_report_df


def create_report(aggregated_df, report_headers_df, report_columns_usage_sr, df_name):
    """
    Auxiliary function to remove unnecessary columns from aggregated DataFrame and
    extract required columns and create report dataframe
    """

    # pylint: disable=unbalanced-tuple-unpacking
    cleaned_df = drop_excessive_columns(aggregated_df, report_columns_usage_sr)

    translated_columns = ['Fabric_device_status', 'Target_Initiator_note', 'Target_model_note', 
                            'Effective_cfg_usage_note', 'Pair_zone_note', 'Multiple_fabric_label_connection',
                            'Zone_and_Pairzone_names_related', 'Zone_name_device_names_related', 'Mixed_zone_note']

    cleaned_df = report.translate_values(cleaned_df, report_headers_df, 'Зонирование_перевод', translated_columns)
    report_df = report.generate_report_dataframe(cleaned_df, report_headers_df, report_columns_usage_sr, df_name)
    report.drop_slot_value(report_df, report_columns_usage_sr)
    return report_df


def drop_excessive_columns(df, report_columns_usage_dct):
    """Auxiliary function to drop unnecessary columns from aggreagated DataFrame"""

    fabric_name_usage = report_columns_usage_dct['fabric_name_usage']
    # list of columns to check if all values in column are NA
    possible_allna_values = ['LSAN_device_state', 'alias_duplicated', 'Wwnn_unpack', 
                                'peerzone_member_type', 'zone_duplicated', 'zone_absorber',
                                'Target_Initiator_note', 'Effective_cfg_usage_note', 'Pair_zone_note', 'Mixed_zone_note',
                                'Device_Port', 'Storage_Port_Type']
    # dictionary of items to check if all values in column (dict key) are equal to certain value (dict value)

    cleaned_df = df.copy()

    # if Fabric name and Fabric labels are equal for config defined switch and zonemembers
    # then zonemember Fabric name and label columns are excessive 
    cleaned_df = dfop.drop_equal_columns_pairs(cleaned_df, columns_main=['Fabric_name', 'Fabric_label'], 
                                        columns_droped=['zonemember_Fabric_name', 'zonemember_Fabric_label'], dropna=True)

    # if all aliases contain one member only
    # if all devices connected to one fabric_label only
    cleaned_df = dfop.drop_equal_columns(cleaned_df, columns_pairs=[('zone_member', 'zonemember_duplicates_free'),
                                                                ('Device_Host_Name_per_fabric_name_and_label', 'Device_Host_Name_per_fabric_label'),
                                                                ('Device_Host_Name_total_fabrics', 'Device_Host_Name_per_fabric_name')])
    # drop columns where all values are NA
    cleaned_df = dfop.drop_column_if_all_na(cleaned_df, possible_allna_values)
    # drop columns where all values after dropping NA are equal to certian value
    possible_identical_values = {'Wwn_type': 'Wwnp', 'Member_in_cfg_Fabric': 'Да', 
                                'Fabric_device_status': 'local', 'portType': 'F-Port', 
                                'Storage_Port_Type': 'host'}
    cleaned_df = dfop.drop_all_identical(cleaned_df, possible_identical_values, dropna=True)
    # drop columns where all values without dropping NA are equal to certian value
    possible_identical_values = {'cfg_type': 'effective'}
    cleaned_df = dfop.drop_all_identical(cleaned_df, possible_identical_values, dropna=False)
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
    zoning_valid_df.drop(columns=['Zone_name_device_names_ratio', 'Zone_name_device_names_related'], inplace=True)
    return zoning_valid_df


def unzoned_device_report(portshow_cfg_aggregated_df, report_headers_df, report_columns_usage_dct, data_names):
    """
    Function to check all fabric devices for usage in zoning configuration and
    check if all fabric devices have aliases.
    Create unzoned devices and no aliases reports
    """

    # switche and virtual connect ports are not part of zoning configuration by defenition
    mask_not_switch_vc = ~portshow_cfg_aggregated_df.deviceType.isin(['SWITCH', 'VC'])
    # show online ports only
    mask_online = portshow_cfg_aggregated_df['portState'] == 'Online'
    mask_wwn_notna = portshow_cfg_aggregated_df['Connected_portWwn'].notna()
    # Access gateway switch connection information is excessive
    mask_native = portshow_cfg_aggregated_df['switchMode'] == 'Native'
    # show ports which are not part of any configuration
    mask_not_zoned = portshow_cfg_aggregated_df['cfg_type'] != 'effective'
    # show_devices that have no aliases
    mask_no_alias = portshow_cfg_aggregated_df['alias'].isna()
    
    unzoned_device_df = portshow_cfg_aggregated_df.loc[mask_native & mask_online & mask_wwn_notna & mask_not_switch_vc & mask_not_zoned]
    unzoned_device_df.dropna(axis='columns', how='all')
    no_alias_device_df = portshow_cfg_aggregated_df.loc[mask_native & mask_online & mask_wwn_notna & mask_not_switch_vc & mask_no_alias]
    unzoned_device_report_df = report.generate_report_dataframe(unzoned_device_df, report_headers_df, report_columns_usage_dct, data_names[0])
    no_alias_device_report_df = report.generate_report_dataframe(no_alias_device_df, report_headers_df, report_columns_usage_dct, data_names[1])
    return unzoned_device_report_df, no_alias_device_report_df


def absent_device(zoning_aggregated_df, report_headers_df, report_columns_usage_dct, data_name):
    """Function to create table with absent and unavailable remote devices in zoning configuration"""
    
    mask_absent = zoning_aggregated_df.Fabric_device_status.isin(['absent', 'remote_na'])
    absent_columns = ['Fabric_name', 'Fabric_label', 'cfg',	'cfg_type',	'zone_member', 'alias_member', 
                        'Fabric_device_status', 'zonemember_Fabric_name', 'zonemember_Fabric_label', 'zone']
    absent_device_df = zoning_aggregated_df.loc[mask_absent, absent_columns]
    absent_device_df = absent_device_df.groupby(absent_columns[:-1], as_index = False, dropna=False).agg({'zone': ', '.join})
    
    absent_device_df = report.translate_values(
        absent_device_df, report_headers_df, 'Зонирование_перевод', translated_columns='Fabric_device_status')
    absent_device_df = dfop.drop_column_if_all_na(
        absent_device_df, columns=['zonemember_Fabric_name', 'zonemember_Fabric_label'])
    zoning_absent_device_report_df = report.generate_report_dataframe(
        absent_device_df, report_headers_df, report_columns_usage_dct, data_name)
    return zoning_absent_device_report_df


def statistics_report(statistics_df, report_headers_df, data_name):
    """Function to convert zonemember_statistics_df, alias_statistics_df and cfg_statistics_df 
    to report DataFrames (traslate column names and All)"""
    
    # create statitics report DataFrame
    statistics_report_df = statistics_df.copy()

    if data_name == 'Статистика_зон':
        possible_allna_columns = ['zone_duplicated', 'zone_absorber', 
                                  'Target_Initiator_note', 'Effective_cfg_usage_note', 'Mixed_zone_note']
        statistics_report_df = dfop.drop_column_if_all_na(statistics_report_df, possible_allna_columns)

        # drop 'Wwnn_to_Wwnp_number_unpacked' column if all values are zero
        statistics_report_df = dfop.drop_all_identical(statistics_report_df, 
                                                    columns_values={'Wwnn_to_Wwnp_number_unpacked': 0}, dropna=True)
    # rename values in columns
    if data_name == 'Статистика_зон':
        translated_columns = ['Fabric_name', 'Fabric_device_status', 
                                'Target_Initiator_note', 'Target_model_note', 'Mixed_zone_note',
                                'Effective_cfg_usage_note', 'Pair_zone_note',
                                'All_devices_multiple_fabric_label_connection',
                                'Zone_and_Pairzone_names_related', 'Zone_name_device_names_related']
    else:
        translated_columns = ['Fabric_name']
    
    statistics_report_df = \
        report.translate_values(statistics_report_df, report_headers_df, 'Зонирование_перевод', translated_columns)
    
    if 'aliasmember_alias' in statistics_report_df.columns:
        statistics_report_df.drop(columns=['aliasmember_alias'], inplace=True)
    statistics_report_df = report.translate_header(statistics_report_df, report_headers_df, data_name)
    return statistics_report_df

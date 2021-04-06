"""Module to create storage hosts DataFrame"""

import numpy as np
import pandas as pd
import re


from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_servicefile import dct_from_columns
from common_operations_dataframe import dataframe_segmentation, dataframe_fillna, translate_values


def storage_host_analysis_main(host_3par_df, system_3par_df, port_3par_df, 
                                portshow_aggregated_df, zoning_aggregated_df, 
                                report_columns_usage_dct, report_data_lst):
    """Main function to analyze storage port configuration"""
        
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    # data_names = ['storage_host_aggregated']
    data_names = ['storage_host_aggregated', 'Презентация']

    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    # storage_host_aggregated_df, = data_lst
    storage_host_aggregated_df, storage_host_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['portshow_aggregated', 'fabric_labels', 'system_3par', 'port_3par', 'host_3par']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating storage hosts table'
        print(info, end =" ") 

        # aggregated DataFrames
        storage_host_aggregated_df = \
            storage_host_aggregation(host_3par_df, system_3par_df, port_3par_df, portshow_aggregated_df, zoning_aggregated_df)

        # after finish display status
        status_info('ok', max_title, len(info))

        # report tables
        storage_host_report_df = storage_host_report(storage_host_aggregated_df, data_names, report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        # data_lst = [storage_host_aggregated_df]
        data_lst = [storage_host_aggregated_df, storage_host_report_df]
        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)

    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        # storage_host_aggregated_df, = verify_data(report_data_lst, data_names, *data_lst)
        storage_host_aggregated_df, storage_host_report_df = verify_data(report_data_lst, data_names, *data_lst)

        # data_lst = [storage_host_aggregated_df]
        data_lst = [storage_host_aggregated_df, storage_host_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return storage_host_aggregated_df


def storage_host_aggregation(host_3par_df, system_3par_df, port_3par_df, portshow_aggregated_df, zoning_aggregated_df):
    
    storage_host_aggregated_df = host_3par_df.copy()
    # add system_name
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, system_3par_df, join_lst=['configname'], filled_lst=['System_Name'])

    # add controller's ports Wwnp and Wwnp
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, port_3par_df, join_lst=['configname', 'Storage_Port'], filled_lst=['NodeName', 'PortName'])

    # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
    for wwn_column in ['Host_Wwn', 'NodeName', 'PortName']:
        mask_wwn = storage_host_aggregated_df[wwn_column].notna()
        storage_host_aggregated_df.loc[mask_wwn, wwn_column] = storage_host_aggregated_df.loc[mask_wwn, wwn_column].apply(lambda wwn: ':'.join(re.findall('..', wwn)))
        storage_host_aggregated_df[wwn_column] = storage_host_aggregated_df[wwn_column].str.lower()
        
    # add controllers ports Fabric_name and Fabric_label
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, join_lst=['PortName'], filled_lst=['Fabric_name', 'Fabric_label'])

    # rename controllers NodeName and PortName
    rename_columns = {'NodeName': 'Storage_Port_Wwnn', 'PortName': 'Storage_Port_Wwnp'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)

    # 'clean' Wwn column to have Wwnp only
    storage_host_aggregated_df['NodeName'] = storage_host_aggregated_df['Host_Wwn']
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, 
                                    join_lst=['Fabric_name', 'Fabric_label', 'NodeName'], 
                                    filled_lst=['PortName'], remove_duplicates=False)
    storage_host_aggregated_df['PortName'].fillna(storage_host_aggregated_df['Host_Wwn'], inplace=True)
    storage_host_aggregated_df.drop(columns=['NodeName'], inplace=True)

    # rename controllers Fabric_name and Fabric_label
    rename_columns = {'Fabric_name': 'Storage_Fabric_name', 'Fabric_label': 'Storage_Fabric_label'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)

    # add host information
    host_columns = ['Fabric_name', 'Fabric_label', 'chassis_name', 'switchName', 'Index_slot_port', 'Connected_portId', 'Device_Host_Name', 'Device_Port', 'Host_OS', 'Device_Location']
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, join_lst=['PortName'], filled_lst=host_columns, remove_duplicates=False)

    # rename host columns
    rename_columns = {'Fabric_name': 'Host_Fabric_name', 'Fabric_label': 'Host_Fabric_label', 'PortName': 'Host_Wwnp'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)

    # verify if host and storage ports are in the same fabric
    mask_same_fabic = (storage_host_aggregated_df['Host_Fabric_name'] == storage_host_aggregated_df['Storage_Fabric_name']) & \
                        (storage_host_aggregated_df['Host_Fabric_label'] == storage_host_aggregated_df['Storage_Fabric_label'])
    mask_fabric_notna = storage_host_aggregated_df[['Host_Fabric_name', 'Storage_Fabric_name', 'Host_Fabric_label', 'Storage_Fabric_label']].notna().all(axis=1)
    storage_host_aggregated_df['Host_Storage_Fabric_equal'] = np.select([mask_fabric_notna & mask_same_fabic, mask_fabric_notna & ~mask_same_fabic], ['Yes', 'No'], default=pd.NA)

    # verify persona (host mode) is defined in coreespondence with host os
    mask_vmware = storage_host_aggregated_df['Persona'].str.lower().str.contains('vmware') & storage_host_aggregated_df['Host_OS'].str.lower().str.contains('vmware')
    mask_windows = storage_host_aggregated_df['Persona'].str.lower().str.contains('windows') & storage_host_aggregated_df['Host_OS'].str.lower().str.contains('windows')
    mask_linux = storage_host_aggregated_df['Persona'].str.lower().str.contains('generic') & storage_host_aggregated_df['Host_OS'].str.lower().str.contains('linux')
    mask_persona_correct = mask_vmware | mask_windows | mask_linux
    mask_os_notna = storage_host_aggregated_df[['Persona', 'Host_OS']].notna().all(axis=1)
    storage_host_aggregated_df['Persona_correct'] = np.select([mask_os_notna & mask_persona_correct, mask_os_notna & ~mask_persona_correct], ['Yes', 'No'], default=pd.NA)

    storage_host_aggregated_df.fillna(np.nan, inplace=True)

    # prepare zoning
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'imported'])
    mask_effective = zoning_aggregated_df['cfg_type'] == 'effective'
    zoning_valid_df = zoning_aggregated_df.loc[mask_effective & mask_connected].copy()

    storage_host_aggregated_df['zone'] = \
        storage_host_aggregated_df.apply(lambda series: verify_storage_host_zoning(series, zoning_valid_df), axis=1)

    # sort aggregated DataFrame
    sort_columns = ['System_Name', 'Host_Id', 'Host_Name', 'Storage_Port']
    storage_host_aggregated_df.sort_values(by=sort_columns, inplace=True)

    return storage_host_aggregated_df



def verify_storage_host_zoning(series, zoning_valid_df):
    
    if series['Host_Storage_Fabric_equal'] == 'Yes':
        group_columns = ['Fabric_name', 'Fabric_label', 'zone']
        storage_host_sr = series[['Storage_Port_Wwnp', 'Host_Wwnp']]
        
        storage_host_zone_df = \
            zoning_valid_df.groupby(by=group_columns).filter(lambda zone: storage_host_sr.isin(zone['PortName']).all())
        
        mask_same_fabic = (storage_host_zone_df['Fabric_name'] == series['Storage_Fabric_name']) & \
                            (storage_host_zone_df['Fabric_label'] == series['Storage_Fabric_label'])
        zoning_valid_df = storage_host_zone_df.loc[mask_same_fabic].copy()

        if not zoning_valid_df.empty:
            zone_sr = zoning_valid_df['zone'].drop_duplicates()
            zones_str = ', '.join(zone_sr.to_list())
            
            return zones_str


def storage_host_report(storage_host_aggregated_df, data_names, report_columns_usage_dct, max_title):
    
    storage_host_report_df = storage_host_aggregated_df.copy()

    storage_host_report_df = drop_equal_columns(storage_host_report_df, columns_pairs=[('Host_Wwnp', 'Host_Wwn')])

    # if (storage_host_report_df['Host_Wwn'] == storage_host_report_df['Host_Wwnp']).all():
    #     storage_host_report_df.drop(columns=['Host_Wwn'], inplace=True)

    # possible_allna_columns = ['Device_Port', 'Device_Location']
    # for column in possible_allna_columns:
    #     if column in storage_host_report_df.columns and storage_host_report_df[column].isna().all():
    #         storage_host_report_df.drop(columns=[column], inplace=True)

    storage_host_report_df = drop_all_na(storage_host_report_df, ['Device_Port', 'Device_Location'])

    # mask_valid_host = storage_host_report_df['Host_Storage_Fabric_equal'] == 'Yes'
    # storage_host_valid_df = storage_host_report_df.loc[mask_valid_host].copy()

    # storage_host_report_df = clean_df(storage_host_report_df)
    # storage_host_valid_df = clean_df(storage_host_valid_df)


    # verify_columns = ['Host_Storage_Fabric_equal', 'Persona_correct']
    # for column in verify_columns:
    #     if column in storage_host_report_df.columns and not (storage_host_report_df[column] == 'No').any():
    #         storage_host_report_df.drop(columns=[column], inplace=True)

    columns_values = {'Host_Storage_Fabric_equal': 'Yes', 'Persona_correct': 'Yes'}
    storage_host_report_df = drop_all_identical(storage_host_report_df, columns_values, dropna=True)
            
    storage_host_report_df, = dataframe_segmentation(storage_host_report_df, data_names[1:], report_columns_usage_dct, max_title)

    # translate values
    translate_dct = {'Yes': 'Да', 'No': 'Нет'}
    storage_host_report_df = translate_values(storage_host_report_df, translate_dct)

    return storage_host_report_df


def drop_all_na(df, columns: list):
    """Function to drop columns if all values are nan"""

    for column in columns:
        if column in df.columns and df[column].isna().all():
            df.drop(columns=[column], inplace=True)
    return df


def drop_all_identical(df, columns_values: dict, dropna=False):
    """Function to drop columns where all values are equal to certian value.
    dropna parameter defines if nan values for each column should be droppped 
    or not before its checking"""

    for column, value in columns_values.items():
        if column in df.columns:
            if dropna and (df[column].dropna() == value).all():
                df.drop(columns=[column], inplace=True)
            elif not dropna and (df[column] == value).all():
                df.drop(columns=[column], inplace=True)
    return df


def drop_equal_columns(df, columns_pairs: list):
    """Function to drop one of two columns if both have equal values.
    Parameter columns_pairs is a list of tuples. Each tuple contains 
    two columns to check"""

    columns_dropped_lst = []
    for column_main, column_dropped in columns_pairs:
        if column_main in df.columns and column_dropped in df.columns:
            if df[column_main].equals(df[column_dropped]):
                columns_dropped_lst.append(column_dropped)

    if columns_dropped_lst:
        df.drop(columns=columns_dropped_lst, inplace=True)   
    return df


def drop_equal_columns_pairs(df, columns_main: list, columns_droped: list, dropna=False):
    """Function to check if values from columns_main and columns_droped columns are respectively equal to each other.
    If they are then drop columns_droped columns. dropna parameter defines if nan values in columns_droped
    columns should be dropped or not before checking"""

    # create DataFrame copy in case if dropna is reaquired
    check_df = df.copy()
    # check if columns are in the DataFrame
    columns_main = [column for column in columns_main if column in check_df.columns]
    columns_droped = [column for column in columns_droped if column in check_df.columns]
    
    if len(columns_main) != len(columns_droped):
        print('Checked and main columns quantity must be equal')
        exit()

    if dropna:
        check_df.dropna(subset = columns_droped, inplace=True)

    # by default columns are droped
    drop_columns = True
    # if any pair of checked columns are not equal then columns are not droped
    for column_main, column_droped in zip(columns_main, columns_droped):
        if not check_df[column_main].equals(check_df[column_droped]):
            drop_columns = False

    if drop_columns:
        df.drop(columns=columns_droped, inplace=True)
    
    return df




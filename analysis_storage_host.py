"""Module to create storage hosts DataFrame"""

import numpy as np
import pandas as pd

from common_operations_dataframe import (convert_wwn, dataframe_fillna,
                                         dataframe_segmentation, replace_wwnn,
                                         sequential_equality_note,
                                         translate_values)
from common_operations_dataframe_presentation import (
    dataframe_segmentation, dataframe_slice_concatenate, drop_all_identical,
    drop_column_if_all_na, drop_equal_columns, drop_equal_columns_pairs, remove_duplicates_from_column,
    translate_values)
from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_table_report import dataframe_to_report


def storage_host_analysis_main(host_3par_df, system_3par_df, port_3par_df, 
                                portshow_aggregated_df, zoning_aggregated_df, 
                                report_creation_info_lst):
    """Main function to analyze storage port configuration"""
        
    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_dct = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['storage_host_aggregated', 'Презентация', 'Презентация_A&B']

    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_constant_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    storage_host_aggregated_df, storage_host_report_df,  storage_host_compare_report_df = data_lst

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
        storage_host_report_df, storage_host_compare_report_df = \
            storage_host_report(storage_host_aggregated_df, data_names, report_columns_usage_dct, max_title)
        # create list with partitioned DataFrames
        data_lst = [storage_host_aggregated_df, storage_host_report_df, storage_host_compare_report_df]
        # saving data to json or csv file
        save_data(report_constant_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        # storage_host_aggregated_df, = verify_data(report_constant_lst, data_names, *data_lst)
        storage_host_aggregated_df, storage_host_report_df, storage_host_compare_report_df \
            = verify_data(report_constant_lst, data_names, *data_lst)
        data_lst = [storage_host_aggregated_df, storage_host_report_df, storage_host_compare_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dataframe_to_report(data_frame, data_name, report_creation_info_lst)
    return storage_host_aggregated_df


def storage_host_aggregation(host_3par_df, system_3par_df, port_3par_df, portshow_aggregated_df, zoning_aggregated_df):
    """Function to create aggregated storage host presentation DataFrame"""

    if system_3par_df.empty:
        return pd.DataFrame()
    
    storage_host_aggregated_df = host_3par_df.copy()
    # add system_name
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, system_3par_df, 
                                                    join_lst=['configname'], filled_lst=['System_Name'])
    # add controller's ports Wwnp and Wwnp
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, port_3par_df, 
                                                    join_lst=['configname', 'Storage_Port'], filled_lst=['NodeName', 'PortName'])
    # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
    storage_host_aggregated_df = convert_wwn(storage_host_aggregated_df, ['Host_Wwn', 'NodeName', 'PortName'])
    # add controllers ports Fabric_name and Fabric_label
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, 
                                                    join_lst=['PortName'], filled_lst=['Fabric_name', 'Fabric_label'])
    # rename controllers NodeName and PortName
    rename_columns = {'NodeName': 'Storage_Port_Wwnn', 'PortName': 'Storage_Port_Wwnp'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)
    # 'clean' Wwn column to have Wwnp only. check Wwnn -> Wwnp correspondance in all fabrics
    
    # TO_REMOVE check Wwnn -> Wwnp correspondance in all fabrics
    # storage_host_aggregated_df = replace_wwnn(storage_host_aggregated_df, 'Host_Wwn', 
    #                                             portshow_aggregated_df, ['NodeName', 'PortName'], 
    #                                             fabric_columns = ['Fabric_name', 'Fabric_label'])
    storage_host_aggregated_df = replace_wwnn(storage_host_aggregated_df, 'Host_Wwn', 
                                                portshow_aggregated_df, ['NodeName', 'PortName'])

    # add Host Wwnp zoning device status in fabric of storage port connection
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, zoning_aggregated_df, 
                                                join_lst=['Fabric_name', 'Fabric_label', 'PortName'], 
                                                filled_lst=['Fabric_device_status'])

    # rename controllers Fabric_name and Fabric_label
    rename_columns = {'Fabric_name': 'Storage_Fabric_name', 'Fabric_label': 'Storage_Fabric_label', 
                        'Fabric_device_status': 'Fabric_host_status'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)

    # add host information
    host_columns = ['Fabric_name', 'Fabric_label', 'chassis_name', 'switchName', 'Index_slot_port', 'Connected_portId', 
                    'Device_Host_Name', 'Device_Port', 'Host_OS', 'Device_Location', 
                    'Device_Host_Name_per_fabric_name_and_label',	'Device_Host_Name_per_fabric_label', 
                    'Device_Host_Name_per_fabric_name', 'Device_Host_Name_total_fabrics']
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, 
                                                    join_lst=['PortName'], filled_lst=host_columns, remove_duplicates=False)

    # rename host columns
    rename_columns = {'Fabric_name': 'Host_Fabric_name', 'Fabric_label': 'Host_Fabric_label', 'PortName': 'Host_Wwnp'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)

    # verify if host and storage ports are in the same fabric
    storage_host_aggregated_df = sequential_equality_note(storage_host_aggregated_df, 
                                                            ['Host_Fabric_name', 'Host_Fabric_label'], 
                                                            ['Storage_Fabric_name', 'Storage_Fabric_label'],
                                                            'Host_Storage_Fabric_equal')
    # verify persona (host mode) is defined in coreespondence with host os
    storage_host_aggregated_df = verify_host_mode(storage_host_aggregated_df)
    # verify if storage port and host port are zoned
    storage_host_aggregated_df = verify_storage_host_zoning(storage_host_aggregated_df, zoning_aggregated_df)
    # sort aggregated DataFrame
    sort_columns = ['System_Name', 'Host_Id', 'Host_Name', 'Storage_Port']
    storage_host_aggregated_df.sort_values(by=sort_columns, inplace=True)
    # create storage name column free of duplicates
    storage_host_aggregated_df = remove_duplicates_from_column(storage_host_aggregated_df, 'System_Name',
                                                                duplicates_subset=['System_Name'], ) 
    return storage_host_aggregated_df


def verify_host_mode(storage_host_aggregated_df):
    """Function to verify if persona (storage host mode) is defined in correspondence with host os"""

    os_lst = ['vmware', 'windows', 'linux']
    # cumulative host mode mask
    mask_persona_correct = None
    for os_type in os_lst:
        # host mode matches os name except for linux
        os_mode = os_type if os_type != 'linux' else 'generic'
        # mask for current os
        mask_os = (storage_host_aggregated_df['Persona'].str.lower().str.contains(os_mode) & \
                    storage_host_aggregated_df['Host_OS'].str.lower().str.contains(os_type))
        # add current mask to cumulative mask
        if mask_persona_correct is None:
            mask_persona_correct = mask_os
        else:
            mask_persona_correct = mask_persona_correct | mask_os

    # perform checking for rows with existing data only both in Persona and Host_OS columns
    mask_os_notna = storage_host_aggregated_df[['Persona', 'Host_OS']].notna().all(axis=1)
    storage_host_aggregated_df['Persona_correct'] = \
        np.select([mask_os_notna & mask_persona_correct, mask_os_notna & ~mask_persona_correct], ['Yes', 'No'], default=pd.NA)
    # replace pd.NA values
    storage_host_aggregated_df.fillna(np.nan, inplace=True)
    
    return storage_host_aggregated_df


def verify_storage_host_zoning(storage_host_aggregated_df, zoning_aggregated_df):
    """Function to verify if storage port and host port are zoned"""
    
    # prepare zoning (slice effective zoning and local or imported ports only)
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'remote_imported'])
    mask_effective = zoning_aggregated_df['cfg_type'] == 'effective'
    zoning_valid_df = zoning_aggregated_df.loc[mask_effective & mask_connected].copy()
    # find zones with 3PAR storages only to reduce search time
    group_columns = ['Fabric_name', 'Fabric_label', 'zone']
    zone_3par_df = \
        zoning_valid_df.groupby(by=group_columns).filter(lambda zone: zone['deviceSubtype'].str.lower().isin(['3par']).any())

    storage_host_aggregated_df['zone'] = \
        storage_host_aggregated_df.apply(lambda series: find_zones(series, zone_3par_df), axis=1)

    return storage_host_aggregated_df


def find_zones(series, zoning_valid_df):
    """Auxiliary function for verify_storage_host_zoning fn 
    to find zones in effective configuration with storage port and server"""
    
    # verify rows where storage port and server are in same fabric only
    if series['Fabric_host_status'] in  ['local', 'remote_imported']: 
        group_columns = ['Fabric_name', 'Fabric_label', 'zone']
        storage_host_sr = series[['Storage_Port_Wwnp', 'Host_Wwnp']]

        # excessive step zoning_valid_df is already filtered TO_REMOVE 
        mask_same_fabic = (zoning_valid_df['Fabric_name'] == series['Storage_Fabric_name']) & \
                            (zoning_valid_df['Fabric_label'] == series['Storage_Fabric_label'])
        zoning_valid_fabric_df = zoning_valid_df.loc[mask_same_fabic].copy()
        
        # find zones with storage port wwnp and host wwnp
        storage_host_zone_df = \
            zoning_valid_fabric_df.groupby(by=group_columns).filter(lambda zone: storage_host_sr.isin(zone['PortName']).all())
        # get zones defined in the same fabric as storage port connection fabric
        mask_same_fabic = (storage_host_zone_df['Fabric_name'] == series['Storage_Fabric_name']) & \
                            (storage_host_zone_df['Fabric_label'] == series['Storage_Fabric_label'])
        zoning_valid_df = storage_host_zone_df.loc[mask_same_fabic].copy()
        # if zones are found return string of zones separated by commas
        if not zoning_valid_df.empty:
            zone_sr = zoning_valid_df['zone'].drop_duplicates()
            zones_str = ', '.join(zone_sr.to_list())
            return zones_str


def storage_host_report(storage_host_aggregated_df, data_names, report_columns_usage_dct, max_title):
    """Function to create storage_host and storage_host fabric_label comparision DataFrames"""

    if storage_host_aggregated_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    storage_host_report_df = storage_host_aggregated_df.copy()
    # dataframe where hosts and storage port are in the same fabric or host imported to storage fabric
    mask_local_imported = storage_host_aggregated_df['Fabric_host_status'].isin(['local', 'remote_imported'])
    storage_host_valid_df = storage_host_aggregated_df.loc[mask_local_imported].copy()

    # drop uninformative columns 
    storage_host_report_df = clean_storage_host(storage_host_report_df)
    storage_host_valid_df = clean_storage_host(storage_host_valid_df)
    # slice required columns and translate column names

    storage_host_report_df, = dataframe_segmentation(storage_host_report_df, data_names[1:2], report_columns_usage_dct, max_title)
    storage_host_valid_df, = dataframe_segmentation(storage_host_valid_df, data_names[1:2], report_columns_usage_dct, max_title)
    # translate values in columns
    translate_dct = {'Yes': 'Да', 'No': 'Нет'}
    storage_host_report_df = translate_values(storage_host_report_df, translate_dct)
    storage_host_valid_df = translate_values(storage_host_valid_df, translate_dct)
    # create comparision storage_host DataFrame based on Fabric_labels
    slice_column = 'Подсеть' if 'Подсеть' in storage_host_valid_df.columns else 'Подсеть порта массива'
    storage_host_compare_report_df = dataframe_slice_concatenate(storage_host_valid_df, column=slice_column)
    return storage_host_report_df, storage_host_compare_report_df


def clean_storage_host(df):
    """Function to clean storage_host and storage_host_valid (storage port and host are in the same fabric)"""

    # drop second column in each tuple of the list if values in columns of the tuple are equal
    df = drop_equal_columns(df, columns_pairs=[('Host_Wwnp', 'Host_Wwn'), 
                                                ('Device_Host_Name_per_fabric_name_and_label', 'Device_Host_Name_per_fabric_label'),
                                                ('Device_Host_Name_total_fabrics', 'Device_Host_Name_per_fabric_name')])
    # drop empty columns
    df = drop_column_if_all_na(df, ['Device_Port', 'Device_Location'])
    # drop columns where all values are equal to the item value
    columns_values = {'Host_Storage_Fabric_equal': 'Yes', 'Persona_correct': 'Yes', 'Fabric_host_status': 'local'}
    df = drop_all_identical(df, columns_values, dropna=True)
    # drop second pair of Fabric_name, Fabric_label if the columns are respectively equal 
    df = drop_equal_columns_pairs(df, columns_main=['Storage_Fabric_name', 'Storage_Fabric_label'], 
                                        columns_droped=['Host_Fabric_name', 'Host_Fabric_label'], dropna=False)
    # rename first pair of Fabric_name, Fabric_label if second one was droped in prev step
    if not 'Host_Fabric_name' in df.columns:
        df.rename(columns={'Storage_Fabric_name': 'Fabric_name', 'Storage_Fabric_label': 'Fabric_label'}, inplace=True)
    return df


        



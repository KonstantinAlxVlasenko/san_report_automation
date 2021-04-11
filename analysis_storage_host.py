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
    data_names = ['storage_host_aggregated', 'Презентация', 'Презентация_A&B']

    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
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
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        # storage_host_aggregated_df, = verify_data(report_data_lst, data_names, *data_lst)
        storage_host_aggregated_df, storage_host_report_df, storage_host_compare_report_df \
            = verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [storage_host_aggregated_df, storage_host_report_df, storage_host_compare_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)
    return storage_host_aggregated_df


def storage_host_aggregation(host_3par_df, system_3par_df, port_3par_df, portshow_aggregated_df, zoning_aggregated_df):

    if system_3par_df.empty:
        return pd.DataFrame()
    
    storage_host_aggregated_df = host_3par_df.copy()
    # add system_name
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, system_3par_df, join_lst=['configname'], filled_lst=['System_Name'])

    # add controller's ports Wwnp and Wwnp
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, port_3par_df, join_lst=['configname', 'Storage_Port'], filled_lst=['NodeName', 'PortName'])

    # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
    storage_host_aggregated_df = convert_wwn(storage_host_aggregated_df, ['Host_Wwn', 'NodeName', 'PortName'])

    # TO_REMOVE
    # for wwn_column in ['Host_Wwn', 'NodeName', 'PortName']:
    #     if storage_host_aggregated_df[wwn_column].notna().any():
    #         mask_wwn = storage_host_aggregated_df[wwn_column].notna()
    #         storage_host_aggregated_df.loc[mask_wwn, wwn_column] = storage_host_aggregated_df.loc[mask_wwn, wwn_column].apply(lambda wwn: ':'.join(re.findall('..', wwn)))
    #         storage_host_aggregated_df[wwn_column] = storage_host_aggregated_df[wwn_column].str.lower()
        
    # add controllers ports Fabric_name and Fabric_label
    storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, join_lst=['PortName'], filled_lst=['Fabric_name', 'Fabric_label'])

    # rename controllers NodeName and PortName
    rename_columns = {'NodeName': 'Storage_Port_Wwnn', 'PortName': 'Storage_Port_Wwnp'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)

    # 'clean' Wwn column to have Wwnp only
    storage_host_aggregated_df = replace_wwnn(storage_host_aggregated_df, 'Host_Wwn', 
                                                portshow_aggregated_df, ['NodeName', 'PortName'], 
                                                fabric_columns = ['Fabric_name', 'Fabric_label'])

    # TO_REMOVE
    # storage_host_aggregated_df['NodeName'] = storage_host_aggregated_df['Host_Wwn']
    # storage_host_aggregated_df = dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, 
    #                                 join_lst=['Fabric_name', 'Fabric_label', 'NodeName'], 
    #                                 filled_lst=['PortName'], remove_duplicates=False)
    # storage_host_aggregated_df['PortName'].fillna(storage_host_aggregated_df['Host_Wwn'], inplace=True)
    # storage_host_aggregated_df.drop(columns=['NodeName'], inplace=True)

    # rename controllers Fabric_name and Fabric_label
    rename_columns = {'Fabric_name': 'Storage_Fabric_name', 'Fabric_label': 'Storage_Fabric_label'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)

    # add host information
    host_columns = ['Fabric_name', 'Fabric_label', 'chassis_name', 'switchName', 'Index_slot_port', 'Connected_portId', 
                    'Device_Host_Name', 'Device_Port', 'Host_OS', 'Device_Location', 
                    'Device_Host_Name_per_fabric_name_and_label',	'Device_Host_Name_per_fabric_label', 'Device_Host_Name_total_fabrics']
    storage_host_aggregated_df = \
        dataframe_fillna(storage_host_aggregated_df, portshow_aggregated_df, join_lst=['PortName'], filled_lst=host_columns, remove_duplicates=False)

    # rename host columns
    rename_columns = {'Fabric_name': 'Host_Fabric_name', 'Fabric_label': 'Host_Fabric_label', 'PortName': 'Host_Wwnp'}
    storage_host_aggregated_df.rename(columns=rename_columns, inplace=bool)

    # verify if host and storage ports are in the same fabric
    storage_host_aggregated_df = sequential_equality_note(storage_host_aggregated_df, 
                                                            ['Host_Fabric_name', 'Host_Fabric_label'], 
                                                            ['Storage_Fabric_name', 'Storage_Fabric_label'],
                                                            'Host_Storage_Fabric_equal')

    # TO_REMOVE
    # mask_same_fabic = (storage_host_aggregated_df['Host_Fabric_name'] == storage_host_aggregated_df['Storage_Fabric_name']) & \
    #                     (storage_host_aggregated_df['Host_Fabric_label'] == storage_host_aggregated_df['Storage_Fabric_label'])
    # mask_fabric_notna = storage_host_aggregated_df[['Host_Fabric_name', 'Storage_Fabric_name', 'Host_Fabric_label', 'Storage_Fabric_label']].notna().all(axis=1)
    # storage_host_aggregated_df['Host_Storage_Fabric_equal'] = np.select([mask_fabric_notna & mask_same_fabic, mask_fabric_notna & ~mask_same_fabic], ['Yes', 'No'], default=pd.NA)

    # verify persona (host mode) is defined in coreespondence with host os
    storage_host_aggregated_df = verify_host_mode(storage_host_aggregated_df)

    # TO_REMOVE
    # mask_vmware = storage_host_aggregated_df['Persona'].str.lower().str.contains('vmware') & storage_host_aggregated_df['Host_OS'].str.lower().str.contains('vmware')
    # mask_windows = storage_host_aggregated_df['Persona'].str.lower().str.contains('windows') & storage_host_aggregated_df['Host_OS'].str.lower().str.contains('windows')
    # mask_linux = storage_host_aggregated_df['Persona'].str.lower().str.contains('generic') & storage_host_aggregated_df['Host_OS'].str.lower().str.contains('linux')
    # mask_persona_correct = mask_vmware | mask_windows | mask_linux
    # mask_os_notna = storage_host_aggregated_df[['Persona', 'Host_OS']].notna().all(axis=1)
    # storage_host_aggregated_df['Persona_correct'] = np.select([mask_os_notna & mask_persona_correct, mask_os_notna & ~mask_persona_correct], ['Yes', 'No'], default=pd.NA)
    # storage_host_aggregated_df.fillna(np.nan, inplace=True)

    # verify if storage port and host port are zoned
    storage_host_aggregated_df = verify_storage_host_zoning(storage_host_aggregated_df, zoning_aggregated_df)

    # TO_REMOVE
    # # prepare zoning
    # mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'imported'])
    # mask_effective = zoning_aggregated_df['cfg_type'] == 'effective'
    # zoning_valid_df = zoning_aggregated_df.loc[mask_effective & mask_connected].copy()
    # storage_host_aggregated_df['zone'] = \
    #     storage_host_aggregated_df.apply(lambda series: find_zones(series, zoning_valid_df), axis=1)

    # sort aggregated DataFrame
    sort_columns = ['System_Name', 'Host_Id', 'Host_Name', 'Storage_Port']
    storage_host_aggregated_df.sort_values(by=sort_columns, inplace=True)
    return storage_host_aggregated_df


def verify_host_mode(storage_host_aggregated_df):
    # verify persona (host mode) is defined in coreespondence with host os

    os_lst = ['vmware', 'windows', 'linux']
    mask_persona_correct = None
    for os_type in os_lst:
        os_mode = os_type if os_type != 'linux' else 'generic'
        mask_os = (storage_host_aggregated_df['Persona'].str.lower().str.contains(os_mode) & \
                    storage_host_aggregated_df['Host_OS'].str.lower().str.contains(os_type))
        if mask_persona_correct:
            mask_persona_correct = mask_persona_correct | mask_os
        else:
            mask_persona_correct = mask_os

    mask_os_notna = storage_host_aggregated_df[['Persona', 'Host_OS']].notna().all(axis=1)
    storage_host_aggregated_df['Persona_correct'] = \
        np.select([mask_os_notna & mask_persona_correct, mask_os_notna & ~mask_persona_correct], ['Yes', 'No'], default=pd.NA)
    storage_host_aggregated_df.fillna(np.nan, inplace=True)
    
    return storage_host_aggregated_df


def  convert_wwn(df, wwn_columns: list):
    """Function to convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)"""

    for wwn_column in wwn_columns:
        if wwn_column in df.columns and df[wwn_column].notna().any():
            mask_wwn = df[wwn_column].notna()
            df.loc[mask_wwn, wwn_column] = df.loc[mask_wwn, wwn_column].apply(lambda wwn: ':'.join(re.findall('..', wwn)))
            df[wwn_column] = df[wwn_column].str.lower()
    return df


def replace_wwnn(wwn_df, wwn_column: str, wwnn_wwnp_df, wwnn_wwnp_columns: list, fabric_columns: list=[]):
    """Function to replace wwnn in wwn_column (column with presumably mixed wwnn and wwnp values) 
    of wwn_df DataFrame with corresponding wwnp value if wwnn is present. wwnn_wwnp_df DataFrame contains strictly defined 
    wwnn and wwnp values in corresponding columns which passed as wwnn_wwnp_columns parameter.
    fabric_columns contains additional columns if required find wwnp for wwnn in certain fabric only."""
    
    wwnn_column, wwnp_column = wwnn_wwnp_columns
    join_columns = fabric_columns.append(wwnn_column)

    if wwnp_column in wwn_df.columns:
        wwn_df[wwnp_column] = np.nan

    # assume that all values in wwn_column are wwnns
    wwn_df[wwnn_column] = wwn_df[wwn_column]
    # find corresponding wwnp value from wwnn_wwnp_df for each presumed wwnn in wwn_df
    # rows with filled values in wwnp_column have confirmed wwnn value in  wwnn_column column of wwn_df
    wwn_df = dataframe_fillna(wwn_df, wwnn_wwnp_df, 
                                    join_lst=join_columns, 
                                    filled_lst=[wwnp_column], remove_duplicates=False)
    # when rows have empty values in wwnp_column mean wwn doesn't exist in fabric or it is wwnp
    wwn_df[wwnp_column].fillna(wwn_df[wwn_column], inplace=True)
    wwn_df.drop(columns=[wwnn_column], inplace=True)
    return wwn_df


# def sequential_equality(df, columns1: list, columns2: list, result_column: str):

#     mask_lst = [df[column1] == df[column2] for column1, column2 in zip(columns1, columns2)]
#     if mask_lst:
#         mask_equality = (mask_lst[0])
#         if len(mask_lst) > 1:
#             for mask in mask_lst[1:]:
#                 mask_equality = mask_equality & (mask)

#     columns = columns1 + columns2
#     mask_notna = df[columns].notna().all(axis=1)
#     df[result_column] = np.select([mask_notna & mask_equality, mask_notna & ~mask_equality], ['Yes', 'No'], default=pd.NA)

#     return df


def sequential_equality_note(df, columns1: list, columns2: list, note_column: str):
    """Function to check if values in the list of columns1 and columns2 are sequentially equal
    for rows where all values are not na. note_column contains 'Yes' if corresponding values 
    are equal and 'No' if they aren't. Rows with any absent value from columns1 and column2
    have na in note_column"""
    
    # to compare more than two columns for sequantial equality they must have equal column names
    # for corresponding columns. two DataFrames sliced from df DataFrame.
    df1 = df[columns1].copy()
    df2 = df[columns2].copy()
    # rename column names in df2 to correspond names in df1
    rename_dct = {column2: column1 for column1, column2 in zip(columns1, columns2)}
    df2.rename(columns=rename_dct, inplace=True)
    # mask where values in corresponding columns are equal
    mask_equality = (df1 == df2).all(axis=1)
    # only rows with notna values for all columns from columns1 and columns2 lists are taken into account
    columns = columns1 + columns2
    mask_notna = df[columns].notna().all(axis=1)
    # check equality and fill note_column with 'Yes' or 'No'
    df[note_column] = np.select([mask_notna & mask_equality, mask_notna & ~mask_equality], ['Yes', 'No'], default=pd.NA)
    df.fillna(np.nan, inplace=True)

    return df


def verify_storage_host_zoning(storage_host_aggregated_df, zoning_aggregated_df):
    """Function to verify if storage port and host port are zoned"""
    
    # prepare zoning (slice effective zoning and local or imported ports only)
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'imported'])
    mask_effective = zoning_aggregated_df['cfg_type'] == 'effective'
    zoning_valid_df = zoning_aggregated_df.loc[mask_effective & mask_connected].copy()

    storage_host_aggregated_df['zone'] = \
        storage_host_aggregated_df.apply(lambda series: find_zones(series, zoning_valid_df), axis=1)

    return storage_host_aggregated_df


def find_zones(series, zoning_valid_df):
    """Auxiliary function for verify_storage_host_zoning fn 
    to find zones in effective configuration with storage port and server"""
    
    # verify rows where storage port and server are in same fabric only
    if series['Host_Storage_Fabric_equal'] == 'Yes':
        group_columns = ['Fabric_name', 'Fabric_label', 'zone']
        storage_host_sr = series[['Storage_Port_Wwnp', 'Host_Wwnp']]
        
        # find zones with storage port wwnp and host wwnp
        storage_host_zone_df = \
            zoning_valid_df.groupby(by=group_columns).filter(lambda zone: storage_host_sr.isin(zone['PortName']).all())
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
    # dataframe where hosts and storage port are in the same fabric
    mask_same_fabic = storage_host_aggregated_df['Host_Storage_Fabric_equal'] == 'Yes'
    storage_host_valid_df = storage_host_aggregated_df.loc[mask_same_fabic].copy()

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
    storage_host_compare_report_df = dataframe_slice_concatenate(storage_host_valid_df, column='Подсеть')
    return storage_host_report_df, storage_host_compare_report_df


def clean_storage_host(df):
    """Function to clean storage_host and storage_host_valid (storage port and host are in the same fabric)"""

    # drop second column in each tuple of the list if values in columns of the tuple are equal
    df = drop_equal_columns(df, columns_pairs=[('Host_Wwnp', 'Host_Wwn'), 
                                                ('Device_Host_Name_per_fabric_name_and_label', 'Device_Host_Name_per_fabric_label')])
    # drop empty columns
    df = drop_all_na(df, ['Device_Port', 'Device_Location'])
    # drop columns where all values are equal to the item value
    columns_values = {'Host_Storage_Fabric_equal': 'Yes', 'Persona_correct': 'Yes'}
    df = drop_all_identical(df, columns_values, dropna=True)
    # drop second pair of Fabric_name, Fabric_label if the columns are respectively equal 
    df = drop_equal_columns_pairs(df, columns_main=['Storage_Fabric_name', 'Storage_Fabric_label'], 
                                        columns_droped=['Host_Fabric_name', 'Host_Fabric_label'], dropna=False)
    # rename first pair of Fabric_name, Fabric_label if second one was droped in prev step
    if not 'Host_Fabric_name' in df.columns:
        df.rename(columns={'Storage_Fabric_name': 'Fabric_name', 'Storage_Fabric_label': 'Fabric_label'}, inplace=True)
    return df


def drop_all_na(df, columns: list):
    """Function to drop columns if all values are nan"""

    clean_df = df.copy()

    for column in columns:
        if column in df.columns and df[column].isna().all():
            clean_df.drop(columns=[column], inplace=True)
    return clean_df


def drop_all_identical(df, columns_values: dict, dropna=False):
    """Function to drop columns where all values are equal to certian value.
    dropna parameter defines if nan values for each column should be droppped 
    or not before its checking"""

    clean_df = df.copy()    

    for column, value in columns_values.items():
        if column in df.columns:
            if dropna and (df[column].dropna() == value).all():
                clean_df.drop(columns=[column], inplace=True)
            elif not dropna and (df[column] == value).all():
                clean_df.drop(columns=[column], inplace=True)
    return clean_df


def drop_equal_columns(df, columns_pairs: list):
    """Function to drop one of two columns if both have equal values.
    Parameter columns_pairs is a list of tuples. Each tuple contains 
    two columns to check"""

    clean_df = df.copy()
    columns_dropped_lst = []
    for column_main, column_dropped in columns_pairs:
        if column_main in df.columns and column_dropped in df.columns:
            if df[column_main].equals(df[column_dropped]):
                columns_dropped_lst.append(column_dropped)

    if columns_dropped_lst:
        clean_df.drop(columns=columns_dropped_lst, inplace=True)   
    return clean_df


def drop_equal_columns_pairs(df, columns_main: list, columns_droped: list, dropna=False):
    """Function to check if values from columns_main and columns_droped columns are respectively equal to each other.
    If they are then drop columns_droped columns. dropna parameter defines if nan values in columns_droped
    columns should be dropped or not before checking"""

    clean_df = df.copy()
    # create DataFrame copy in case if dropna is required
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
        clean_df.drop(columns=columns_droped, inplace=True)
    
    return clean_df


def dataframe_slice_concatenate(df, column: str):
    """Function to create comparision DataFrame. 
    Initial DataFrame df is sliced based on unique values in designated column.
    Then sliced DataFrames concatenated horizontally which indexes were previously reset."""
    
    if not column in df.columns:
        print('\n')
        print(f"Column {column} doesn't exist")
        return df

    column_values = sorted(df[column].unique().tolist())
    sorted_df_lst = []
    for value in column_values:
        mask_value = df[column] == value
        tmp_df = df.loc[mask_value].copy()
        tmp_df.reset_index(inplace=True, drop=True)
        sorted_df_lst.append(tmp_df.copy())
    
    return pd.concat(sorted_df_lst, axis=1)


        



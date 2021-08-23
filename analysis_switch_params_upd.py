"""
Module to generate aggregated switch parameters table and 
'Switches list', 'Fabric list', 'Fabric global parameters', 
'Switches parameters', 'Licenses' report tables
"""

import numpy as np

from analysis_switch_statistics import fabric_switch_statistics
from analysis_switch_aggregation import switch_param_aggregation
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_servicefile import dataframe_import, data_extract_objects, dct_from_columns
from common_operations_dataframe_presentation import dataframe_segmentation, translate_values


def switch_params_analysis_main(fabricshow_ag_labels_df, chassis_params_df, 
                                switch_params_df, maps_params_df, blade_module_loc_df, ag_principal_df, report_data_lst):
    """Main function to create aggregated switch parameters table and report tables"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['report_columns_usage', 'switch_params_aggregated', 'fabric_switch_statistics', 
                    'Коммутаторы', 'Фабрика', 'Параметры_коммутаторов', 'MAPS', 'Лицензии', 
                    'Глобальные_параметры_фабрики', 'Статистика_коммутаторов']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    report_columns_usage_dct, switch_params_aggregated_df, fabric_switch_statistics_df, switches_report_df, fabric_report_df, \
        switches_parameters_report_df, maps_report_df, licenses_report_df, global_fabric_parameters_report_df, fabric_switch_statistics_report_df  = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['chassis_parameters', 'switch_parameters', 'switchshow_ports', 
                            'maps_parameters', 'blade_interconnect', 'fabric_labels', 'fabricshow_summary']

    # clean fabricshow DataFrame from unneccessary data
    fabric_clean_df = fabric_clean(fabricshow_ag_labels_df)
    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:

        # data imported from init file (regular expression patterns) to extract values from data columns
        # re_pattern list contains comp_keys, match_keys, comp_dct    
        _, _, *re_pattern_lst = data_extract_objects('common_regex', max_title)

        # import data with switch models, firmware and etc
        switch_models_df = dataframe_import('switch_models', max_title)

        # current operation information string
        info = f'Generating aggregated switch parameters table'
        print(info, end =" ") 

        # create aggregated table by joining DataFrames
        switch_params_aggregated_df, report_columns_usage_dct = \
            switch_param_aggregation(fabric_clean_df, chassis_params_df, \
                switch_params_df, maps_params_df, switch_models_df, ag_principal_df, re_pattern_lst)
        # add 'Device_Location for Blade chassis switches
        switch_params_aggregated_df = fill_device_location(switch_params_aggregated_df, blade_module_loc_df)

        # after finish display status
        status_info('ok', max_title, len(info))

        # current operation information string
        info = f'Counting switch statistics'
        print(info, end =" ") 
        fabric_switch_statistics_df = fabric_switch_statistics(switch_params_aggregated_df, re_pattern_lst)
        # after finish display status
        status_info('ok', max_title, len(info))        

        # check if switch config files missing
        mask_fabric = switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].notna().all(axis=1)
        mask_no_config = switch_params_aggregated_df['chassis_name'].isna()
        missing_configs_num = switch_params_aggregated_df.loc[mask_no_config]['Fabric_name'].count()
        if missing_configs_num:
            info = f'{missing_configs_num} switch configuration{"s" if missing_configs_num > 1 else ""} MISSING'
            print(info, end =" ")
            status_info('warning', max_title, len(info))

        switches_report_df, fabric_report_df, switches_parameters_report_df, \
            maps_report_df, licenses_report_df, global_fabric_parameters_report_df, fabric_switch_statistics_report_df = \
                switchs_params_report(switch_params_aggregated_df, fabric_switch_statistics_df, data_names, report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        data_lst = [report_columns_usage_dct, switch_params_aggregated_df, fabric_switch_statistics_df,
                    switches_report_df, fabric_report_df, 
                    switches_parameters_report_df, maps_report_df, licenses_report_df,
                    global_fabric_parameters_report_df, fabric_switch_statistics_report_df]

        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        report_columns_usage_dct, switch_params_aggregated_df, fabric_switch_statistics_df, switches_report_df, fabric_report_df,  \
            switches_parameters_report_df, maps_report_df, licenses_report_df, global_fabric_parameters_report_df, fabric_switch_statistics_report_df = verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [report_columns_usage_dct, switch_params_aggregated_df, fabric_switch_statistics_df,
                    switches_report_df, fabric_report_df, 
                    switches_parameters_report_df, maps_report_df, licenses_report_df,
                    global_fabric_parameters_report_df, fabric_switch_statistics_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names[1:], data_lst[1:]):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return report_columns_usage_dct, switch_params_aggregated_df, fabric_clean_df


def fabric_clean(fabricshow_ag_labels_df):
    """Function to prepare fabricshow_ag_labels DataFrame for join operation"""

    # create copy of fabricshow_ag_labels DataFrame
    fabric_clean_df = fabricshow_ag_labels_df.copy()
    # remove switches which are not part of research 
    # (was not labeled during Fabric labeling procedure)
    # fabric_clean_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)

    # remove Front and Translate Domain switches
    mask = fabric_clean_df.Enet_IP_Addr != '0.0.0.0'
    fabric_clean_df = fabric_clean_df.loc[mask]
    # reset fabrics DataFrame index after droping switches
    fabric_clean_df.reset_index(inplace=True, drop=True)
    # extract required columns
    fabric_clean_df = fabric_clean_df.loc[:, ['Fabric_name', 'Fabric_label', 'Worldwide_Name', 'Name', 'Enet_IP_Addr',	'SwitchMode']]
    # rename columns as in switch_params DataFrame
    fabric_clean_df.rename(columns={'Worldwide_Name': 'switchWwn', 'Name': 'switchName'}, inplace=True)
    return fabric_clean_df


def fill_device_location(switch_params_aggregated_df, blade_module_loc_df):
    """Function to add 'Device_Location for Blade chassis switches"""

    if not blade_module_loc_df.empty:
        # convert Blade modules DataFrane to the two columns (serial number and chasiss name - bay number) DataFrame
        blade_module_sn_loc_df = blade_module_loc_df.loc[:, ['Interconnect_SN', 'Device_Location']]
        blade_module_sn_loc_df.dropna(subset = ['Interconnect_SN'], inplace = True)
        blade_module_sn_loc_df.drop_duplicates(subset = ['Interconnect_SN'], inplace = True)
        blade_module_sn_loc_df.rename(columns = {'Interconnect_SN': 'ssn'}, inplace = True)
        # add Device_Location to the switch_aggregated_df DataFrame
        switch_params_aggregated_df = switch_params_aggregated_df.merge(blade_module_sn_loc_df, how = 'left', on = 'ssn')
    else:
        # if no logs for BladeSystem then add location column with NaN values
        switch_params_aggregated_df = \
            switch_params_aggregated_df.reindex(columns = [*switch_params_aggregated_df.columns.to_list(), 'Device_Location'])
    return switch_params_aggregated_df


def switchs_params_report(switch_params_aggregated_df, fabric_switch_statistics_df, data_names, report_columns_usage_dct, max_title):
    """Function to create switch related report tables"""

    # partition aggregated DataFrame to required tables
    switches_report_df, fabric_report_df,  \
        switches_parameters_report_df, maps_report_df, licenses_report_df = \
            dataframe_segmentation(switch_params_aggregated_df, data_names[3:-2], \
                report_columns_usage_dct, max_title)

    maps_report_df.replace(to_replace={'No FV lic': np.nan}, inplace=True)
    # maps_report_df.dropna(axis=1, how = 'all', inplace=True)

    # global parameters are equal for all switches in one fabric thus checking Principal switches only
    mask_principal = switch_params_aggregated_df['switchRole'] == 'Principal'
    mask_valid_fabric = ~switch_params_aggregated_df['Fabric_name'].isin(['x', '-'])
    switch_params_principal_df = switch_params_aggregated_df.loc[mask_principal & mask_valid_fabric].copy()
    global_fabric_parameters_report_df, = dataframe_segmentation(switch_params_principal_df, data_names[-2], \
                report_columns_usage_dct, max_title)            

    # drop rows with empty switch names columns
    fabric_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
    switches_parameters_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
    licenses_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)

    # drop fabric_id if all have same value
    if fabric_report_df['Fabric ID'].dropna().nunique() == 1:
        fabric_report_df.drop(columns=['Fabric ID'], inplace=True)
    # drop Fabric_Name (not Fabric_name) if column is empty
    if fabric_report_df['Название фабрики'].isna().all():
        fabric_report_df.drop(columns=['Название фабрики'], inplace=True)
       
    global_fabric_parameters_report_df.reset_index(inplace=True, drop=True)

    # fabric switch statistics
    fabric_switch_statistics_report_df = fabric_switch_statistics_df.copy()
    translate_dct = dct_from_columns('customer_report', max_title, 'Статистика_коммутаторов_перевод_eng', 
                                'Статистика_коммутаторов_перевод_ru', init_file = 'san_automation_info.xlsx')
    fabric_switch_statistics_report_df = translate_values(fabric_switch_statistics_report_df, translate_dct)
    # translate column names
    fabric_switch_statistics_report_df.rename(columns=translate_dct, inplace=True)
    return switches_report_df, fabric_report_df, switches_parameters_report_df, \
                maps_report_df, licenses_report_df, global_fabric_parameters_report_df, fabric_switch_statistics_report_df



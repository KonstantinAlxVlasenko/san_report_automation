"""
Module to generate aggregated switch parameters table and 
'Switches list', 'Fabric list', 'Fabric global parameters', 
'Switches parameters', 'Licenses' report tables
"""

import numpy as np
import pandas as pd

from common_operations_dataframe import dataframe_segmentation, dataframe_fillna
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_servicefile import dataframe_import


def switch_params_analysis_main(fabricshow_ag_labels_df, chassis_params_df, 
                                switch_params_df, maps_params_df, blade_module_loc_df, ag_principal_df, report_data_lst):
    """Main function to create aggregated switch parameters table and report tables"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['report_columns_usage', 'switch_params_aggregated', 'Коммутаторы', 'Фабрика', 
                    'Глобальные_параметры_фабрики', 'Параметры_коммутаторов', 'Лицензии']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    report_columns_usage_dct, switch_params_aggregated_df, switches_report_df, fabric_report_df, \
        global_fabric_parameters_report_df, switches_parameters_report_df, licenses_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['chassis_parameters', 'switch_parameters', 'switchshow_ports', 
                            'maps_parameters', 'blade_interconnect', 'fabric_labels']

    # clean fabricshow DataFrame from unneccessary data
    fabric_clean_df = fabric_clean(fabricshow_ag_labels_df)
    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:

        # import data with switch models, firmware and etc
        switch_models_df = dataframe_import('switch_models', max_title)

        # current operation information string
        info = f'Generating aggregated switch parameters table'
        print(info, end =" ") 


        # create aggregated table by joining DataFrames
        switch_params_aggregated_df, report_columns_usage_dct = \
            fabric_aggregation(fabric_clean_df, chassis_params_df, \
                switch_params_df, maps_params_df, switch_models_df, ag_principal_df)
        # add 'Device_Location for Blade chassis switches
        switch_params_aggregated_df = fill_device_location(switch_params_aggregated_df, blade_module_loc_df)

        # after finish display status
        status_info('ok', max_title, len(info))


        # partition aggregated DataFrame to required tables
        switches_report_df, fabric_report_df, global_fabric_parameters_report_df, \
            switches_parameters_report_df, licenses_report_df = \
                dataframe_segmentation(switch_params_aggregated_df, data_names[2:], \
                    report_columns_usage_dct, max_title)            

        # drop rows with empty switch names columns
        fabric_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
        switches_parameters_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
        licenses_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)

        # parameters are equal for all switches in one fabric
        if report_columns_usage_dct['fabric_name_usage']:
            global_fabric_parameters_report_df.drop_duplicates(subset=['Фабрика', 'Подсеть'], inplace=True)
        else:
            global_fabric_parameters_report_df.drop_duplicates(subset=['Подсеть'], inplace=True)
        global_fabric_parameters_report_df.reset_index(inplace=True, drop=True)      

        # create list with partitioned DataFrames
        data_lst = [report_columns_usage_dct, switch_params_aggregated_df, 
                    switches_report_df, fabric_report_df, 
                    global_fabric_parameters_report_df, 
                    switches_parameters_report_df, licenses_report_df]

        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        report_columns_usage_dct, switch_params_aggregated_df, switches_report_df, fabric_report_df, global_fabric_parameters_report_df, \
            switches_parameters_report_df, licenses_report_df = verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [report_columns_usage_dct, switch_params_aggregated_df, 
                    switches_report_df, fabric_report_df, 
                    global_fabric_parameters_report_df, 
                    switches_parameters_report_df, licenses_report_df]
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


def ag_switch_info(switch_params_aggregated_df, ag_principal_df):
    """Function to add AG switches and VC switchtype, fw version to switch lists"""

    ag_columns_lst = ['AG_Switch_WWN', 'AG_Switch_Type', 'AG_Switch_Firmware_Version']
    switch_columns_lst = ['switchWwn', 'switchType', 'FOS_version']
    ag_translate_dct = dict(zip(ag_columns_lst, switch_columns_lst))
    ag_fw_type_df = ag_principal_df.copy()
    ag_fw_type_df = ag_fw_type_df.loc[:, ag_columns_lst]
    ag_fw_type_df.rename(columns=ag_translate_dct, inplace=True)

    switch_params_aggregated_df = \
        dataframe_fillna(switch_params_aggregated_df, ag_fw_type_df, join_lst=switch_columns_lst[0:1], filled_lst=switch_columns_lst[1:])

    return switch_params_aggregated_df


def fabric_aggregation(fabric_clean_df, chassis_params_df, switch_params_df, maps_params_df, switch_models_df, ag_principal_df):
    """Function to complete fabric DataFrame with information from 
    chassis_params_fabric, switch_params, maps_params DataFrames """

    # complete fabric DataFrame with information from switch_params DataFrame
    switch_params_aggregated_df = fabric_clean_df.merge(switch_params_df, how = 'left', on = ['switchWwn', 'switchName'])
    switch_params_aggregated_df['SwitchName'].fillna(switch_params_aggregated_df['switchName'], inplace=True)
    switch_params_aggregated_df['switchName'].fillna(switch_params_aggregated_df['SwitchName'], inplace=True)
    switch_params_aggregated_df['boot.ipa'].fillna(switch_params_aggregated_df['Enet_IP_Addr'], inplace=True)
    switch_params_aggregated_df['switchMode'].fillna(switch_params_aggregated_df['SwitchMode'], inplace=True)

    # complete f_s DataFrame with information from chassis_params DataFrame
    switch_params_aggregated_df = switch_params_aggregated_df.merge(chassis_params_df, how = 'left', on=['configname', 'chassis_name', 'chassis_wwn'])

    switch_params_aggregated_df = ag_switch_info(switch_params_aggregated_df, ag_principal_df)

    # convert switch_index in f_s_c and maps_params DataFrames to same type
    maps_params_df.switch_index = maps_params_df.switch_index.astype('float64', errors='ignore')
    switch_params_aggregated_df.switch_index = switch_params_aggregated_df.switch_index.astype('float64', errors='ignore')
    # complete f_s_c DataFrame with information from maps_params DataFrame
    switch_params_aggregated_df = switch_params_aggregated_df.merge(maps_params_df, how = 'left', on = ['configname', 'chassis_name', 'switch_index'])
    # convert switchType in f_s_c_m and switch_models DataFrames to same type
    # convert f_s_c_m_df.switchType from string to float
    switch_params_aggregated_df.switchType = switch_params_aggregated_df.switchType.astype('float64', errors='ignore')
    # remove fractional part from f_s_c_m_df.switchType
    switch_params_aggregated_df.switchType = np.floor(switch_params_aggregated_df.switchType)
    switch_models_df.switchType = switch_models_df.switchType.astype('float64', errors='ignore')
    # complete f_s_c_m DataFrame with information from switch_models DataFrame
    switch_params_aggregated_df = switch_params_aggregated_df.merge(switch_models_df, how='left', on='switchType')

    # sorting DataFrame
    switch_params_aggregated_df.sort_values(by=['Fabric_name', 'Fabric_label', 'switchType', 'chassis_name', 'switch_index'], \
        ascending=[True, True, False, True, True], inplace=True)
    # reset index values
    switch_params_aggregated_df.reset_index(inplace=True, drop=True)

    # add empty column FOS suuported to fill manually 
    switch_params_aggregated_df['FW_Supported'] = pd.Series()

    # license check
    license_dct = {'Trunking_license': 'Trunking', 'Fabric_Vision_license': 'Fabric Vision'}
    for lic_check, lic_name in license_dct.items():
        switch_params_aggregated_df[lic_check] = \
            switch_params_aggregated_df.loc[switch_params_aggregated_df['licenses'].notnull(), 'licenses'].apply(lambda x: lic_name in x)
        switch_params_aggregated_df[lic_check].replace(to_replace={True: 'Да', False: 'Нет'}, inplace = True)

    # check if chassis_name and switch_name columns are equal
    # if yes then no need to use chassis information in tables
    # remove switches with unparsed data
    # chassis_names_check_df = switch_params_aggregated_df.dropna(subset=['chassis_name', 'SwitchName'], how = 'all')
    chassis_names_check_df = switch_params_aggregated_df.dropna(subset=['chassis_name'], how = 'all')
    if all(chassis_names_check_df.chassis_name == chassis_names_check_df.SwitchName):
        chassis_column_usage = False
    else:
        chassis_column_usage = True
    # Check number of Fabric_names. 
    # If there is only one Fabric_name then no need to use Fabric_name column in report Dataframes
    fabric_name_usage = True if switch_params_aggregated_df.Fabric_name.nunique() > 1 else False
        
    report_columns_usage_dct = {'fabric_name_usage': fabric_name_usage, 'chassis_info_usage': chassis_column_usage}
    
    return switch_params_aggregated_df, report_columns_usage_dct


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
        switch_params_aggregated_df = switch_params_aggregated_df.reindex(columns = [*switch_params_aggregated_df.columns.to_list(), 'Device_Location'])

    return switch_params_aggregated_df

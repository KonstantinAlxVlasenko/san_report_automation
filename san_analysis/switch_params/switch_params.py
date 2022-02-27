"""
Module to generate aggregated switch parameters table and 
'Switches list', 'Fabric list', 'Fabric global parameters', 
'Switches parameters', 'Licenses' report tables
"""

import numpy as np
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .switch_aggregation import switch_param_aggregation
from .switch_statistics import fabric_switch_statistics


def switch_params_analysis(fabricshow_ag_labels_df, chassis_params_df, 
                                switch_params_df, maps_params_df, blade_module_loc_df, ag_principal_df, report_creation_info_lst):
    """Main function to create aggregated switch parameters table and report tables"""
    
    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['report_columns_usage', 'switch_params_aggregated']

    # data_names = ['report_columns_usage', 'switch_params_aggregated', 'fabric_switch_statistics', 
    #                 'Коммутаторы', 'Фабрика', 'Параметры_коммутаторов', 'MAPS', 'Лицензии', 
    #                 'Глобальные_параметры_фабрики', 'Статистика_коммутаторов']


    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # reade data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)

    # list of data to analyze from report_info table
    analyzed_data_names = ['chassis_parameters', 'switch_parameters', 'switchshow_ports', 
                            'maps_parameters', 'blade_interconnect', 'fabric_labels', 'fabricshow_summary']

    # clean fabricshow DataFrame from unneccessary data
    fabric_clean_df = fabric_clean(fabricshow_ag_labels_df)
    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:

        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, _ = sfop.regex_pattern_import('common_regex', max_title)
        # import data with switch models, firmware and etc
        switch_models_df = sfop.dataframe_import('switch_models', max_title)

        # current operation information string
        info = f'Generating aggregated switch parameters table'
        print(info, end =" ") 

        # create aggregated table by joining DataFrames
        switch_params_aggregated_df, report_columns_usage_dct = \
            switch_param_aggregation(fabric_clean_df, chassis_params_df, \
                switch_params_df, maps_params_df, switch_models_df, ag_principal_df, pattern_dct)

        report_creation_info_lst.append(report_columns_usage_dct)

        # add 'Device_Location for Blade chassis switches
        switch_params_aggregated_df = fill_device_location(switch_params_aggregated_df, blade_module_loc_df)

        # after finish display status
        meop.status_info('ok', max_title, len(info))    

        # check if switch config files missing
        # mask_fabric = switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].notna().all(axis=1)
        mask_valid_fabric = ~switch_params_aggregated_df[['Fabric_name', 'Fabric_label']].isin(['-', 'x']).any(axis=1)
        mask_no_config = switch_params_aggregated_df['configname'].isna()
        mask_no_fd_xd = switch_params_aggregated_df['Enet_IP_Addr'] != '0.0.0.0'
        missing_configs_num = switch_params_aggregated_df.loc[mask_valid_fabric & mask_no_config & mask_no_fd_xd, 'switchWwn'].count()
        if missing_configs_num:
            info = f'{missing_configs_num} switch configuration{"s" if missing_configs_num > 1 else ""} MISSING'
            print(info, end =" ")
            meop.status_info('warning', max_title, len(info))

        # create list with partitioned DataFrames
        data_lst = [report_columns_usage_dct, switch_params_aggregated_df]
        # writing data to sql
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)
    
    else:
        # verify if loaded data is empty and replace information string with empty DataFrame
        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        report_columns_usage_dct, switch_params_aggregated_df, *_ = data_lst

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names[1:], data_lst[1:]):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)

    return report_columns_usage_dct, switch_params_aggregated_df, fabric_clean_df


def fabric_clean(fabricshow_ag_labels_df):
    """Function to prepare fabricshow_ag_labels DataFrame for join operation"""

    # create copy of fabricshow_ag_labels DataFrame
    fabric_clean_df = fabricshow_ag_labels_df.copy()
    # remove switches which are not part of research 
    # (was not labeled during Fabric labeling procedure)
    # fabric_clean_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)

    # # remove Front and Translate Domain switches
    # mask = fabric_clean_df.Enet_IP_Addr != '0.0.0.0'
    # fabric_clean_df = fabric_clean_df.loc[mask]

    # reset fabrics DataFrame index after droping switches
    fabric_clean_df.reset_index(inplace=True, drop=True)
    # extract required columns
    fabric_clean_df = fabric_clean_df.loc[:, ['Fabric_name', 'Fabric_label', 'Worldwide_Name', 'Domain_ID', 'Name', 'Enet_IP_Addr',	'SwitchMode']]
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


# def switchs_params_report(switch_params_aggregated_df, fabric_switch_statistics_df, report_headers_df, report_columns_usage_dct, data_names):
#     """Function to create switch related report tables"""

#     switches_report_df, fabric_report_df,  \
#         switches_parameters_report_df, maps_report_df, licenses_report_df = \
#             dfop.generate_report_dataframe(switch_params_aggregated_df, report_headers_df, report_columns_usage_dct, *data_names[3:-2])

#     maps_report_df.replace(to_replace={'No FV lic': np.nan}, inplace=True)

#     # global parameters are equal for all switches in one fabric thus checking Principal switches only
#     mask_principal = switch_params_aggregated_df['switchRole'] == 'Principal'
#     mask_valid_fabric = ~switch_params_aggregated_df['Fabric_name'].isin(['x', '-'])
#     switch_params_principal_df = switch_params_aggregated_df.loc[mask_principal & mask_valid_fabric].copy()
#     global_fabric_parameters_report_df = dfop.generate_report_dataframe(switch_params_principal_df, report_headers_df, 
#                                                                 report_columns_usage_dct, data_names[-2])

#     # drop rows with empty switch names columns
#     fabric_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
#     fabric_report_df = dfop.translate_values(fabric_report_df, report_headers_df, 'Коммутаторы_перевод')
#     fabric_report_df = dfop.drop_column_if_all_na(fabric_report_df, ['Примечение. Номер домена', 'Примечение. Время работы'])

#     switches_parameters_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
#     print('\n!!!!!!!!!!!!!!')
#     licenses_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
#     licenses_report_df = dfop.drop_fd_xd_switch(licenses_report_df)
    
#     switches_parameters_report_df = dfop.drop_column_if_all_na(switches_parameters_report_df, 'FC-FC Маршрутизация')
#     switches_parameters_report_df = dfop.drop_fd_xd_switch(switches_parameters_report_df)

#     # drop fabric_id if all have same value
#     if fabric_report_df['Fabric ID'].dropna().nunique() == 1:
#         fabric_report_df.drop(columns=['Fabric ID'], inplace=True)
#     # drop Fabric_Name (not Fabric_name) if column is empty
#     if fabric_report_df['Название фабрики'].isna().all():
#         fabric_report_df.drop(columns=['Название фабрики'], inplace=True)
       
#     global_fabric_parameters_report_df.reset_index(inplace=True, drop=True)

#     # fabric switch statistics                                         
#     fabric_switch_statistics_report_df = dfop.translate_dataframe(fabric_switch_statistics_df, report_headers_df, 
#                                                             df_name='Статистика_коммутаторов_перевод')
#     # drop allna columns
#     fabric_switch_statistics_report_df.dropna(axis=1, how='all', inplace=True)
#     dfop.drop_zero(fabric_switch_statistics_report_df)

#     return switches_report_df, fabric_report_df, switches_parameters_report_df, \
#                 maps_report_df, licenses_report_df, global_fabric_parameters_report_df, fabric_switch_statistics_report_df



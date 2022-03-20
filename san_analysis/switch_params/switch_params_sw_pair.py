"""Module to add switch pair id information to switch parameters DataFrame"""


import numpy as np
import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .switch_statistics_cp import fabric_switch_statistics


def switch_params_sw_pair_update(switch_params_aggregated_df, switch_pair_df, project_constants_lst):
    """Main function add switch pair id information to switch parameters DataFrame"""
    
    # # report_steps_dct contains current step desciption and force and export tags
    # # report_headers_df contains column titles, 
    # # report_columns_usage_sr show if fabric_name, chassis_name and group_name of device ports should be used
    # report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_sr = report_creation_info_lst
    # # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    # *_, max_title = report_constant_lst

    project_steps_df, max_title, data_dependency_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst


    # names to save data obtained after current module execution
    data_names = ['switch_params_aggregated_upd', 'fabric_switch_statistics', 
                    'Коммутаторы', 'Фабрика', 'Параметры_коммутаторов', 'MAPS', 'Лицензии', 
                    'Глобальные_параметры_фабрики', 'Статистика_коммутаторов']
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    
    # reade data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)


    # list of data to analyze from report_info table
    analyzed_data_names = ['switch_params_aggregated', 'chassis_parameters', 'switch_parameters', 'switchshow_ports', 
                            'maps_parameters', 'blade_interconnect', 'fabric_labels', 'fabricshow_summary', 'switch_pair']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, _ = sfop.regex_pattern_import('common_regex', max_title)
        # current operation information string
        info = f'Updating aggregated switch parameters table'
        print(info, end =" ") 

        switch_params_aggregated_df = dfop.dataframe_fillna(switch_params_aggregated_df, switch_pair_df, join_lst=['switchWwn'], filled_lst=['switchPair_id'])
        # add notes to switch_params_aggregated_df DataFrame
        switch_params_aggregated_df = add_notes(switch_params_aggregated_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))

        # current operation information string
        info = f'Counting switch statistics'
        print(info, end =" ") 
        fabric_switch_statistics_df = fabric_switch_statistics(switch_params_aggregated_df, pattern_dct)
        # after finish display status
        meop.status_info('ok', max_title, len(info))        


        switches_report_df, fabric_report_df, switches_parameters_report_df, \
            maps_report_df, licenses_report_df, global_fabric_parameters_report_df, fabric_switch_statistics_report_df = \
                switchs_params_report(switch_params_aggregated_df, fabric_switch_statistics_df, report_headers_df, report_columns_usage_sr, data_names)

        # create list with partitioned DataFrames
        data_lst = [switch_params_aggregated_df, fabric_switch_statistics_df,
                    switches_report_df, fabric_report_df, 
                    switches_parameters_report_df, maps_report_df, licenses_report_df,
                    global_fabric_parameters_report_df, fabric_switch_statistics_report_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)
    
    else:
        # verify if loaded data is empty and replace information string with empty DataFrame
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        switch_params_aggregated_df, *_ = data_lst

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)

    return switch_params_aggregated_df


def add_notes(switch_params_aggregated_df):
    """Function to add notes to switch_params_aggregated_df DataFrame"""

    def fabric_domain_unique_note(switch_params_aggregated_df):
        """Function to verify if fabric domain ID is unique within fabric_name"""

        mask_duplicated_fabric_domain = switch_params_aggregated_df.groupby(by=['Fabric_name', 'switchDomain'])['switchWwn'].transform('count') > 1
        mask_native = switch_params_aggregated_df['switchDomain'].notna()
        switch_params_aggregated_df.loc[mask_duplicated_fabric_domain & mask_native, 'Fabric_domain_note'] = 'duplicated_fabric_domain'
        return switch_params_aggregated_df
    

    def uptime_limit_note(switch_params_aggregated_df):
        """Function to verify if uptime is less then a year"""

        switch_params_aggregated_df['uptime_days'] = switch_params_aggregated_df['uptime_days'].apply(pd.to_numeric)
        mask_uptime_exceeded = switch_params_aggregated_df['uptime_days'] > 365
        mask_uptime_notna = switch_params_aggregated_df['uptime_days'].notna()
        switch_params_aggregated_df.loc[mask_uptime_notna & mask_uptime_exceeded, 'Uptime_note'] = 'uptime_exceeded'
        return switch_params_aggregated_df

    
    def sw_pair_absent_note(switch_params_aggregated_df):
        """Function to verify if switch have paired switch"""
        
        mask_switch_pair_absent = switch_params_aggregated_df.groupby(by=['Fabric_name', 'switchPair_id'])['switchPair_id'].transform('count') < 2
        switch_params_aggregated_df.loc[mask_switch_pair_absent , 'Switch_pair_absence_note'] = 'switch_pair_absent'
        return switch_params_aggregated_df
    
    def switch_pair_fos_note(switch_params_aggregated_df):
        """Function to verify if switch pairs have same FOS"""

        mask_fos_different = switch_params_aggregated_df.groupby(by=['Fabric_name', 'switchPair_id'])['FOS_version'].transform('nunique') > 1
        switch_params_aggregated_df.loc[mask_fos_different , 'Switch_pair_FOS_note'] = 'different_fos_within_sw_pair'
        return switch_params_aggregated_df  


    # add notes to switch_params_aggregated_df DataFrame
    switch_params_aggregated_df = fabric_domain_unique_note(switch_params_aggregated_df)
    switch_params_aggregated_df = uptime_limit_note(switch_params_aggregated_df)
    switch_params_aggregated_df = sw_pair_absent_note(switch_params_aggregated_df)
    switch_params_aggregated_df = switch_pair_fos_note(switch_params_aggregated_df)
    return switch_params_aggregated_df


def switchs_params_report(switch_params_aggregated_df, fabric_switch_statistics_df, report_headers_df, report_columns_usage_sr, data_names):
    """Function to create switch related report tables"""

    switches_report_df, fabric_report_df,  \
        switches_parameters_report_df, maps_report_df, licenses_report_df = \
            dfop.generate_report_dataframe(switch_params_aggregated_df, report_headers_df, report_columns_usage_sr, *data_names[2:-2])

    maps_report_df.replace(to_replace={'No FV lic': np.nan}, inplace=True)

    # global parameters are equal for all switches in one fabric thus checking Principal switches only
    mask_principal = switch_params_aggregated_df['switchRole'] == 'Principal'
    mask_valid_fabric = ~switch_params_aggregated_df['Fabric_name'].isin(['x', '-'])
    switch_params_principal_df = switch_params_aggregated_df.loc[mask_principal & mask_valid_fabric].copy()
    global_fabric_parameters_report_df = dfop.generate_report_dataframe(switch_params_principal_df, report_headers_df, 
                                                                report_columns_usage_sr, data_names[-2])

    # drop rows with empty switch names columns
    fabric_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
    fabric_report_df = dfop.translate_values(fabric_report_df, report_headers_df, 'Коммутаторы_перевод')
    fabric_report_df = dfop.drop_column_if_all_na(fabric_report_df, 
                                                    ['Примечение. Номер домена', 'Примечение. Время работы', 
                                                    'Примечание. Парный коммутатор', 'Примечание. Микрокод парных коммутаторов'])

    switches_parameters_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
    licenses_report_df.dropna(subset = ['Имя коммутатора'], inplace = True)
    licenses_report_df = dfop.drop_fd_xd_switch(licenses_report_df)
    
    switches_parameters_report_df = dfop.drop_column_if_all_na(switches_parameters_report_df, 'FC-FC Маршрутизация')
    switches_parameters_report_df = dfop.drop_fd_xd_switch(switches_parameters_report_df)

    # drop fabric_id if all have same value
    if fabric_report_df['Fabric ID'].dropna().nunique() == 1:
        fabric_report_df.drop(columns=['Fabric ID'], inplace=True)
    # drop Fabric_Name (not Fabric_name) if column is empty
    if fabric_report_df['Название фабрики'].isna().all():
        fabric_report_df.drop(columns=['Название фабрики'], inplace=True)
       
    global_fabric_parameters_report_df.reset_index(inplace=True, drop=True)

    # fabric switch statistics                                         
    fabric_switch_statistics_report_df = dfop.translate_dataframe(fabric_switch_statistics_df, report_headers_df, 
                                                            df_name='Статистика_коммутаторов_перевод')
    # drop allna columns
    fabric_switch_statistics_report_df.dropna(axis=1, how='all', inplace=True)
    dfop.drop_zero(fabric_switch_statistics_report_df)

    return switches_report_df, fabric_report_df, switches_parameters_report_df, \
                maps_report_df, licenses_report_df, global_fabric_parameters_report_df, fabric_switch_statistics_report_df
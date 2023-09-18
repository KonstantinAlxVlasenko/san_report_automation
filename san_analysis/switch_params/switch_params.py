"""Module to generate aggregated switch parameters table and 
'Switches list', 'Fabric list', 'Fabric global parameters', 
'Switches parameters', 'Licenses' report tables"""


import pandas as pd

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop

from .switch_aggregation import switch_param_aggregation


def switch_params_analysis(fabricshow_ag_labels_df, chassis_params_df, chassisshow_df,
                                switch_params_df, maps_params_df, blade_module_loc_df, ag_principal_df, project_constants_lst):
    """Main function to create aggregated switch parameters table and report tables"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'switch_params_analysis_out', 'switch_params_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # clean fabricshow DataFrame from unneccessary data
    fabric_clean_df = fabric_clean(fabricshow_ag_labels_df)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data 
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, _ = sfop.regex_pattern_import('common_regex', max_title)
        # import data with switch models, firmware and etc
        switch_models_df = sfop.dataframe_import('switch_models', max_title)
        switch_rack_df = get_switch_rack_details(report_requisites_sr['device_rack_path'], max_title)

        # current operation information string
        info = f'Generating aggregated switch parameters table'
        print(info, end =" ")

        # create aggregated table by joining DataFrames
        switch_params_aggregated_df, report_columns_usage_sr = switch_param_aggregation(
            fabric_clean_df, chassis_params_df, chassisshow_df, 
            switch_params_df, maps_params_df, switch_models_df, 
            switch_rack_df, ag_principal_df, pattern_dct)

        project_constants_lst.append(report_columns_usage_sr)

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
        data_lst = [report_columns_usage_sr, switch_params_aggregated_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)
    else:
        # verify if loaded data is empty and replace information string with empty DataFrame
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        report_columns_usage_sr, switch_params_aggregated_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names[1:], data_lst[1:]):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return report_columns_usage_sr, switch_params_aggregated_df, fabric_clean_df


def fabric_clean(fabricshow_ag_labels_df):
    """Function to prepare fabricshow_ag_labels DataFrame for join operation"""

    # create copy of fabricshow_ag_labels DataFrame
    fabric_clean_df = fabricshow_ag_labels_df.copy()
    # reset fabrics DataFrame index after droping switches
    fabric_clean_df.reset_index(inplace=True, drop=True)
    # extract required columns
    fabric_clean_df = fabric_clean_df.loc[:, ['Fabric_name', 'Fabric_label', 'Worldwide_Name', 'Domain_ID', 'Name', 'Enet_IP_Addr',	'SwitchMode']]
    # rename columns as in switch_params DataFrame
    fabric_clean_df.rename(columns={'Worldwide_Name': 'switchWwn', 'Name': 'switchName'}, inplace=True)
    return fabric_clean_df


def get_switch_rack_details(device_rack_path, max_title):
    """Function imports device rack details dataframe.
    If it has valid format (contains 'switchWwn' and 'Device_Rack') columns functions returns df.
    If path is not exist or file format is invalid then fn returns empty file"""

    switch_rack_sheet = 'switch_rack'

    if device_rack_path:
        switch_rack_df = sfop.dataframe_import(switch_rack_sheet, max_title, 
                                                init_file=device_rack_path, header=0)
        # check if dataframe has valid format
        if dfop.verify_columns_in_dataframe(switch_rack_df, columns=['switchWwn', 'Device_Rack']):
            return switch_rack_df
        else:
            print('Switch rack details dataframe has invalid format and will be deleted')
            meop.continue_request()
    else:
        info = f'Importing {switch_rack_sheet} dataframe'
        print(info, end = ' ')
        meop.status_info('skip', max_title, len(info))
    return pd.DataFrame(columns=['switchWwn', 'Device_Rack'])


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




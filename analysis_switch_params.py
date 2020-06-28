"""Module to generate 'Switches', 'Fabric', 'Fabric global parameters', 'Switches parameters', 'Licenses' customer report tables"""

import pandas as pd
import numpy as np
from common_operations_filesystem import load_data, save_data, save_xlsx_file 
from common_operations_dataframe import dataframe_segmentation
from common_operations_servicefile import dataframe_import
from common_operations_miscellaneous import status_info, force_extract_check

def fabric_main(fabricshow_ag_labels_df, chassis_params_df, \
    switch_params_df, maps_params_df, report_data_lst):
    """Main function to create tables
    """

    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = [
        'Коммутаторы', 'Фабрика', 'Глобальные_параметры_фабрики', 
        'Параметры_коммутаторов', 'Лицензии'
        ]
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration 
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    switches_report_df, fabric_report_df, global_fabric_parameters_report_df, \
        switches_parameters_report_df, licenses_report_df = data_lst
    
    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # list with True (if data loaded) and/or False (if data was not found and None returned)
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # flag if fabrics labels was forced to be changed 
    fabric_labels_change = True if report_steps_dct['fabric_labels'][1] else False
    # initialization chassis information and farbric name columns usage
    report_columns_usage_dct = {'fabric_name_usage': True, 'chassis_info_usage': True}
    # import data with switch models, firmware and etc
    switch_models_df = dataframe_import('switch_models', max_title)                     
    # clean fabricshow DataFrame from unneccessary data
    fabric_clean_df = fabric_clean(fabricshow_ag_labels_df)
    # create aggregated table by joining DataFrames
    switch_params_aggregated_df, report_columns_usage_dct = \
        fabric_aggregation(fabric_clean_df, chassis_params_df, \
            switch_params_df, maps_params_df, switch_models_df)
    save_xlsx_file(switch_params_aggregated_df, 'switch_params_aggregated', \
        report_data_lst, report_type = 'analysis')

    # when no data saved or force extract flag is on or fabric labels have been changed than 
    # analyze extracted config data  
    if not all(data_check) or any(force_extract_keys_lst) or fabric_labels_change:
        # information string if fabric labels force changed was initiated
        # and statistics recounting required
        if fabric_labels_change and not any(force_extract_keys_lst) and all(data_check):
            info = f'Switch information force extract due to change in Fabrics labeling'
            print(info, end =" ")
            status_info('ok', max_title, len(info))

        # partition aggregated DataFrame to required tables
        switches_report_df, fabric_report_df, global_fabric_parameters_report_df, \
            switches_parameters_report_df, licenses_report_df = \
                dataframe_segmentation(switch_params_aggregated_df, data_names, \
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
        data_lst = [switches_report_df, fabric_report_df, global_fabric_parameters_report_df, \
            switches_parameters_report_df, licenses_report_df]
        
        # current operation information string
        info = f'Generating Fabric and Switches tables'
        print(info, end =" ")   
        # after finish display status
        status_info('ok', max_title, len(info))

        # saving DataFrames to csv file
        save_data(report_data_lst, data_names, *data_lst)
        # save_data(report_data_lst, data_auxillary_names, *data_auxillary_lst)
        
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return switch_params_aggregated_df, report_columns_usage_dct, fabric_clean_df


def fabric_clean(fabricshow_ag_labels_df):
    """Function to prepare fabricshow_ag_labels DataFrame for join operation"""

    # create copy of fabricshow_ag_labels DataFrame
    fabric_clean_df = fabricshow_ag_labels_df.copy()
    # remove switches which are not part of research 
    # (was not labeled during Fabric labeling procedure)
    fabric_clean_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)

    # remove Front and Translate Domain switches
    mask = fabric_clean_df.Enet_IP_Addr != '0.0.0.0'
    fabric_clean_df = fabric_clean_df.loc[mask]
    # reset fabrics DataFrame index after droping switches
    fabric_clean_df.reset_index(inplace=True, drop=True)
    # extract required columns
    fabric_clean_df = fabric_clean_df.loc[:, ['Fabric_name', 'Fabric_label', 'Worldwide_Name', 'Name']]
    # rename columns as in switch_params DataFrame
    fabric_clean_df.rename(columns={'Worldwide_Name': 'switchWwn', 'Name': 'switchName'}, inplace=True)

    return fabric_clean_df


def fabric_aggregation(fabric_clean_df, chassis_params_df, switch_params_df, maps_params_df, switch_models_df):
    """Function to complete fabric DataFrame with information from 
    chassis_params_fabric, switch_params, maps_params DataFrames """

    # complete fabric DataFrame with information from switch_params DataFrame
    f_s_df = fabric_clean_df.merge(switch_params_df, how = 'left', on = ['switchWwn', 'switchName'])
    # complete f_s DataFrame with information from chassis_params DataFrame
    f_s_c_df = f_s_df.merge(chassis_params_df, how = 'left', on=['configname', 'chassis_name', 'chassis_wwn'])

    # convert switch_index in f_s_c and maps_params DataFrames to same type
    maps_params_df.switch_index = maps_params_df.switch_index.astype('float64', errors='ignore')
    f_s_c_df.switch_index = f_s_c_df.switch_index.astype('float64', errors='ignore')

    # complete f_s_c DataFrame with information from maps_params DataFrame
    f_s_c_m_df = f_s_c_df.merge(maps_params_df, how = 'left', on = ['configname', 'chassis_name', 'switch_index'])

    # convert switchType in f_s_c_m and switch_models DataFrames to same type
    # convert f_s_c_m_df.switchType from string to float
    f_s_c_m_df.switchType = f_s_c_m_df.switchType.astype('float64', errors='ignore')
    # remove fractional part from f_s_c_m_df.switchType
    f_s_c_m_df.switchType = np.floor(f_s_c_m_df.switchType)
    switch_models_df.switchType = switch_models_df.switchType.astype('float64', errors='ignore')
    # complete f_s_c_m DataFrame with information from switch_models DataFrame
    f_s_c_m_i_df = f_s_c_m_df.merge(switch_models_df, how='left', on='switchType')
    # sorting DataFrame
    f_s_c_m_i_df.sort_values(by=['Fabric_name', 'Fabric_label', 'switchType', 'chassis_name', 'switch_index'], \
        ascending=[True, True, False, True, True], inplace=True)
    # reset index values
    f_s_c_m_i_df.reset_index(inplace=True, drop=True)

    # # set DHCP to 'off' for Directors
    # director_type = [42.0, 62.0, 77.0, 120.0, 121.0, 165.0, 166.0]
    # f_s_c_m_i_df.loc[f_s_c_m_i_df.switchType.isin(director_type), 'DHCP'] = 'Off'
    
    # add empty column FOS suuported to fill manually 
    f_s_c_m_i_df['FW_Supported'] = pd.Series()

    # license check
    license_dct = {'Trunking_license': 'Trunking', 'Fabric_Vision_license': 'Fabric Vision'}
    for lic_check, lic_name in license_dct.items():
        f_s_c_m_i_df[lic_check] = f_s_c_m_i_df.loc[f_s_c_m_i_df['licenses'].notnull(), 'licenses'].apply(lambda x: lic_name in x)
        f_s_c_m_i_df[lic_check].replace(to_replace={True: 'Да', False: 'Нет'}, inplace = True)

    # check if chassis_name and switch_name columns are equal
    # if yes then no need to use chassis information in tables
    # remove switches with unparsed data
    chassis_names_check_df = f_s_c_m_i_df.dropna(subset=['chassis_name', 'SwitchName'], how = 'all')
    if all(chassis_names_check_df.chassis_name == chassis_names_check_df.SwitchName):
        chassis_column_usage = False
    else:
        chassis_column_usage = True
    # Check number of Fabric_names. 
    # If there is only one Fabric_name then no need to use Fabric_name column in report Dataframes
    fabric_name_usage = True if f_s_c_m_i_df.Fabric_name.nunique() > 1 else False
        
    report_columns_usage_dct = {'fabric_name_usage': fabric_name_usage, 'chassis_info_usage': chassis_column_usage}
    
    return f_s_c_m_i_df, report_columns_usage_dct
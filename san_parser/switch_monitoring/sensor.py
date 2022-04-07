"""Module to extract sensor information"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop
import utilities.regular_expression_operations as reop


def sensor_extract(chassis_params_df, project_constants_lst):
    """Function to extract sensor information"""  

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'sensor_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration    
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
    if force_run:    
        print('\nEXTRACTING ENVIRONMENT DATA ...\n')   
        
        # number of switches to check
        switch_num = len(chassis_params_df.index)   
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('sensor', max_title)

        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        sensor_lst = []

        # checking each chassis for switch level parameters
        for i, chassis_params_sr in chassis_params_df.iterrows():   
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_params_sr["chassis_name"]} sensor readings'
            print(info, end =" ")  

            sw_sensor_lst = current_config_extract(sensor_lst, pattern_dct, chassis_params_sr)
            meop.show_collection_status(sw_sensor_lst, max_title, len(info))     
    
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'sensor_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, sensor_lst)
        sensor_df, *_ = data_lst   
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)    
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        sensor_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)    
    return sensor_df


def current_config_extract(sensor_lst, pattern_dct, chassis_params_sr):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
    chassis_info_lst = [chassis_params_sr[key] for key in chassis_info_keys]
    sshow_file, *_ = chassis_info_lst
                           
    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'sensor': False}

    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()                        
            if not line:
                break
            # sensor section start   
            if re.search(pattern_dct['switchcmd_sensorhow'], line) and not collected['sensor']:
                collected['sensor'] = True
                line, sw_sensor_lst = reop.lines_extract(sensor_lst, pattern_dct, 'sensor', chassis_info_lst, line, file, save_local=True)                                                 
            # sensor section end
    return sw_sensor_lst
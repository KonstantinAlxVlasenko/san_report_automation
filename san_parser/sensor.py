"""Module to extract sensor information"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop


# import dataframe_operations as dfop
# from common_operations_filesystem import load_data, save_data
# from common_operations_miscellaneous import (force_extract_check, line_to_list,
#                                              status_info, update_dct,
#                                              verify_data)
# from common_operations_servicefile import columns_import, data_extract_objects
# from common_operations_miscellaneous import verify_force_run
# from common_operations_dataframe import list_to_dataframe
# from common_operations_table_report import dataframe_to_report
# from common_operations_database import read_db, write_db


def sensor_extract(chassis_params_df, report_creation_info_lst):
    """Function to extract sensor information"""  

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['sensor']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration    
    # data_lst = load_data(report_constant_lst, *data_names)
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)
    
    if force_run:    
        print('\nEXTRACTING ENVIRONMENT DATA ...\n')   
        
        # # extract chassis parameters names from init file
        # chassis_columns = sfop.columns_import('chassis', max_title, 'columns')
        
        # number of switches to check
        switch_num = len(chassis_params_df.index)   
        # data imported from init file to extract values from config file
        *_, comp_keys, match_keys, comp_dct = sfop.data_extract_objects('sensor', max_title)

        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        sensor_lst = []

        # checking each chassis for switch level parameters
        for i, chassis_params_sr in chassis_params_df.iterrows():   
            
            # # data unpacking from iter param
            # # dictionary with parameters for the current chassis
            # chassis_params_data_dct = dict(zip(chassis_columns, chassis_params_data))
            # chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
            # chassis_info_lst = [chassis_params_data_dct.get(key) for key in chassis_info_keys]            

            chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
            chassis_info_lst = [chassis_params_sr[key] for key in chassis_info_keys]
            sshow_file, chassis_name, _ = chassis_info_lst
                        
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_name} sensor readings'
            print(info, end =" ")           
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            collected = {'sensor': False}

            with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                # check file until all groups of parameters extracted
                while not all(collected.values()):
                    line = file.readline()                        
                    if not line:
                        break
                    # sensor section start   
                    # switchcmd_sensorshow_comp
                    if re.search(comp_dct[comp_keys[0]], line) and not collected['sensor']:
                        collected['sensor'] = True                      
                        # switchcmd_end_comp
                        while not re.search(comp_dct[comp_keys[2]], line):
                            line = file.readline()
                            match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # islshow_match
                            if match_dct[match_keys[1]]:
                                sensor_reading = dsop.line_to_list(comp_dct[comp_keys[1]], line, *chassis_info_lst)
                                # appending list with only REQUIRED port info for the current loop iteration 
                                # to the list with all ISL port info
                                sensor_lst.append(sensor_reading)
                            if not line:
                                break                                
                    # sensor section end
            meop.status_info('ok', max_title, len(info))      
    
        # convert list to DataFrame
        sensor_df = dfop.list_to_dataframe(sensor_lst, max_title, sheet_title_import='sensor')
        # saving data to csv file
        data_lst = [sensor_df]
        # save_data(report_constant_lst, data_names, *data_lst)
        # write data to sql db
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)    

    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        # sensor_df = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        # data_lst = [sensor_df]

        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        sensor_df, *_ = data_lst

    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)    
    
    return sensor_df

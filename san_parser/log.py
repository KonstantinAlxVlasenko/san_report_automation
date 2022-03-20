"""Module to extract portFcPortCmdShow information"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop


def log_extract(chassis_params_df, project_constants_lst):
    """Function to extract logs"""

    # # report_steps_dct contains current step desciption and force and export tags
    # report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # # report_constant_lst contains information: 
    # # customer_name, project directory, database directory, max_title
    # *_, max_title = report_constant_lst
    
    project_steps_df, max_title, data_dependency_df, *_ = project_constants_lst
    
    # names to save data obtained after current module execution
    data_names = ['errdump']
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')

    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:             
        print('\nEXTRACTING LOGS ...\n')
        
        # number of switches to check
        switch_num = len(chassis_params_df.index)
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('log', max_title)

        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping       
        errdump_lst = []  

        # checking each chassis for switch level parameters
        for i, chassis_params_sr in chassis_params_df.iterrows():       
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_params_sr["chassis_name"]} switch logs'
            print(info, end =" ")
            
            current_config_extract(errdump_lst, pattern_dct, chassis_params_sr)
            meop.status_info('ok', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'errdump_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, errdump_lst)
        errdump_df, *_ = data_lst    
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)   
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        errdump_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return errdump_df


def current_config_extract(errdump_lst, pattern_dct, chassis_params_sr):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
    chassis_info_lst = [chassis_params_sr[key] for key in chassis_info_keys]

    sshow_file, chassis_name, _ = chassis_info_lst

    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'errdump': False}
    
    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # errdump section start
            # errdump_start_comp
            if re.search(pattern_dct['errdump_start'], line) and not collected['errdump']:
                # when section is found corresponding collected dict values changed to True
                collected['errdump'] = True
                # switchcmd_end_comp
                while not re.search(pattern_dct['switchcmd_end'], line):
                    line = file.readline()
                    if not line:
                        break
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # errdump_message_match
                    if match_dct['errdump_message']:
                        error_message = dsop.line_to_list(pattern_dct['errdump_message'], line, *chassis_info_lst)
                        # append current value to the list of values 
                        errdump_lst.append(error_message)
                    if not line:
                        break                                
            # errdump section end
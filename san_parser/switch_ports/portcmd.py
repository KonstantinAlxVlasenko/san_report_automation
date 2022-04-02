"""Module to extract portFcPortCmdShow information"""


import re

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop
from .portcmd_sections import port_fc_portcmd_section_extract


def portcmd_extract(chassis_params_df, project_constants_lst):
    """Function to extract portshow, portloginshow, portstatsshow information"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'portcmd_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
    if force_run:             
        print('\nEXTRACTING PORTSHOW, PORTLOGINSHOW, PORTSTATSSHOW INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        switch_num = len(chassis_params_df.index)

        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping       
        portshow_lst = []  
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('portcmd', max_title)
        portcmd_params, portcmd_params_add = dfop.list_from_dataframe(re_pattern_df, 'portcmd_params', 'portcmd_params_add')
        
        # for i, chassis_params_data in enumerate(chassis_params_fabric_lst):
        for i, chassis_params_sr in chassis_params_df.iterrows():
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_params_sr["chassis_name"]} switch portshow, portloginshow and statsshow'
            print(info, end =" ")
            # if chassis_params_sr["chassis_name"] == 's1bchwcmn05-fcsw1':
            current_config_extract(portshow_lst, pattern_dct, 
                            chassis_params_sr, portcmd_params, portcmd_params_add)                  
            meop.status_info('ok', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'portcmd_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, portshow_lst)
        portshow_df, *_ = data_lst
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)      
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        portshow_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return portshow_df


def current_config_extract(portshow_lst, pattern_dct, 
                            chassis_params_sr, portcmd_params, portcmd_params_add):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
    chassis_info_lst = [chassis_params_sr[key] for key in chassis_info_keys]
    sshow_file, *_ = chassis_info_lst            
    
    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'portshow': False}
    
    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # sshow_port section start
            if re.search(pattern_dct['section_sshow_port'], line):
                # when section is found corresponding collected dict values changed to True
                collected['portshow'] = True
                while not re.search(pattern_dct['rebuilt finished'],line):
                    line = file.readline()
                    if not line:
                        break
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # portFcPortCmdShow section start
                    if match_dct['slot_port_number']:
                        line = port_fc_portcmd_section_extract(portshow_lst, pattern_dct, chassis_info_lst, 
                                                        portcmd_params, portcmd_params_add, line, file)
                    # portFcPortCmdShow section end
            # sshow_port section end
"""Module to extract switch parameters"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop


def switch_params_extract(chassis_params_df, project_constants_lst):
    """Function to extract switch parameters"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'switch_params_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
    if force_run:    
        print('\nEXTRACTING SWITCH PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...\n')   
        
        # number of switches to check
        switch_num = len(chassis_params_df.index)   
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        switch_params_lst = []
        # list to store switch ports details 
        switchshow_ports_lst = []    
        
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('switch', max_title)
        switch_params, switch_params_add = dfop.list_from_dataframe(re_pattern_df, 'switch_params', 'switch_params_add')
        
        # checking each chassis for switch level parameters
        for i, chassis_params_sr in chassis_params_df.iterrows():
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_params_sr["chassis_name"]} switch parameters. Number of LS: {chassis_params_sr["Number_of_LS"]}'
            print(info, end =" ")
            switch_params_current_lst = current_config_extract(switch_params_lst, switchshow_ports_lst, pattern_dct, 
                                                                chassis_params_sr, switch_params, switch_params_add)       
            if dsop.list_is_empty(switch_params_current_lst):
                meop.status_info('no data', max_title, len(info))
            else:
                meop.status_info('ok', max_title, len(info))                                

        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'switch_columns', 'switchshow_portinfo_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, switch_params_lst, switchshow_ports_lst)
        switch_params_df, switchshow_ports_df, *_ = data_lst        
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        switch_params_df, switchshow_ports_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return switch_params_df, switchshow_ports_df


def current_config_extract(switch_params_lst, switchshow_ports_lst, pattern_dct, 
                            chassis_params_sr, switch_params, switch_params_add):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
    chassis_info_lst = [chassis_params_sr[key] for key in chassis_info_keys]
    sshow_file, *_ = chassis_info_lst

    # when num of logical switches is 0 or None than mode is Non-VF otherwise VF
    ls_mode_on = (True if not chassis_params_sr["Number_of_LS"] in ['0', None] else False)
    ls_mode = ('ON' if not chassis_params_sr["Number_of_LS"] in ['0', None] else 'OFF')
    # logical switches indexes. if switch is in Non-VF mode then ls_id is 0
    ls_ids = chassis_params_sr['LS_IDs'].split(', ') if chassis_params_sr['LS_IDs'] else ['0']               
    
    # check each logical switch in chassis
    for i in ls_ids:
        # search control dictionary. continue to check sshow_file until all parameters groups are found
        collected = {'configshow': False, 'switchshow': False}
        # dictionary to store all DISCOVERED switch parameters
        # collecting data only for the logical switch in current loop
        switch_params_dct = {}      
        with open(sshow_file, encoding='utf-8', errors='ignore') as file:
            # check file until all groups of parameters extracted
            while not all(collected.values()):
                line = file.readline()                        
                if not line:
                    break
                # configshow section start
                if re.search(fr'^\[Switch Configuration Begin *: *{i}\]$', line) and not collected['configshow']:
                    # when section is found corresponding collected dict values changed to True
                    collected['configshow'] = True
                    
                    while not re.search(fr'^\[Switch Configuration End : {i}\]$',line):
                        line = file.readline()
                        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}                           
                        # 'switch_configall_match' pattern #0
                        if match_dct['configshowall_param']:
                            switch_params_dct[match_dct['configshowall_param'].group(1).rstrip()] = match_dct['configshowall_param'].group(2).rstrip()              
                        if not line:
                            break
                # config section end
                # switchshow section start
                elif re.search(pattern_dct['switchcmd_switchshow'], line) and not collected['switchshow']:
                    collected['switchshow'] = True
                    line = meop.goto_switch_context(ls_mode_on, line, file, i)
                    line = switchshow_section_extract(switch_params_dct, switchshow_ports_lst, pattern_dct, chassis_info_lst, line, file, i)                    
                # switchshow section end
                
        # additional values which need to be added to the switch params dictionary 
        # switch_params_add order ('configname', 'chassis_name', 'switch_index', 'ls_mode')
        # values extracted in manual mode. if change values order change keys order in init.xlsx switch tab "params_add" column
        switch_params_values = (*chassis_info_lst, str(i), ls_mode)

        if switch_params_dct:
            # adding additional parameters and values to the switch_params_switch_dct
            dsop.update_dct(switch_params_add, switch_params_values, switch_params_dct)                                                
            # creating list with REQUIRED chassis parameters for the current switch.
            # if no value in the switch_params_dct for the parameter then None is added
            switch_params_current_lst = [switch_params_dct.get(switch_param, None) for switch_param in switch_params]
            # and appending this list to the list of all switches switch_params_fabric_lst            
            switch_params_lst.append(switch_params_current_lst)
        else:
            switch_params_current_lst = []
    return switch_params_current_lst


def switchshow_section_extract(switch_params_dct, switchshow_ports_lst, pattern_dct, 
                        chassis_info_lst, 
                        line, file, switch_index):
    """Function to extract switch parameters and switch port information from switchshow section"""

    while not re.search(pattern_dct['switchcmd_end'],line):
        line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # 'switch_switchshow_match' pattern #1
        if match_dct['switchshow_param']:
            switch_params_dct[match_dct['switchshow_param'].group(1).rstrip()] = match_dct['switchshow_param'].group(2).rstrip()
        # 'ls_attr_match' pattern #2
        if match_dct['ls_attr']:
            ls_attr = pattern_dct['ls_attr'].findall(line)[0]
            for k, v in zip(ls_attr[::2], ls_attr[1::2]):
                switch_params_dct[k] = v
        # 'switchshow_portinfo_match' pattern #3 
        if match_dct['switchshow_portinfo']:
            switchinfo_lst = [*chassis_info_lst, str(switch_index), 
                                switch_params_dct.get('switchName'), switch_params_dct.get('switchWwn'), 
                                switch_params_dct.get('switchState'), switch_params_dct.get('switchMode')]
            switchshow_port_lst = dsop.line_to_list(pattern_dct['switchshow_portinfo'], line, *switchinfo_lst)
            # if switch has no slots than slot number is 0
            if not switchshow_port_lst[9]:
                switchshow_port_lst[9] = str(0)
            switchshow_ports_lst.append(switchshow_port_lst)                      
        if not line:
            break
    return line  
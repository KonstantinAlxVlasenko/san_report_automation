"""Module to extract port information (sfp transceivers, portcfg, trunk area settings)"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop


def portcfg_sfp_extract(switch_params_df, project_constants_lst):
    """Function to extract switch port information"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst
    
    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'port_sfp_cfg_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data 
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:    
        print('\nEXTRACTING SWITCH PORTS SFP, PORTCFG INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')   
        
        # number of switches to check
        switch_num = len(switch_params_df.index)
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('portinfo', max_title)
        sfp_params, sfp_params_add, portcfg_params = dfop.list_from_dataframe(re_pattern_df, 'sfp_params', 'sfp_params_add', 'portcfg_params')

        # dictionary to save portcfg ALL information for all ports in fabric
        portcfgshow_dct = dict((key, []) for key in portcfg_params)
        # list to store only REQUIRED switch parameters
        # collecting sfpshow data for all switches ports during looping
        sfpshow_lst = []
        # list to save portcfg information for all ports in fabric
        portcfgshow_lst = []
        
        for i, switch_params_sr in switch_params_df.iterrows():
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} ports sfp and cfg'
            print(info, end =" ")                      
            current_config_extract(sfpshow_lst, portcfgshow_dct, pattern_dct,
                                    switch_params_sr, sfp_params, sfp_params_add, portcfg_params)                     
            meop.status_info('ok', max_title, len(info))

        # after check all config files create list of lists from dictionary. 
        # each nested list contains portcfg information for one port
        for portcfg_param in portcfg_params:
            portcfgshow_lst.append(portcfgshow_dct.get(portcfg_param))            
        portcfgshow_lst = list(zip(*portcfgshow_lst))
        
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'sfp_columns', 'portcfg_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, sfpshow_lst, portcfgshow_lst)
        sfpshow_df, portcfgshow_df, *_ = data_lst  
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)   
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        sfpshow_df, portcfgshow_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return sfpshow_df, portcfgshow_df


def current_config_extract(sfpshow_lst, portcfgshow_dct, pattern_dct,
                            switch_params_sr, sfp_params, sfp_params_add, portcfg_params):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                        'SwitchName', 'switchWwn']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False
    sshow_file, _, _, switch_index, switch_name, *_ = switch_info_lst

    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'sfpshow': False, 'portcfgshow': False}
    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()                        
            if not line:
                break
            # sfpshow section start
            if re.search(pattern_dct['switchcmd_sfpshow'], line) and not collected['sfpshow']:  #r'^(SWITCHCMD )?(/fabos/cliexec/)?sfpshow +-all *: *$'
                collected['sfpshow'] = True
                if ls_mode_on:
                    while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                        line = file.readline()
                        if not line:
                            break
                while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                    line = file.readline()
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    if match_dct['slot_port']:
                        # dictionary to store all DISCOVERED switch ports information
                        # collecting data only for the logical switch in current loop
                        sfpshow_dct = {}
                        _, slot_num, port_num = dsop.line_to_list(pattern_dct['slot_port'], line)
                        # if switch has no slots then all ports have slot 0
                        slot_num = '0' if not slot_num else slot_num
                        while not re.match('\r?\n', line):
                            line = file.readline()
                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                            # power_match
                            if match_dct['power']:
                                sfp_power_lst = dsop.line_to_list(pattern_dct['power'], line)
                                # cut off RX or TX Power
                                sfp_power_value_unit = sfp_power_lst[1:]
                                for v, k in zip(sfp_power_value_unit[::2], sfp_power_value_unit[1::2]):
                                    if k == 'uWatts':
                                        k = 'uW'
                                    key = sfp_power_lst[0] + '_' + k
                                    sfpshow_dct[key] = v
                            # transceiver_match
                            elif match_dct['transceiver']:
                                sfpshow_dct[match_dct['transceiver'].group(1).rstrip()] = match_dct['transceiver'].group(2).rstrip()
                            # no_sfp_match
                            elif match_dct['no_sfp']:
                                    sfpshow_dct['Vendor Name'] = 'No SFP module'
                            # not_available_match
                            elif match_dct['info_na']:
                                    sfpshow_dct[match_dct['info_na'].group(1).rstrip()] = match_dct['info_na'].group(2).rstrip()
                            # sfp_info_match
                            elif match_dct['sfp_info']:
                                sfpshow_dct[match_dct['sfp_info'].group(1).rstrip()] = match_dct['sfp_info'].group(2).rstrip()                                        
                            if not line:
                                break
                            
                        # additional values which need to be added to the dictionary with all DISCOVERED parameters during current loop iteration
                        # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column                                   
                        sfpshow_port_values = [*switch_info_lst, slot_num, port_num]                                       
                        # adding additional parameters and values to the sfpshow_dct
                        dsop.update_dct(sfp_params_add, sfpshow_port_values, sfpshow_dct)               
                        # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                        sfpshow_lst.append([sfpshow_dct.get(param, None) for param in sfp_params])
            # sfpshow section end
            # portcfgshow section start
            if re.search(r'^(SWITCHCMD )?(/fabos/cliexec/)?portcfgshow *: *$', line) and not collected['portcfgshow']:
                collected['portcfgshow'] = True
                if ls_mode_on:
                    while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                        line = file.readline()
                        if not line:
                            break
                while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$|No ports found in switch',line):
                    line = file.readline()
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # 'slot_port_line_match'
                    if match_dct['slot_port_line']:
                        # dictionary to store all DISCOVERED switch ports information
                        portcfgshow_tmp_dct = {}
                        # extract slot and port numbers
                        slot_num, port_nums_str = dsop.line_to_list(pattern_dct['slot_port_line'], line)
                        port_nums_lst = port_nums_str.split()
                        port_nums = len(port_nums_lst)
                        # list with switch and slot information
                        switch_info_slot_lst = switch_info_lst.copy()
                        switch_info_slot_lst.append(slot_num)
                        # adding switch and slot information for each port to dictionary
                        for portcfg_param, switch_info_value in zip(portcfg_params[:7], switch_info_slot_lst):
                            portcfgshow_tmp_dct[portcfg_param] = [switch_info_value for i in range(port_nums)]
                        # adding port numbers to dictionary    
                        portcfgshow_tmp_dct[portcfg_params[7]] = port_nums_lst                                
                        while not re.match('\r?\n', line):
                            line = file.readline()
                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                            # portcfg_match
                            if match_dct['portcfg']:
                                # extract param name and values for each port and adding to dictionary
                                param_name, param_values_str = dsop.line_to_list(pattern_dct['portcfg'], line)
                                portcfgshow_tmp_dct[param_name] = param_values_str.split()
                            if not line:
                                break
                        # saving portcfg information of REQUIRED parameters from dictionary with DISCOVERED parameters
                        for portcfg_param in portcfg_params:
                            portcfgshow_dct[portcfg_param].extend(portcfgshow_tmp_dct.get(portcfg_param, [None for i in range(port_nums)]))              
            # portcfgshow section end

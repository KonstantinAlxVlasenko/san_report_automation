"""Module to extract interswitch connection information and Fabric Shortest Path First (FSPF) link state database"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop


def interswitch_connection_extract(switch_params_df, project_constants_lst):
    """Function to extract interswitch connection information"""  

    # # report_steps_dct contains current step desciption and force and export tags
    # report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # # report_constant_lst contains information: 
    # # customer_name, project directory, database directory, max_title
    # *_, max_title = report_constant_lst

    project_steps_df, max_title, data_dependency_df, *_ = project_constants_lst

    # names to save data obtained after current module execution
    data_names = ['isl', 'trunk', 'porttrunkarea', 'lsdb']
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')

    # read data from database if they were saved on previos program execution iteration    
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:    
        print('\nEXTRACTING INTERSWITCH CONNECTION INFORMATION (ISL, TRUNK, TRUNKAREA) ...\n')   
        
        # number of switches to check
        switch_num = len(switch_params_df.index)   
     
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('isl', max_title)
        lsdb_params = dfop.list_from_dataframe(re_pattern_df, 'lsdb_params')

        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        isl_lst = []
        trunk_lst = []
        porttrunkarea_lst = []
        lsdb_lst = []

        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} isl, trunk and trunk area ports. Switch mode: {switch_params_sr["switchMode"]}'
            print(info, end =" ")   

            if switch_params_sr["switchMode"] == 'Native':
                current_config_extract(isl_lst, trunk_lst, porttrunkarea_lst, lsdb_lst, pattern_dct, 
                                        switch_params_sr, lsdb_params)
                meop.status_info('ok', max_title, len(info))
            # if switch in Access Gateway mode then skip
            else:
                meop.status_info('skip', max_title, len(info))        
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'isl_columns', 'trunk_columns', 'porttrunkarea_columns', 'lsdb_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, isl_lst, trunk_lst, porttrunkarea_lst, lsdb_lst)
        isl_df, trunk_df, porttrunkarea_df, lsdb_df, *_ = data_lst    
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        isl_df, trunk_df, porttrunkarea_df, lsdb_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return isl_df, trunk_df, porttrunkarea_df, lsdb_df



def current_config_extract(isl_lst, trunk_lst, porttrunkarea_lst, lsdb_lst, pattern_dct, 
                            switch_params_sr, lsdb_params):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""


    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                        'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False
    
    sshow_file, _, _, switch_index, switch_name, *_, switch_mode = switch_info_lst
                     
    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'isl': False, 'trunk': False, 'trunkarea': False, 'lsdb': False}

    if switch_mode == 'Native':
        with open(sshow_file, encoding='utf-8', errors='ignore') as file:
            # check file until all groups of parameters extracted
            while not all(collected.values()):
                line = file.readline()                        
                if not line:
                    break
                # isl section start   
                # switchcmd_islshow_comp
                if re.search(pattern_dct['switchcmd_islshow'], line) and not collected['isl']:
                    collected['isl'] = True
                    if ls_mode_on:
                        while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                            line = file.readline()
                            if not line:
                                break                        
                    # switchcmd_end_comp
                    while not re.search(pattern_dct['switchcmd_end'], line):
                        line = file.readline()
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # islshow_match
                        if match_dct['islshow']:
                            isl_port = dsop.line_to_list(pattern_dct['islshow'], line, *switch_info_lst[:-1])
                            # portcfg parameters
                            if isl_port[-1]:
                                isl_port[-1] = isl_port[-1].replace(' ', ', ')
                            # appending list with only REQUIRED port info for the current loop iteration 
                            # to the list with all ISL port info
                            isl_lst.append(isl_port)
                        if not line:
                            break                                
                # isl section end
                # trunk section start   
                # switchcmd_trunkshow_comp
                if re.search(pattern_dct['switchcmd_trunkshow'], line) and not collected['trunk']:
                    collected['trunk'] = True
                    if ls_mode_on:
                        while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                            line = file.readline()
                            if not line:
                                break                        
                    # switchcmd_end_comp
                    while not re.search(pattern_dct['switchcmd_end'], line):                             
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # trunkshow_match
                        if match_dct['trunkshow']:
                            trunk_port = dsop.line_to_list(pattern_dct['trunkshow'], line, *switch_info_lst[:-1])
                            # if trunk line has trunk number then remove ":" from trunk number
                            if trunk_port[9]:
                                trunk_port[9] = trunk_port[9].strip(':')
                                trunk_num = trunk_port[9]
                            # if trunk line has no number then use number from previous line
                            else:
                                trunk_port[9] = trunk_num
                            # appending list with only REQUIRED trunk info for the current loop iteration 
                            # to the list with all trunk port info
                            trunk_lst.append(trunk_port)
                        line = file.readline()
                        if not line:
                            break                                
                # trunk section end
                # porttrunkarea section start
                # switchcmd_trunkarea_comp
                if re.search(pattern_dct['switchcmd_trunkarea'], line) and not collected['trunkarea']:
                    collected['trunkarea'] = True
                    if ls_mode_on:
                        while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                            line = file.readline()
                            if not line:
                                break
                    # switchcmd_end_comp
                    while not re.search(pattern_dct['switchcmd_end'], line):
                        line = file.readline()
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # 'porttrunkarea_match'
                        if match_dct['porttrunkarea']:
                            porttrunkarea_port_lst = dsop.line_to_list(pattern_dct['porttrunkarea'], line, *switch_info_lst[:6])
                            # for No_light ports port and slot numbers are '--'
                            if porttrunkarea_port_lst[11] == '--':
                                porttrunkarea_port_lst[10] = '--'
                            # if switch has no slots than slot number is 0
                            for idx in [6, 10]:                                   
                                if not porttrunkarea_port_lst[idx]:
                                    porttrunkarea_port_lst[idx] = str(0)

                            porttrunkarea_lst.append(porttrunkarea_port_lst)                                                       
                        if not line:
                            break                        
                # porttrunkarea section end
                # lsdb section start
                if re.search(pattern_dct['switchcmd_lsdbshow'], line) and not collected['lsdb']:
                    # when section is found corresponding collected dict values changed to True
                    collected['lsdb'] = True
                    if ls_mode_on:
                        while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                            line = file.readline()
                            if not line:
                                break
                    # switchcmd_end_comp
                    while not re.search(pattern_dct['switchcmd_end'],line):  
                        line = file.readline()
                        if not line:
                            break
                        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # lsdb_domain section start
                        if match_dct['lsdb_domain']:
                            # dictionary to store all DISCOVERED parameters
                            lsdb_param_dct = {}
                            # Domain ID described by this LSR. 
                            # A (self) keyword after the domain ID indicates that LSR describes the local switch.
                            domain_self_tag_lst = dsop.line_to_list(pattern_dct['lsdb_domain'], line)
                            # lsdb_link_comp
                            while not (re.search(pattern_dct['lsdb_link'],line) or re.search(pattern_dct['switchcmd_end'],line)):
                                line = file.readline()
                                if not line:
                                    break
                                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                # param extraction
                                if match_dct['lsdb_param']:
                                    lsdb_name = match_dct['lsdb_param'].group(1).rstrip()
                                    lsdb_value = match_dct['lsdb_param'].group(2).rstrip()
                                    lsdb_param_dct[lsdb_name] = lsdb_value
                            # list with required params only in order
                            lsdb_param_lst = [lsdb_param_dct.get(param_name) for param_name in lsdb_params]
                        # lsdb_domain section end
                        if match_dct['lsdb_link']:
                            # extract link information
                            lsdb_link_lst = dsop.line_to_list(pattern_dct['lsdb_link'], line)
                            # add link information to the global list with current switch and lsdb information 
                            lsdb_lst.append([*switch_info_lst[:6], *domain_self_tag_lst,*lsdb_param_lst, *lsdb_link_lst])
                # lsdb section end
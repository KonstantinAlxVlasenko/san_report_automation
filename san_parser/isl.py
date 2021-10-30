"""Module to extract interswitch connection information and Fabric Shortest Path First (FSPF) link state database"""


import re

import pandas as pd
from pandas.core import indexing
import dataframe_operations as dfop
from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects
from common_operations_miscellaneous import verify_force_run
from common_operations_dataframe import list_to_dataframe
from common_operations_table_report import dataframe_to_report
from common_operations_database import read_db, write_db


def interswitch_connection_extract(switch_params_df, report_creation_info_lst):
    """Function to extract interswitch connection information"""  

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['isl', 'trunk', 'porttrunkarea', 'lsdb']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration    
    # data_lst = load_data(report_constant_lst, *data_names)
    data_lst = read_db(report_constant_lst, report_steps_dct, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, max_title)

    if force_run:    
        print('\nEXTRACTING INTERSWITCH CONNECTION INFORMATION (ISL, TRUNK, TRUNKAREA) ...\n')   
        
        # # extract chassis parameters names from init file
        # switch_columns = columns_import('switch', max_title, 'columns')
        
        # number of switches to check
        switch_num = len(switch_params_df.index)   
     
        # data imported from init file to extract values from config file
        *_, comp_keys, match_keys, comp_dct = data_extract_objects('isl', max_title)
        lsdb_params = columns_import('isl', max_title, 'lsdb_params')


        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        isl_lst = []
        trunk_lst = []
        porttrunkarea_lst = []
        lsdb_lst = []

        # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():       
            # # data unpacking from iter param
            # # dictionary with parameters for the current switch
            # switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
            # switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
            #                     'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
            # switch_info_lst = [switch_params_data_dct.get(key) for key in switch_info_keys]
            # ls_mode_on = True if switch_params_data_dct['LS_mode'] == 'ON' else False

            switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                                'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
            switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
            ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False
            
            sshow_file, _, _, switch_index, switch_name, *_, switch_mode = switch_info_lst
                        
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} isl, trunk and trunk area ports. Switch mode: {switch_mode}'
            print(info, end =" ")           
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
                        if re.search(comp_dct[comp_keys[0]], line) and not collected['isl']:
                            collected['isl'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break                        
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[2]], line):
                                line = file.readline()
                                match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # islshow_match
                                if match_dct[match_keys[1]]:
                                    isl_port = line_to_list(comp_dct[comp_keys[1]], line, *switch_info_lst[:-1])
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
                        if re.search(comp_dct[comp_keys[3]], line) and not collected['trunk']:
                            collected['trunk'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break                        
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[2]], line):                             
                                match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # trunkshow_match
                                if match_dct[match_keys[4]]:
                                    trunk_port = line_to_list(comp_dct[comp_keys[4]], line, *switch_info_lst[:-1])
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
                        if re.search(comp_dct[comp_keys[5]], line) and not collected['trunkarea']:
                            collected['trunkarea'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[2]], line):
                                line = file.readline()
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # 'porttrunkarea_match'
                                if match_dct[match_keys[6]]:
                                    porttrunkarea_port_lst = line_to_list(comp_dct[comp_keys[6]], line, *switch_info_lst[:6])
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
                        if re.search(comp_dct['switchcmd_lsdbshow'], line) and not collected['lsdb']:
                            # when section is found corresponding collected dict values changed to True
                            collected['lsdb'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break
                            # switchcmd_end_comp
                            while not re.search(comp_dct['switchcmd_end'],line):  
                                line = file.readline()
                                if not line:
                                    break
                                # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                match_dct ={comp_key: comp_dct[comp_key].match(line) for comp_key in comp_keys}
                                # lsdb_domain section start
                                if match_dct['lsdb_domain']:
                                    # dictionary to store all DISCOVERED parameters
                                    lsdb_param_dct = {}
                                    # Domain ID described by this LSR. 
                                    # A (self) keyword after the domain ID indicates that LSR describes the local switch.
                                    domain_self_tag_lst = line_to_list(comp_dct['lsdb_domain'], line)
                                    # lsdb_link_comp
                                    while not (re.search(comp_dct['lsdb_link'],line) or re.search(comp_dct['switchcmd_end'],line)):
                                        line = file.readline()
                                        if not line:
                                            break
                                        match_dct ={comp_key: comp_dct[comp_key].match(line) for comp_key in comp_keys}
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
                                    lsdb_link_lst = line_to_list(comp_dct['lsdb_link'], line)
                                    # add link information to the global list with current switch and lsdb information 
                                    lsdb_lst.append([*switch_info_lst[:6], *domain_self_tag_lst,*lsdb_param_lst, *lsdb_link_lst])
                        # lsdb section end
                status_info('ok', max_title, len(info))
            # if switch in Access Gateway mode then skip
            else:
                status_info('skip', max_title, len(info))        

        # convert list to DataFrame
        isl_df = dfop.list_to_dataframe(isl_lst, max_title,  sheet_title_import='isl')
        trunk_df = dfop.list_to_dataframe(trunk_lst, max_title,  sheet_title_import='isl', columns_title_import = 'trunk_columns')
        porttrunkarea_df = dfop.list_to_dataframe(porttrunkarea_lst, max_title,  sheet_title_import='isl', columns_title_import = 'porttrunkarea_columns')
        lsdb_df = dfop.list_to_dataframe(lsdb_lst, max_title,  sheet_title_import='isl', columns_title_import = 'lsdb_columns')
        # saving data to csv file
        data_lst = [isl_df, trunk_df, porttrunkarea_df, lsdb_df]
        # save_data(report_constant_lst, data_names, *data_lst)
        # write data to sql db
        write_db(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        isl_df, trunk_df, porttrunkarea_df, lsdb_df = verify_data(report_constant_lst, data_names, *data_lst)
        data_lst = [isl_df, trunk_df, porttrunkarea_df, lsdb_df]

        data_lst = verify_data(report_constant_lst, data_names, *data_lst)
        isl_df, trunk_df, porttrunkarea_df, lsdb_df = data_lst

    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)
    
    return isl_df, trunk_df, porttrunkarea_df, lsdb_df




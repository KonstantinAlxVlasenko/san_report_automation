
"""Module to extract fabrics information"""

import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects
from common_operations_miscellaneous import verify_force_run
from common_operations_dataframe import list_to_dataframe
from common_operations_table_report import dataframe_to_report
from common_operations_database import read_db, write_db


def fabric_membership_extract(switch_params_df, report_creation_info_lst):
    """
    Function to extract from principal switch configuration 
    list of switches in fabric including AG switches
    """

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['fabricshow', 'ag_principal']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    data_lst = read_db(report_constant_lst, report_steps_dct, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, max_title)
    
    if force_run:                 
        print('\nEXTRACTING FABRICS INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # number of switches to check
        switch_num = len(switch_params_df.index)
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        fabricshow_lst = []
        ag_principal_lst = []    
        # data imported from init file to extract values from config file
        *_, comp_keys, match_keys, comp_dct = data_extract_objects('fabricshow', max_title)
        ag_params = columns_import('fabricshow', max_title, 'ag_params')  
        
        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():        

            switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 
                                'switch_index', 'SwitchName', 'switchWwn', 'switchRole', 
                                'Fabric_ID', 'FC_Router', 'switchMode']
            switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
            ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False 
            
            sshow_file, _, _, switch_index, switch_name, _, switch_role = switch_info_lst[:7]

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} fabric environment. Switch role: {switch_role}'
            print(info, end =" ")
            
            collected = {'fabricshow': False, 'ag_principal': False}
            
            # check config of Principal switch only 
            if switch_role == 'Principal':
                # principal_switch_lst contains sshow_file, chassis_name, chassis_wwn, switch_index, switch_name, switch_fid
                principal_switch_lst = [*switch_info_lst[:6], *switch_info_lst[7:9]]
                                        
                # search control dictionary. continue to check sshow_file until all parameters groups are found
                with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                    # check file until all groups of parameters extracted
                    while not all(collected.values()):
                        line = file.readline()
                        if not line:
                            break
                        # fabricshow section start
                        if re.search(r'^(SWITCHCMD /fabos/cliexec/)?fabricshow\s*:$', line):
                            # when section is found corresponding collected dict values changed to True
                            collected['fabricshow'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break
                            while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                                line = file.readline()
                                # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # match_keys ['fabricshow_match'] 
                                # 'fabricshow_match'
                                if match_dct[match_keys[0]]:
                                    fabricshow_lst.append(line_to_list(comp_dct[comp_keys[0]], line, *principal_switch_lst))                                      
                                if not line:
                                    break
                        # ag_principal section start
                        # switchcmd_agshow_comp
                        if re.search(comp_dct[comp_keys[4]], line):
                            collected['ag_principal'] = True
                            # if switch in LS mode switch to required LS number
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break                        
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[10]], line):
                                match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # ag_num_match
                                if match_dct[match_keys[5]]:
                                    # dictionary to store all DISCOVERED switch ports information
                                    # collecting data only for the logical switch in current loop
                                    # Access Gateway common information dictionary
                                    ag_info_dct = {}
                                    # Attached F-Port information dictionary
                                    ag_attach_dct = {}
                                    # Access Gateway F-Port information dictionary
                                    ag_fport_dct = {}
                                    # Domaid ID, port_ID, port_index dictionary 
                                    did_port_dct = {}
                                    
                                    # move cursor to one line down to get inside while loop
                                    line = file.readline()                                
                                    # ag_switchcmd_end_comp
                                    while not re.search(comp_dct[comp_keys[9]], line):
                                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                        # ag_info_match
                                        if match_dct[match_keys[6]]:
                                            ag_info_dct[match_dct[match_keys[6]].group(1).rstrip()] = match_dct[match_keys[6]].group(2).rstrip()
                                        # ag_attached_match
                                        elif match_dct[match_keys[7]]:
                                            # if Attached F-Port information dictionary is empty than create dictionary with N-Port ID(s) as keys and empty lists as values
                                            # if ag_attach_dct has been already created (not empty) then it's preserved
                                            ag_attach_dct = ag_attach_dct or dict((n_portid, []) for n_portid in ag_info_dct['N-Port ID(s)'].split(','))
                                            # extracting attached F-port data from line to list
                                            ag_attach_lst = line_to_list(comp_dct[comp_keys[7]], line)
                                            # getting port_ID of N-port from port_id of F-port
                                            n_portid = ag_attach_lst[0][:-2] + '00'
                                            # adding current line F-port information to Attached F-Port information dictionary 
                                            if n_portid in ag_attach_dct.keys():
                                                ag_attach_dct[n_portid].append(ag_attach_lst)
                                        # ag_fport_match
                                        elif match_dct[match_keys[8]]:
                                            # create Access Gateway F-Port information dictionary
                                            ag_fport_dct = ag_fport_dct or dict((n_portid, []) for n_portid in ag_info_dct['N-Port ID(s)'].split(','))
                                            # extracting access gateway F-port data from line to list
                                            ag_fport_lst = line_to_list(comp_dct[comp_keys[8]], line)
                                            # getting port_ID of N-port from port_id of F-port
                                            n_portid = ag_fport_lst[1][:-2] + '00'
                                            # adding current line F-port information to Access Gateway F-Port information dictionary
                                            if n_portid in ag_fport_dct.keys():                                                
                                                ag_fport_dct[n_portid].append(ag_fport_lst)
                                        line = file.readline()                                                                                 
                                        if not line:
                                            break

                                    # list of N-ports extracted from N-Port ID(s) line
                                    n_portids_lst = ag_info_dct['N-Port ID(s)'].split(',')
                                    # (domain_id, n_portid)
                                    did_port_lst = [(int(n_portid[:4], 0), n_portid) for n_portid in n_portids_lst]
                                    # creating dictionary with n_portid as keys and (domain_id, n_portid) as values
                                    did_port_dct = {port[1]:list(port) for port in did_port_lst}

                                    # change values representation in dictionaries
                                    # before {n_portid: [(port_id_1, port_wwn_1, f-port_num_1)], [(port_id_2, port_wwn_2, f-port_num_2)]}
                                    # after {n_portid: [(port_id_1, port_id_2), (port_wwn_1, port_wwn_2), (f-port_num_1, f-port_num_1)]                                      
                                    ag_attach_dct = {n_portid:list(zip(*ag_attach_dct[n_portid])) 
                                                     for n_portid in n_portids_lst if ag_attach_dct.get(n_portid)}
                                    ag_fport_dct = {n_portid:list(zip(*ag_fport_dct[n_portid])) 
                                                    for n_portid in n_portids_lst if ag_fport_dct.get(n_portid)}
                                        
                                    # add connected switch port_index to did_port_dct extracted from ag_attach_dct
                                    # (domain_id, n_portid, n_port_index)
                                    # if no port_index then add None 
                                    for n_portid in n_portids_lst:
                                        if ag_attach_dct.get(n_portid):
                                            did_port_dct[n_portid].append(ag_attach_dct[n_portid][2][0])
                                        else:
                                            did_port_dct[n_portid].append(None)
                                    
                                    # for each element of list convert tuples to strings
                                    # if no data extracted for the n_portid then add None for each parameter
                                    for n_portid in n_portids_lst:
                                        if ag_attach_dct.get(n_portid):
                                            ag_attach_dct[n_portid] = [', '.join(v) for v in ag_attach_dct[n_portid]]
                                        else:
                                            ag_attach_dct[n_portid] = [None]*3                                            
                                    for n_portid in n_portids_lst:
                                        if ag_fport_dct.get(n_portid):
                                            ag_fport_dct[n_portid] = [', '.join(v) for v in ag_fport_dct[n_portid]]
                                        else:
                                            ag_fport_dct[n_portid] = [None]*3

                                    # getting data from ag_info_dct in required order
                                    ag_info_lst = [ag_info_dct.get(param, None) for param in ag_params]               
                                    # appending list with only REQUIRED ag info for the current loop iteration to the list with all ag switch info
                                    for n_portid in n_portids_lst:
                                        ag_principal_lst.append([*principal_switch_lst[:-1], *ag_info_lst, *did_port_dct[n_portid],
                                                            *ag_attach_dct[n_portid], *ag_fport_dct[n_portid]])
                                else:
                                    line = file.readline()
                                if not line:
                                    break
                        # ag_principal section end       
                status_info('ok', max_title, len(info))
            else:
                status_info('skip', max_title, len(info))

        # convert list to DataFrame
        fabricshow_df = list_to_dataframe(fabricshow_lst, max_title, sheet_title_import='fabricshow')
        ag_principal_df = list_to_dataframe(ag_principal_lst, max_title, sheet_title_import='fabricshow', columns_title_import = 'ag_columns')
        # saving data to csv file
        data_lst = [fabricshow_df, ag_principal_df]
        # save_data(report_constant_lst, data_names, *data_lst)
        write_db(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    else:
        fabricshow_df, ag_principal_df = verify_data(report_constant_lst, data_names, *data_lst)
        data_lst = [fabricshow_df, ag_principal_df]

    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dataframe_to_report(data_frame, data_name, report_creation_info_lst)
            
    return fabricshow_df, ag_principal_df


"""Module to extract connected devices information"""

import os
import re

import pandas as pd
import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop


def connected_devices_extract(switch_params_df, project_constants_lst):
    """Function to extract connected devices information
    (fdmi, nsshow, nscamshow)"""
           
    # # report_steps_dct contains current step desciption and force and export tags
    # report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # # report_constant_lst contains information: 
    # # customer_name, project directory, database directory, max_title
    # *_, max_title = report_constant_lst


    project_steps_df, max_title, data_dependency_df, report_requisites_sr, *_ = project_constants_lst

    if pd.notna(report_requisites_sr['nsshow_dedicated_folder']):
        nsshow_folder = os.path.normpath(report_requisites_sr['nsshow_dedicated_folder'])
    else:
        nsshow_folder = None
    
    # names to save data obtained after current module execution
    data_names = ['fdmi', 'nsshow', 'nscamshow', 'nsshow_dedicated', 'nsportshow']
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')

    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
    if force_run:      
        print('\nEXTRACTING INFORMATION ABOUT CONNECTED DEVICES (FDMI, NSSHOW, NSCAMSHOW) ...\n')           
        
        # number of switches to check
        switch_num = len(switch_params_df.index)   
     
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('ns_fdmi', max_title)
        fdmi_params, fdmi_params_add, nsshow_params, nsshow_params_add = dfop.list_from_dataframe(re_pattern_df, 'fdmi_params', 'fdmi_params_add', 
                                                                                                        'nsshow_params', 'nsshow_params_add')
        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        fdmi_lst = []
        # lists with local Name Server (NS) information 
        nsshow_lst = []
        nscamshow_lst = []
        nsshow_dedicated_lst = []
        # list with zoning enforcement
        nsportshow_lst = []
        
        # dictionary with required to collect nsshow data
        # first element of list is regular expression pattern number, second - list to collect data
        nsshow_dct = {'nsshow': ['switchcmd_nsshow', nsshow_lst], 'nscamshow': ['switchcmd_nscamshow', nscamshow_lst]}
        
        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():       
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} connected devices'
            print(info, end =" ")
            current_config_extract(fdmi_lst, nsshow_dct, nsportshow_lst, pattern_dct, 
                                    switch_params_sr,
                                    fdmi_params, fdmi_params_add, nsshow_params, nsshow_params_add)                    
            meop.status_info('ok', max_title, len(info))
        
        # check files in dedicated nsshow folder
        if nsshow_folder:
            print('\nEXTRACTING NAMESERVER INFORMATION FROM DEDICATED FILES ...\n')
            # collects files in folder with txt extension
            txt_files = fsop.find_files(nsshow_folder, max_title, filename_extension='txt')
            log_files = fsop.find_files(nsshow_folder, max_title, filename_extension='log')
            nsshow_files_lst = txt_files + log_files
            # number of files to check
            configs_num = len(nsshow_files_lst)  
            if configs_num:
                for i, nsshow_file in enumerate(nsshow_files_lst):       
                    # file name with extension
                    filename_wext = os.path.basename(nsshow_file)
                    # remove extension from filename
                    filename, _ = os.path.splitext(filename_wext)
                    # current operation information string
                    info = f'[{i+1} of {configs_num}]: {filename} dedicated nsshow config.'
                    print(info, end =" ")
                    prev_nsshow_dedicated_lst = nsshow_dedicated_lst.copy()
                    # parse nsshow information from current file
                    parse_nsshow_dedicated(nsshow_file, nsshow_dedicated_lst, pattern_dct, nsshow_params, nsshow_params_add)
                    if prev_nsshow_dedicated_lst != nsshow_dedicated_lst:
                        meop.status_info('ok', max_title, len(info))
                    else:
                        meop.status_info('empty', max_title, len(info))
        
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'fdmi_columns', 'nsshow_columns', 'nsshow_columns', 'nsshow_columns', 'nsportshow_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, fdmi_lst, nsshow_lst, nscamshow_lst, nsshow_dedicated_lst, nsportshow_lst)
        fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df, *_ = data_lst          
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  

    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df


def parse_nsshow_dedicated(nsshow_file, nsshow_dedicated_lst, pattern_dct, nsshow_params, nsshow_params_add):
    """Function to extract NameSerevr information from dedicated file"""               
    
    with open(nsshow_file, encoding='utf-8', errors='ignore') as file:
        line = file.readline()
        while line:
            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
            # port_pid_match
            if match_dct['port_pid']:
                # dictionary to store all DISCOVERED switch ports information
                nsshow_port_dct = {}
                # current connected device wwnp
                pid = dsop.line_to_list(pattern_dct['port_pid'], line)
                # move cursor to one line down to get inside while loop
                line = file.readline()                                
                # pid_switchcmd_end_comp
                while not re.search(pattern_dct['port_pid'], line):
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # nsshow_port_match
                    if match_dct['fdmi_port']:
                        nsshow_port_dct[match_dct['fdmi_port'].group(1).rstrip()] = match_dct['fdmi_port'].group(2).rstrip()
                    line = file.readline()
                    if not line:
                        break
                # adding additional parameters and values to the fdmi_dct
                dsop.update_dct(nsshow_params_add[6:], pid, nsshow_port_dct)               
                # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                nsshow_dedicated_lst.append([nsshow_port_dct.get(nsshow_param) for nsshow_param in nsshow_params])
            else:
                line = file.readline()


def current_config_extract(fdmi_lst, nsshow_dct, nsportshow_lst, pattern_dct, 
                            switch_params_sr,
                            fdmi_params, fdmi_params_add, nsshow_params, nsshow_params_add):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                        'SwitchName', 'switchWwn', 'switchMode']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False           
    
    sshow_file, *_, switch_index, switch_name, _, switch_mode = switch_info_lst            
                
    # search control dictionary. continue to check sshow_file until all parameters groups are found
    # Name Server service started only in Native mode
    collected = {'fdmi': False, 'nsshow': False, 'nscamshow': False, 'nsportshow': False} \
        if switch_mode == 'Native' else {'fdmi': False, 'nsportshow': False}

    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()                        
            if not line:
                break
            # fdmi section start   
            # switchcmd_fdmishow_comp
            if re.search(pattern_dct['switchcmd_fdmishow'], line) and not collected['fdmi']:
                collected['fdmi'] = True
                if ls_mode_on:
                    while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                        line = file.readline()
                        if not line:
                            break                        
                # local_database_comp
                while not re.search(pattern_dct['local_database'], line):
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # wwnp_match
                    if match_dct['wwpn']:
                        # dictionary to store all DISCOVERED switch ports information
                        # collecting data only for the logical switch in current loop
                        fdmi_dct = {}
                        # switch_info and current connected device wwnp
                        switch_wwnp = dsop.line_to_list(pattern_dct['wwpn'], line, *switch_info_lst[:6])
                        # move cursor to one line down to get inside while loop
                        line = file.readline()                                
                        # wwnp_local_comp
                        while not re.search(pattern_dct['wwpn_local'], line):
                            line = file.readline()
                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                            # fdmi_port_match
                            if match_dct['fdmi_port']:
                                fdmi_dct[match_dct['fdmi_port'].group(1).rstrip()] = match_dct['fdmi_port'].group(2).rstrip()                                       
                            if not line:
                                break
                        # adding additional parameters and values to the fdmi_dct
                        dsop.update_dct(fdmi_params_add, switch_wwnp, fdmi_dct)               
                        # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                        fdmi_lst.append([fdmi_dct.get(param, None) for param in fdmi_params])
                    else:
                        line = file.readline()
                    if not line:
                        break                                
            # fdmi section end
            # ns_portshow section start   
            # switchcmd_nsportshow_comp
            if re.search(pattern_dct['switchcmd_nsportshow'], line) and not collected['nsportshow']:
                collected['nsportshow'] = True
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
                    if match_dct['ns_portshow']:
                        port_enforcement = dsop.line_to_list(pattern_dct['ns_portshow'], line, *switch_info_lst[:6])
                        nsportshow_lst.append(port_enforcement)
                    if not line:
                        break                                
            # ns_portshow section end      
            # only switches in Native mode have Name Server service started 
            if switch_mode == 'Native':
                # nsshow section start (nsshow_type: nsshow, nscamshow)                 
                for nsshow_type in nsshow_dct.keys():
                    # unpacking re number and list to save REQUIRED params
                    ns_pattern_name, ns_lst = nsshow_dct[nsshow_type]
                    # switchcmd_nsshow_comp, switchcmd_nscamshow_comp
                    if re.search(pattern_dct[ns_pattern_name], line) and not collected[nsshow_type]:
                        collected[nsshow_type] = True
                        if ls_mode_on:
                            while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                line = file.readline()
                                if not line:
                                    break                        
                        # switchcmd_end_comp
                        while not re.search(pattern_dct['switchcmd_end'], line):
                            # line = file.readline()
                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                            # port_pid__match
                            if match_dct['port_pid']:
                                # dictionary to store all DISCOVERED switch ports information
                                # collecting data only for the logical switch in current loop
                                nsshow_port_dct = {}
                                # switch_info and current connected device wwnp
                                switch_pid = dsop.line_to_list(pattern_dct['port_pid'], line, *switch_info_lst[:6])
                                # move cursor to one line down to get inside while loop
                                line = file.readline()                                
                                # pid_switchcmd_end_comp
                                while not re.search(pattern_dct['pid_switchcmd_end'], line):
                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                    # nsshow_port_match
                                    if match_dct['fdmi_port']:
                                        nsshow_port_dct[match_dct['fdmi_port'].group(1).rstrip()] = match_dct['fdmi_port'].group(2).rstrip()
                                    line = file.readline()
                                    if not line:
                                        break
                                        
                                # adding additional parameters and values to the fdmi_dct
                                dsop.update_dct(nsshow_params_add, switch_pid, nsshow_port_dct)               
                                # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                                ns_lst.append([nsshow_port_dct.get(nsshow_param, None) for nsshow_param in nsshow_params])
                            else:
                                line = file.readline()
                            if not line:
                                break                                
                # nsshow section end


"""Module to extract connected devices information"""

import os
import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop

# import dataframe_operations as dfop
import pandas as pd
# from common_operations_database import read_db, write_db
# from common_operations_dataframe import list_to_dataframe
# from common_operations_filesystem import find_files
# from common_operations_miscellaneous import (line_to_list, status_info,
#                                              update_dct, verify_data,
#                                              verify_force_run)
# from common_operations_servicefile import (columns_import,
#                                            data_extract_objects,
#                                            dct_from_columns)
# from common_operations_table_report import dataframe_to_report


def connected_devices_extract(switch_params_df, report_entry_sr, report_creation_info_lst):
    """Function to extract connected devices information
    (fdmi, nsshow, nscamshow)"""
           
    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    if pd.notna(report_entry_sr['nsshow_dedicated_folder']):
        nsshow_folder = os.path.normpath(report_entry_sr['nsshow_dedicated_folder'])
    else:
        nsshow_folder = None
    
    # names to save data obtained after current module execution
    data_names = ['fdmi', 'nsshow', 'nscamshow', 'nsshow_dedicated']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)
    
    if force_run:      
        print('\nEXTRACTING INFORMATION ABOUT CONNECTED DEVICES (FDMI, NSSHOW, NSCAMSHOW) ...\n')   
        
        # # extract chassis parameters names from init file
        # switch_columns = sfop.columns_import('switch', max_title, 'columns')
        
        # number of switches to check
        switch_num = len(switch_params_df.index)   
     
        # data imported from init file to extract values from config file
        params, params_add, comp_keys, match_keys, comp_dct = sfop.data_extract_objects('connected_dev', max_title)
        nsshow_params, nsshow_params_add = sfop.columns_import('connected_dev', max_title, 'nsshow_params', 'nsshow_params_add')

        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        fdmi_lst = []
        # lists with local Name Server (NS) information 
        nsshow_lst = []
        nscamshow_lst = []
        nsshow_dedicated_lst = []
        
        # dictionary with required to collect nsshow data
        # first element of list is regular expression pattern number, second - list to collect data
        nsshow_dct = {'nsshow': [5, nsshow_lst], 'nscamshow': [6, nscamshow_lst]}
        
        # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():       
            # # data unpacking from iter param
            # # dictionary with parameters for the current chassis
            # switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
            # switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
            #                     'SwitchName', 'switchWwn', 'switchMode']
            # switch_info_lst = [switch_params_data_dct.get(key) for key in switch_info_keys]
            # ls_mode_on = True if switch_params_data_dct['LS_mode'] == 'ON' else False


            switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                                'SwitchName', 'switchWwn', 'switchMode']
            switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
            ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False           
            
            sshow_file, *_, switch_index, switch_name, _, switch_mode = switch_info_lst            
                        
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} connected devices'
            print(info, end =" ")
                       
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            # Name Server service started only in Native mode
            collected = {'fdmi': False, 'nsshow': False, 'nscamshow': False} \
                if switch_mode == 'Native' else {'fdmi': False}
    
            with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                # check file until all groups of parameters extracted
                while not all(collected.values()):
                    line = file.readline()                        
                    if not line:
                        break
                    # fdmi section start   
                    # switchcmd_fdmishow_comp
                    if re.search(comp_dct[comp_keys[0]], line):
                        collected['fdmi'] = True
                        if ls_mode_on:
                            while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                line = file.readline()
                                if not line:
                                    break                        
                        # local_database_comp
                        while not re.search(comp_dct[comp_keys[4]], line):
                            match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # wwnp_match
                            if match_dct[match_keys[1]]:
                                # dictionary to store all DISCOVERED switch ports information
                                # collecting data only for the logical switch in current loop
                                fdmi_dct = {}
                                # switch_info and current connected device wwnp
                                switch_wwnp = dsop.line_to_list(comp_dct[comp_keys[1]], line, *switch_info_lst[:6])
                                # move cursor to one line down to get inside while loop
                                line = file.readline()                                
                                # wwnp_local_comp
                                while not re.search(comp_dct[comp_keys[3]], line):
                                    line = file.readline()
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # fdmi_port_match
                                    if match_dct[match_keys[2]]:
                                        fdmi_dct[match_dct[match_keys[2]].group(1).rstrip()] = match_dct[match_keys[2]].group(2).rstrip()                                       
                                    if not line:
                                        break
                                        
                                # adding additional parameters and values to the fdmi_dct
                                dsop.update_dct(params_add, switch_wwnp, fdmi_dct)               
                                # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                                fdmi_lst.append([fdmi_dct.get(param, None) for param in params])
                            else:
                                line = file.readline()
                            if not line:
                                break                                
                    # fdmi section end                   
                    # only switches in Native mode have Name Server service started 
                    if switch_mode == 'Native':
                        # nsshow section start                 
                        for nsshow_type in nsshow_dct.keys():
                            # unpacking re number and list to save REQUIRED params
                            re_num, ns_lst = nsshow_dct[nsshow_type]
                            # switchcmd_nsshow_comp, switchcmd_nscamshow_comp
                            if re.search(comp_dct[comp_keys[re_num]], line):
                                collected[nsshow_type] = True
                                if ls_mode_on:
                                    while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                        line = file.readline()
                                        if not line:
                                            break                        
                                # switchcmd_end_comp
                                while not re.search(comp_dct[comp_keys[9]], line):
                                    # line = file.readline()
                                    match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # port_pid__match
                                    if match_dct[match_keys[7]]:
                                        # dictionary to store all DISCOVERED switch ports information
                                        # collecting data only for the logical switch in current loop
                                        nsshow_port_dct = {}
                                        # switch_info and current connected device wwnp
                                        switch_pid = dsop.line_to_list(comp_dct[comp_keys[7]], line, *switch_info_lst[:6])
                                        # move cursor to one line down to get inside while loop
                                        line = file.readline()                                
                                        # pid_switchcmd_end_comp
                                        while not re.search(comp_dct[comp_keys[8]], line):
                                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                            # nsshow_port_match
                                            if match_dct[match_keys[2]]:
                                                nsshow_port_dct[match_dct[match_keys[2]].group(1).rstrip()] = match_dct[match_keys[2]].group(2).rstrip()
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
                    parse_nsshow_dedicated(nsshow_file, nsshow_dedicated_lst, comp_dct, comp_keys, nsshow_params, nsshow_params_add)
                    if prev_nsshow_dedicated_lst != nsshow_dedicated_lst:
                        meop.status_info('ok', max_title, len(info))
                    else:
                        meop.status_info('empty', max_title, len(info))
        
        # convert list to DataFrame
        fdmi_df = dfop.list_to_dataframe(fdmi_lst, max_title, sheet_title_import='connected_dev')
        nsshow_df = dfop.list_to_dataframe(nsshow_lst, max_title, sheet_title_import='connected_dev', columns_title_import = 'nsshow_columns')
        nscamshow_df = dfop.list_to_dataframe(nscamshow_lst, max_title, sheet_title_import='connected_dev', columns_title_import = 'nsshow_columns')
        nsshow_dedicated_df = dfop.list_to_dataframe(nsshow_dedicated_lst, max_title, sheet_title_import='connected_dev', columns_title_import = 'nsshow_columns')
        # saving data to csv file
        data_lst = [fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df]
        # save_data(report_constant_lst, data_names, *data_lst)
        # write data to sql db
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)  

    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        # fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        # data_lst = [fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df]

        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df = data_lst

    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)

    return fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df


def parse_nsshow_dedicated(nsshow_file, nsshow_dedicated_lst, comp_dct, comp_keys, nsshow_params, nsshow_params_add):
    """Function to extract NameSerevr information from dedicated file"""               
    
    with open(nsshow_file, encoding='utf-8', errors='ignore') as file:
        line = file.readline()
        while line:
            match_dct = {comp_key: comp_dct[comp_key].match(line) for comp_key in comp_keys}
            # port_pid_match
            if match_dct['port_pid']:
                # dictionary to store all DISCOVERED switch ports information
                nsshow_port_dct = {}
                # current connected device wwnp
                pid = dsop.line_to_list(comp_dct['port_pid'], line)
                # move cursor to one line down to get inside while loop
                line = file.readline()                                
                # pid_switchcmd_end_comp
                while not re.search(comp_dct['port_pid'], line):
                    match_dct = {comp_key: comp_dct[comp_key].match(line) for comp_key in comp_keys}
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

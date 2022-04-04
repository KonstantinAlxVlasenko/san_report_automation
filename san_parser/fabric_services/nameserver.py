"""Module to extract connected devices information (fdmishow, nsshow, nscamshow, nsportshow)"""

import os
import re

# import pandas as pd
# import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .nameserver_sections import (fdmi_section_extract,
                                  nsportshow_section_extract,
                                  nsshow_section_extract,
                                  nsshow_file_extract)


def connected_devices_extract(switch_params_df, project_constants_lst):
    """Function to extract connected devices information
    (fdmi, nsshow, nscamshow)"""
           
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst
    
    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'nameserver_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data  
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
        nsshow_manual_lst = []
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
        
        nsshow_folder = report_requisites_sr['switch_nsshow_folder']

        # check files in dedicated nsshow folder
        if nsshow_folder:
            print('\nEXTRACTING NAMESERVER INFORMATION FROM DEDICATED FILES ...\n')
            
            # collects files in folder with txt extension
            txt_files = fsop.find_files(nsshow_folder, max_title, filename_extension='txt')
            log_files = fsop.find_files(nsshow_folder, max_title, filename_extension='log')
            nsshow_files_lst = txt_files + log_files
            nsshow_manual_files_extract(nsshow_files_lst, nsshow_manual_lst, pattern_dct,
                                    nsshow_params, nsshow_params_add, max_title)
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'fdmi_columns', 'nsshow_columns', 'nsshow_columns', 'nsshow_columns', 'nsportshow_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, fdmi_lst, nsshow_lst, nscamshow_lst, nsshow_manual_lst, nsportshow_lst)
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
            if re.search(pattern_dct['switchcmd_fdmishow'], line) and not collected['fdmi']:
                collected['fdmi'] = True
                line = meop.goto_switch_context(ls_mode_on, line, file, switch_index)
                line = fdmi_section_extract(fdmi_lst, pattern_dct, switch_info_lst, 
                                            fdmi_params, fdmi_params_add, line, file)
            # fdmi section end
            # ns_portshow section start   
            if re.search(pattern_dct['switchcmd_nsportshow'], line) and not collected['nsportshow']:
                collected['nsportshow'] = True
                line = meop.goto_switch_context(ls_mode_on, line, file, switch_index)
                line = nsportshow_section_extract(nsportshow_lst, pattern_dct, switch_info_lst, line, file)                                                   
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
                        line = meop.goto_switch_context(ls_mode_on, line, file, switch_index)
                        line = nsshow_section_extract(ns_lst, pattern_dct, switch_info_lst, 
                                                        nsshow_params, nsshow_params_add, line, file)
                # nsshow section end


def nsshow_manual_files_extract(nsshow_files_lst, nsshow_manual_lst, pattern_dct,
                            nsshow_params, nsshow_params_add, max_title):
    """Function to check NameServer files collected manually and extract values from them."""
    
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
            prev_nsshow_dedicated_lst = nsshow_manual_lst.copy()
            # parse nsshow information from current file
            nsshow_file_extract(nsshow_file, nsshow_manual_lst, pattern_dct, nsshow_params, nsshow_params_add)
            if prev_nsshow_dedicated_lst != nsshow_manual_lst:
                meop.status_info('ok', max_title, len(info))
            else:
                meop.status_info('empty', max_title, len(info))

"""Module to extract connected devices information (fdmishow, nsshow, nscamshow, nsportshow)"""

import os
import re

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.regular_expression_operations as reop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop

from .nameserver_sections import (nsshow_file_extract,
                                  san_device_ports_section_extract)


def connected_devices_extract(switch_params_df, project_constants_lst):
    """Function to extract connected devices information
    (fdmi, nsshow, nscamshow)"""
           
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst
    
    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'nameserver_collection')
    # module information
    meop.show_module_info(project_steps_df, data_names)
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
        fdmi_params, fdmi_params_add, nsshow_params, nsshow_params_add = \
            dfop.list_from_dataframe(re_pattern_df, 'fdmi_params', 'fdmi_params_add', 'nsshow_params', 'nsshow_params_add')
        # nested list(s) to store required values of the module in defined order for all switches in SAN
        # san fdmishow information
        san_fdmi_lst = []
        # lists with local amd fabric Name Server (NS) information in san 
        san_nsshow_lst = []
        san_nscamshow_lst = []
        san_nsshow_manual_lst = []
        # list with zoning enforcement information (HARD WWN,  HARD PORT, etc) in san
        san_nsportshow_lst = []
        
        for i, switch_params_sr in switch_params_df.iterrows():       
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} connected devices'
            print(info, end =" ")
            sw_fdmi_lst = current_config_extract(san_fdmi_lst, san_nsshow_lst, san_nscamshow_lst, san_nsportshow_lst, 
                                                    pattern_dct, switch_params_sr,
                                                    fdmi_params, fdmi_params_add, nsshow_params, nsshow_params_add)                   
            meop.show_collection_status(sw_fdmi_lst, max_title, len(info))
        
        nsshow_folder = report_requisites_sr['switch_nsshow_folder']
        # check files in dedicated nsshow folder
        if nsshow_folder:
            print('\nEXTRACTING NAMESERVER INFORMATION FROM DEDICATED FILES ...\n')
            
            # collects files in folder with txt extension
            txt_files = fsop.find_files(nsshow_folder, max_title, filename_extension='txt')
            log_files = fsop.find_files(nsshow_folder, max_title, filename_extension='log')
            nsshow_files_lst = txt_files + log_files
            nsshow_manual_files_extract(nsshow_files_lst, san_nsshow_manual_lst, pattern_dct,
                                    nsshow_params, nsshow_params_add, max_title)
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'fdmi_columns', 'nsshow_columns', 'nsshow_columns', 'nsshow_columns', 'nsportshow_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, san_fdmi_lst, san_nsshow_lst, san_nscamshow_lst, san_nsshow_manual_lst, san_nsportshow_lst)
        fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df, *_ = data_lst          
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  

    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df


def current_config_extract(san_fdmi_lst, san_nsshow_lst, san_nscamshow_lst, san_nsportshow_lst, 
                            pattern_dct, switch_params_sr,
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
                line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                line, sw_fdmi_lst = san_device_ports_section_extract(san_fdmi_lst, pattern_dct, line, file, 
                                                                        switch_info_lst, fdmi_params, fdmi_params_add,
                                                                        device_start_pattern_name='wwpn', 
                                                                        device_stop_pattern_name='wwpn_local',
                                                                        cmd_stop_pattern_name='local_database')
            # fdmi section end
            # ns_portshow section start (zoning_enforcement information (HARD WWN,  HARD PORT, etc)) 
            elif re.search(pattern_dct['switchcmd_nsportshow'], line) and not collected['nsportshow']:
                collected['nsportshow'] = True
                line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                line, sw_nsportshow_lst = reop.extract_list_from_line(san_nsportshow_lst, pattern_dct, line, file, 
                                                                        extract_pattern_name='ns_portshow', 
                                                                        save_local=True, line_add_values=switch_info_lst[:6])                                               
            # ns_portshow section end      
            # only switches in Native mode runing Name Server service 
            if switch_mode == 'Native':
                # nsshow section start
                if re.search(pattern_dct['switchcmd_nsshow'], line) and not collected['nsshow']:
                    collected['nsshow'] = True
                    line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line, sw_nsshow_lst = san_device_ports_section_extract(san_nsshow_lst, pattern_dct, line, file, 
                                                                            switch_info_lst, nsshow_params, nsshow_params_add,
                                                                            device_start_pattern_name='port_pid', 
                                                                            device_stop_pattern_name='pid_switchcmd_end')
                # nscamshow section start
                elif re.search(pattern_dct['switchcmd_nscamshow'], line) and not collected['nscamshow']:
                    collected['nscamshow'] = True
                    line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line, sw_nscamshow_lst = san_device_ports_section_extract(san_nscamshow_lst, pattern_dct, line, file, 
                                                                                switch_info_lst, nsshow_params, nsshow_params_add,
                                                                                device_start_pattern_name='port_pid', 
                                                                                device_stop_pattern_name='pid_switchcmd_end')
                # nsshow section end
    return sw_fdmi_lst


def nsshow_manual_files_extract(nsshow_files_lst, san_nsshow_manual_lst, pattern_dct,
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
            prev_nsshow_dedicated_lst = san_nsshow_manual_lst.copy()
            # parse nsshow information from current file
            nsshow_file_extract(nsshow_file, san_nsshow_manual_lst, pattern_dct, nsshow_params, nsshow_params_add)
            if prev_nsshow_dedicated_lst != san_nsshow_manual_lst:
                meop.status_info('ok', max_title, len(info))
            else:
                meop.status_info('empty', max_title, len(info))

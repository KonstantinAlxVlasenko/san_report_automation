"""Module to extract Blade system information"""

import os
import re

import pandas as pd

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop

from .bladesystem_sections import *


def blade_system_extract(project_constants_lst):
    """Function to extract blade systems information"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst
    blade_folder = report_requisites_sr['blade_showall_folder']

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'blade_collection')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data 
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
    if force_run: 
        # lists to store only REQUIRED infromation
        # collecting data for all blades during looping
        # list containing enclosure, blade and hba information for all blade systems
        san_blade_lst = []
        # list containing enclosure and interconnect modules information for all blade systems
        san_module_lst = []
        # list containing virtual connect ports information for all blade systems
        san_blade_vc_lst = []

        pattern_dct, re_pattern_df = sfop.regex_pattern_import('blades', max_title)
        enclosure_params, module_params, blade_params = dfop.list_from_dataframe(re_pattern_df, 'enclosure_params', 'module_params', 'blade_params')

              
        if blade_folder:    
            print('\nEXTRACTING BLADES SYSTEM INFORMATION ...\n')   
            
            # collects files in folder with txt extension
            txt_files = fsop.find_files(blade_folder, max_title, filename_extension='txt')
            log_files = fsop.find_files(blade_folder, max_title, filename_extension='log')
            noext_files = fsop.find_files(blade_folder, max_title, filename_extension=None)
            blade_configs_lst = txt_files + log_files + noext_files
            # number of files to check
            configs_num = len(blade_configs_lst)  

            if configs_num:
                for i, blade_config in enumerate(blade_configs_lst):       
                    # file name with extension
                    configname_wext = os.path.basename(blade_config)
                    # remove extension from filename
                    configname, _ = os.path.splitext(configname_wext)
                    
                    # current operation information string
                    info = f'[{i+1} of {configs_num}]: {configname} system.'
                    print(info, end =" ")

                    blade_lst, enclosure_vc_lst, info = current_config_extract(san_blade_lst, san_module_lst, san_blade_vc_lst, 
                                                                            pattern_dct, blade_config, info,
                                                                            enclosure_params, module_params, blade_params)                        
                    # show status blades information extraction from file
                    if blade_lst or enclosure_vc_lst:
                        meop.status_info('ok', max_title, len(info))
                    else:
                        meop.status_info('no data', max_title, len(info))
        else:
            # current operation information string
            info = f'Collecting enclosure, interconnect modules, blade servers, hba'
            print(info, end =" ")
            meop.status_info('skip', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'enclosure_columns', 'blade_columns', 'blade_vc_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, san_module_lst, san_blade_lst, san_blade_vc_lst)
        blade_module_df, blade_servers_df, blade_vc_df, *_ = data_lst 
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  

    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        blade_module_df, blade_servers_df, blade_vc_df = data_lst
        
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return blade_module_df, blade_servers_df, blade_vc_df


def current_config_extract(san_blade_lst, san_module_lst, san_blade_vc_lst, 
                            pattern_dct, blade_config, info,
                            enclosure_params, module_params, blade_params):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    # Active Onboard Administrator IP address
    oa_ip = None
    # interconnect modules number
    module_num = 0
    
    # search control dictionary. continue to check file until all parameters groups are found
    collected = {'enclosure': False, 'oa_ip': False, 'module': False, 'servers': False, 'vc': False}
    # if blade_lst remains empty after file checkimng than status_info shows NO_DATA for current file
    blade_lst = []
    enclosure_vc_lst = []
    
    with open(blade_config, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # blade enclosure section start
            if re.search(r'>SHOW ENCLOSURE INFO', line):
                collected['enclosure'] = True
                enclosure_lst = blade_enclosure_section(pattern_dct, file, enclosure_params)
            # blade enclosure section end
            # vc enclosure section start
            elif re.search(r'^ +ENCLOSURE INFORMATION$', line):
                collected['enclosure'] = True
                enclosure_total_dct = vc_enclosure_section(pattern_dct, file, enclosure_params)
            # vc enslosure section end
            # vc fabric connection section start
            elif re.search(r'FABRIC INFORMATION', line):
                info_type = 'Type VC'
                print(info_type, end = " ")
                info = info + " " + info_type
                collected['vc'] = True
                vc_fabric_connection_section(san_blade_vc_lst, enclosure_vc_lst, 
                                    enclosure_total_dct, pattern_dct, file)
            # vc fabric connection section end
            # active onboard administrator ip section start
            elif re.search(r'>SHOW TOPOLOGY *$', line):
                info_type = 'Type Blade Enclosure'
                print(info_type, end = " ")
                info = info + " " + info_type
                collected['oa_ip'] = True
                oa_ip = active_oa_section(file, pattern_dct)
            # active onboard administrator ip section end
            # interconnect modules section start
            elif re.search(r'>SHOW INTERCONNECT INFO ALL', line):
                collected['modules'] = True
                module_num = interconnect_module_section(san_module_lst, pattern_dct,
                                    file, enclosure_lst, oa_ip, module_num, module_params)
            # interconnect modules section end
            # blade server, hba and flb section start
            elif re.search(r'>SHOW SERVER INFO ALL', line):
                collected['servers'] = True
                blade_lst = server_hba_flb_section(san_blade_lst, blade_lst, pattern_dct,
                                                    file, enclosure_lst, blade_params)
            # blade server, hba and flb section end
        
        # adding OA IP to module_comprehensive_lst based on interconnect modules number
        for num in range(-1, -module_num-1, -1):
            san_module_lst[num][3] = oa_ip
    return  blade_lst , enclosure_vc_lst, info




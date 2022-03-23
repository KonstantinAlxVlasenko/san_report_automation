"""Module to download 3PAR configuratin files from STATs and 
extract configuaration information from downloaded and local files"""


import os
import re

import pandas as pd
import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .storage_3par_download import configs_download


def storage_3par_extract(nsshow_df, nscamshow_df, project_constants_lst, software_path_sr):
    """Function to extract 3PAR storage information"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'storage_3par_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
    if force_run:
        # lists to store only REQUIRED infromation
        # collecting data for all systems during looping
        # list containing system parameters
        system_3par_comprehensive_lst = []
        # list containing 3par FC port information
        port_3par_comprehensive_lst = []
        # list containing hosts defined on 3par ports
        host_3par_comprehensive_lst = []

        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('3par', max_title)
        params, params_add = dfop.list_from_dataframe(re_pattern_df, 'system_params', 'system_params_add')

        # verify if 3par systems registered in fabric NameServer
        ns_3par_df = verify_ns_3par(nsshow_df, nscamshow_df, pattern_dct)

        if not ns_3par_df.empty:
            print('\n')
            print('3PAR Storage Systems detected in SAN')
            print(ns_3par_df)
            print('\n')
            # find configuration files to parse (download from STATs, local folder or use configurations
            # downloaded on previous iterations)
            configs_3par_lst = configs_download(ns_3par_df, pattern_dct, project_constants_lst, software_path_sr)

            # configs_3par_lst = configs_download(ns_3par_df, project_folder, local_3par_folder, pattern_dct, report_creation_info_lst)

            if configs_3par_lst:
                print('\nEXTRACTING 3PAR STORAGE INFORMATION ...\n')   
                # number of files to check
                configs_num = len(configs_3par_lst)  

                for i, config_3par in enumerate(configs_3par_lst):       
                    # file name
                    configname = os.path.basename(config_3par)
                    # current operation information string
                    info = f'[{i+1} of {configs_num}]: {configname} system'
                    print(info, end =" ")
                    showsys_lst, port_lst, host_lst = parse_config(config_3par, params, params_add, pattern_dct)
                    system_3par_comprehensive_lst.extend(showsys_lst)
                    port_3par_comprehensive_lst.extend(port_lst)
                    host_3par_comprehensive_lst.extend(host_lst)
                    if port_lst or host_lst:
                        meop.status_info('ok', max_title, len(info))
                    else:
                        meop.status_info('no data', max_title, len(info))
        else:
            # current operation information string
            info = f'Collecting 3PAR storage systems information'
            print(info, end =" ")
            meop.status_info('skip', max_title, len(info))
            
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'system_columns', 'port_columns', 'host_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, system_3par_comprehensive_lst, port_3par_comprehensive_lst, host_3par_comprehensive_lst)
        system_3par_df, port_3par_df, host_3par_df, *_ = data_lst        
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        system_3par_df, port_3par_df, host_3par_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return system_3par_df, port_3par_df, host_3par_df


def verify_ns_3par(nsshow_df, nscamshow_df, pattern_dct):
    """Function to verify if 3PAR storage systems present in fabrics by checking
    local and cached Name Server DataFrames"""

    storage_columns = ['System_Model', 'Serial_Number']
    ns_lst = [nsshow_df, nscamshow_df]
    ns_3par_lst = []

    for ns_df in ns_lst:
        ns_3par_df = ns_df.copy()
        # extract 3par records
        ns_3par_df[storage_columns] =  ns_3par_df['NodeSymb'].str.extract(pattern_dct['ns_3par'])
        ns_3par_df = ns_3par_df[storage_columns].copy()
        ns_3par_df.dropna(subset=['Serial_Number'], inplace=True)
        ns_3par_lst.append(ns_3par_df)
        
    ns_3par_df, nscam_3par_df = ns_3par_lst
    # concatenate local and cached NS 3PAR records
    ns_3par_df = pd.concat([ns_3par_df, nscam_3par_df])
    ns_3par_df.drop_duplicates(inplace=True)
    ns_3par_df.reset_index(drop=True, inplace=True)
    return ns_3par_df


def parse_config(config_3par, params, params_add, pattern_dct):
    """Function to parse 3PAR config file""" 

    # file name
    configname = os.path.basename(config_3par)
    # search control dictionary. continue to check file until all parameters groups are found
    collected = {'system': False, 'ip': False, 'port': False, 'host': False}

    # initialize structures to store collected data for current storage
    # dictionary to store all DISCOVERED parameters
    showsys_dct = {}
    showsys_lst = []
    # if lists remains empty after file parsing than status_info shows NO_DATA for current file
    port_lst = []
    host_lst = []
    # Storage IP address
    ip_addr = None
    
    with open(config_3par, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # showsys section start
            if re.search(pattern_dct['showsys_header'], line) and not collected['system']:
                collected['system'] = True
                # while not reach empty line
                while not re.search(pattern_dct['section_end'],line):
                    line = file.readline()
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # name_value_pair_match
                    if match_dct['parameter_value_pair']:
                        result = match_dct['parameter_value_pair']
                        showsys_dct[result.group(1).strip()] = result.group(2).strip()  
                    if not line:
                        break
            # showsys section end
            # port section start
            elif re.search(pattern_dct['showport_header'], line) and not collected['port']:
                collected['port'] = True
                while not re.search(pattern_dct['section_end'], line):
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # port_line_match
                    if match_dct['port_line']:
                        port_line = dsop.line_to_list(pattern_dct['port_line'], line, configname)
                        port_lst.append(port_line)
                    line = file.readline()
                    if not line:
                        break
            # port section end
            # host section start
            elif re.search(pattern_dct['showhost_header'], line) and not collected['host']:
                collected['host'] = True
                while not re.search(pattern_dct['section_end'], line):
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # port_line_match
                    if match_dct['host_line']:
                        host_line = dsop.line_to_list(pattern_dct['host_line'], line, configname)
                        host_lst.append(host_line)
                    line = file.readline()
                    if not line:
                        break
            # host section end
            # ip_address section start
            elif re.search(pattern_dct['ip_address'], line) and not collected['ip']:
                collected['ip'] = True
                ip_addr = re.search(pattern_dct['ip_address'], line).group(1)
            # ip_address section end

    # additional values which need to be added to the switch params dictionary 
    # switch_params_add order ('configname', 'chassis_name', 'switch_index', 'ls_mode')
    # values axtracted in manual mode. if change values order change keys order in init.xlsx switch tab "params_add" column
    showsys_values = (configname, ip_addr)

    if showsys_dct:
        # adding additional parameters and values to the parameters dct
        dsop.update_dct(params_add, showsys_values, showsys_dct)                                                
        # creating list with REQUIRED parameters for the current system.
        # if no value in the dct for the parameter then None is added 
        # and appending this list to the list of all systems     
        showsys_lst.append([showsys_dct.get(param) for param in params])

    return showsys_lst, port_lst, host_lst




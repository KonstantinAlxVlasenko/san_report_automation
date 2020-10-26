import os
import re

import pandas as pd

from common_operations_filesystem import (create_files_list, load_data,
                                          save_data)
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects

"""Module to extract blade system information"""


def blade_system_extract(blade_folder, report_data_lst):
    """Function to extract blade systems information"""
    

    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['blade_interconnect', 'blade_servers', 'blade_vc']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    module_comprehensive_lst, blades_comprehensive_lst, blade_vc_comprehensive_lst = data_lst

    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # print data which were loaded but for which force extract flag is on
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any of data_lst was not saved or 
    # force extract flag is on then re-extract data  from configueation files
    if not all(data_lst) or any(force_extract_keys_lst):

        # lists to store only REQUIRED infromation
        # collecting data for all blades during looping
        # list containing enclosure, blade and hba information for all blade systems
        blades_comprehensive_lst = []
        # list containing enclosure and interconnect modules information for all blade systems
        module_comprehensive_lst = []
        # list containing virtual connect ports information for all blade systems
        blade_vc_comprehensive_lst = []

        if blade_folder:    
            print('\nEXTRACTING BLADES SYSTEM INFORMATION ...\n')   
            
            # collects files in folder with txt extension
            txt_files = create_files_list(blade_folder, '.txt', max_title)
            log_files = create_files_list(blade_folder, '.log', max_title)
            blade_configs_lst = txt_files + log_files
            # number of files to check
            configs_num = len(blade_configs_lst)  

            if configs_num:

                # data imported from init file to extract values from config file
                enclosure_params, _, comp_keys, match_keys, comp_dct = data_extract_objects('blades', max_title)
                module_params = columns_import('blades', max_title, 'module_params')
                blade_params = columns_import('blades', max_title, 'blade_params')

                for i, blade_config in enumerate(blade_configs_lst):       
                    # file name with extension
                    configname_wext = os.path.basename(blade_config)
                    # remove extension from filename
                    configname, _ = os.path.splitext(configname_wext)
                    # Active Onboard Administrator IP address
                    oa_ip = None
                    # interconnect modules number
                    module_num = 0

                    # current operation information string
                    info = f'[{i+1} of {configs_num}]: {configname} system.'
                    print(info, end =" ")
                    
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
                            # enclosure section start
                            if re.search(r'>SHOW ENCLOSURE INFO|^ +ENCLOSURE INFORMATION$', line):
                                enclosure_dct = {}
                                collected['enclosure'] = True
                                # while not reach empty line
                                while not re.search(r'Serial Number',line):
                                    line = file.readline()
                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # name_value_pair_match
                                    if match_dct[match_keys[0]]:
                                        result = match_dct[match_keys[0]]
                                        enclosure_dct[result.group(1).strip()] = result.group(2).strip()      
                                    if not line:
                                        break
                                # rename Description key to Enclosure Type key for VC
                                if enclosure_dct.get('Description'):
                                    enclosure_dct['Enclosure Type'] = enclosure_dct.pop('Description')
                                # creating list with REQUIRED enclosure information only
                                enclosure_lst = [enclosure_dct.get(param) for param in enclosure_params]
                            # enclosure section end
                            # vc fabric connection section start
                            elif re.search(r'FABRIC INFORMATION', line):
                                info_type = 'Type VC'
                                print(info_type, end = " ")
                                info = info + " " + info_type
                                line = file.readline()
                                collected['vc'] = True
                                while not re.search(r'FC-CONNECTION INFORMATION', line):

                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # vc_port_match
                                    if match_dct[match_keys[14]]:
                                        vc_port = line_to_list(comp_dct[comp_keys[14]], line, *enclosure_lst)
                                        enclosure_vc_lst.append(vc_port)
                                        blade_vc_comprehensive_lst.append(vc_port)
                                        line = file.readline()
                                    else:
                                        line = file.readline()
                                        if not line:
                                            break

                            # vc fabric connection section end
                            # active onboard administrator ip section start
                            elif re.search(r'>SHOW TOPOLOGY *$', line):
                                info_type = 'Type Blade Enclosure'
                                print(info_type, end = " ")
                                info = info + " " + info_type
                                line = file.readline()
                                collected['oa_ip'] = True
                                while not re.search(r'^>SHOW', line):
                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # oa_ip_match
                                    if match_dct[match_keys[1]]:
                                        oa_ip = match_dct[match_keys[1]].group(1)
                                        line = file.readline()
                                        break
                                    else:
                                        line = file.readline()
                                        if not line:
                                            break
                            # active onboard administrator ip section end
                            # interconnect modules section start
                            elif re.search(r'>SHOW INTERCONNECT INFO ALL', line):
                                line = file.readline()
                                collected['modules'] = True
                                while not re.search(r'^>SHOW', line):
                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # module_type_num_match
                                    if match_dct[match_keys[2]]:
                                        module_dct = {}
                                        module_lst= []
                                        module = match_dct[match_keys[2]]
                                        # interconnect module slot number
                                        module_slot = module.group(1)
                                        # interconnect module type (Ethernet, FC)
                                        module_type = module.group(2).rstrip()
                                        line = file.readline()
                                        # module_section_end_comp
                                        while not re.search(comp_dct[comp_keys[3]], line):
                                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                            # name_value_pair_match
                                            if match_dct[match_keys[0]]:
                                                result = match_dct[match_keys[0]]
                                                name = result.group(1).strip()
                                                value = result.group(2).strip()
                                                # if value is empty string use None
                                                if value == '':
                                                    value = None
                                                module_dct[name] = value
                                                line = file.readline()
                                            else:
                                                line = file.readline()
                                                if not line:
                                                    break
                                        # creating list with REQUIRED interconnect module information only
                                        module_lst = [module_dct.get(param) for param in module_params]
                                        # add current module information to list containing all modules infromation
                                        # oa_ip added as None and extracted later in the file
                                        module_comprehensive_lst.append([*enclosure_lst, oa_ip, module_slot, module_type, *module_lst])
                                        # based on module's number oa_ip is added to module_comprehensive_lst after extraction
                                        module_num += 1
                                    else:
                                        line = file.readline()
                                        if not line:
                                            break
                            # interconnect modules section end
                            # blade server, hba and flb section start
                            elif re.search(r'>SHOW SERVER INFO ALL', line):
                                line = file.readline()
                                collected['servers'] = True
                                while not re.search(r'^>SHOW', line):
                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # blade_server_num_match
                                    if match_dct[match_keys[4]]:
                                        blade_dct = {}
                                        blade_lst = []
                                        hba_lst = []
                                        result = match_dct[match_keys[4]]
                                        blade_dct[result.group(1)] = result.group(2)
                                        # blade_num = result.group(2)
                                        # print("Blade number:", blade_num)
                                        line = file.readline()
                                        # server_section_end_comp
                                        while not re.search(comp_dct[comp_keys[11]], line):
                                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                            # mezzanin hba section start
                                            # mezzanine_model_match
                                            if match_dct[match_keys[6]]:
                                                result = match_dct[match_keys[6]]
                                                hba_description = result.group(1)
                                                hba_model = result.group(2)
                                                line = file.readline()
                                                # mezzanine_wwn_comp
                                                while re.search(comp_dct[comp_keys[7]], line):
                                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                                    # mezzanine_wwn_match
                                                    result = match_dct[match_keys[7]]
                                                    wwnp = result.group(1)
                                                    hba_lst.append([hba_description, hba_model, wwnp])
                                                    line = file.readline()
                                            # mezzanin hba section end
                                            # flex flb hba section start
                                            # flb_model_match and flex_ethernet_match
                                            elif match_dct[match_keys[8]] or match_dct[match_keys[15]]:
                                                if match_dct[match_keys[8]]:
                                                    result = match_dct[match_keys[8]]
                                                    flex_description = result.group(1)
                                                    flex_model = re.search(comp_dct[comp_keys[13]], line).group(1)
                                                elif match_dct[match_keys[15]]:
                                                    result = match_dct[match_keys[15]]
                                                    flex_description = result.group(1)
                                                    flex_model = result.group(1)
                                                line = file.readline()
                                                # wwn_mac_line_comp
                                                while re.search(comp_dct[comp_keys[9]], line):
                                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                                    # flb_wwn_match
                                                    if match_dct[match_keys[10]]:
                                                        result = match_dct[match_keys[10]]
                                                        wwnp = result.group(1)
                                                        hba_lst.append([flex_description, flex_model, wwnp])

                                                    line = file.readline()
                                            # flex flb hba section end
                                            # blade server section start
                                            # blade_server_info_match
                                            elif match_dct[match_keys[5]]:
                                                result = match_dct[match_keys[5]]
                                                # name = result.group(1) + result.group(2) if result.group(2) else result.group(1)
                                                name = result.group(1).rstrip()
                                                value = result.group(3).rstrip()
                                                # to avoid Type parameter overwrire
                                                # add parameter only if parameter has not been added to blade dictionary before
                                                if not blade_dct.get(name):
                                                    blade_dct[name] = value
                                                line = file.readline()
                                            # blade server section end
                                            # if none of matches found for current blade server than next line
                                            else:
                                                line = file.readline()
                                                if not line:
                                                    break
                                        # unpopulated blade slots have 'Server Blade Type' line but populated have 'Type' line
                                        # add 'Server Blade Type' parameter for populated slots for consistency
                                        if blade_dct.get('Type'):
                                            blade_dct['Server Blade Type'] = blade_dct.pop('Type')
                                        # creating list with REQUIRED blade server information only
                                        blade_lst = [blade_dct.get(param) for param in blade_params]
                                        # if hba or flex cards installed in blade server
                                        if len(hba_lst):
                                            # add line for each hba to blades_comprehensive_lst
                                            for hba in hba_lst:
                                                blades_comprehensive_lst.append([*enclosure_lst, *blade_lst, *hba])
                                        # if no nba add one line with enclosure and blade info only
                                        else:
                                            blades_comprehensive_lst.append([*enclosure_lst, *blade_lst, None, None])
                                    # if no blade_server_num_match found in >SHOW SERVER INFO ALL section than next line
                                    else:
                                        line = file.readline()
                                        if not line:
                                            break
                            # blade server, hba and flb section end
                        
                        # adding OA IP to module_comprehensive_lst based on interconnect modules number
                        for num in range(-1, -module_num-1, -1):
                            module_comprehensive_lst[num][3] = oa_ip
                        # show status blades information extraction from file
                        if blade_lst or enclosure_vc_lst:
                            status_info('ok', max_title, len(info))
                        else:
                            status_info('no data', max_title, len(info))
                # save extracted data to json file
                save_data(report_data_lst, data_names, module_comprehensive_lst, blades_comprehensive_lst, blade_vc_comprehensive_lst)
        else:
            # current operation information string
            info = f'Collecting enclosure, interconnect modules, blade servers, hba'
            print(info, end =" ")
            status_info('skip', max_title, len(info))
            # save empty data to json file
            save_data(report_data_lst, data_names, module_comprehensive_lst, blades_comprehensive_lst, blade_vc_comprehensive_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        module_comprehensive_lst, blades_comprehensive_lst, blade_vc_comprehensive_lst = verify_data(report_data_lst, data_names, *data_lst)
    
    return module_comprehensive_lst, blades_comprehensive_lst, blade_vc_comprehensive_lst

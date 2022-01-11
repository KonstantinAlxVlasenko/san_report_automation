"""Module to extract blade system information"""

import os
import re

import pandas as pd
import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop


def blade_system_extract(report_entry_sr, report_creation_info_lst):
    """Function to extract blade systems information"""

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    if pd.notna(report_entry_sr['blade_showall_folder']):
        blade_folder = os.path.normpath(report_entry_sr['blade_showall_folder'])
    else:
        blade_folder = None

    # names to save data obtained after current module execution
    data_names = ['blade_interconnect', 'blade_servers', 'blade_vc']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)

    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)
    
    if force_run: 
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
            txt_files = fsop.find_files(blade_folder, max_title, filename_extension='txt')
            log_files = fsop.find_files(blade_folder, max_title, filename_extension='log')
            noext_files = fsop.find_files(blade_folder, max_title, filename_extension=None)

            blade_configs_lst = txt_files + log_files + noext_files
            

            # number of files to check
            configs_num = len(blade_configs_lst)  

            if configs_num:

                # data imported from init file to extract values from config file
                
                # enclosure_params, _, comp_keys, match_keys, comp_dct = sfop.data_extract_objects('blades', max_title)
                # module_params = sfop.columns_import('blades', max_title, 'module_params')
                # blade_params = sfop.columns_import('blades', max_title, 'blade_params')

                pattern_dct, re_pattern_df = sfop.regex_pattern_import('blades', max_title)
                enclosure_params, module_params, blade_params = dfop.list_from_dataframe(re_pattern_df, 'enclosure_params', 'module_params', 'blade_params')

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
                            # blade enclosure section start
                            if re.search(r'>SHOW ENCLOSURE INFO', line):
                                enclosure_dct = {}
                                collected['enclosure'] = True
                                # while not reach empty line
                                while not re.search(r'Serial Number',line):
                                    line = file.readline()
                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                    # name_value_pair_match
                                    if match_dct['name_value_pair']:
                                        result = match_dct['name_value_pair']
                                        enclosure_dct[result.group(1).strip()] = result.group(2).strip()      
                                    if not line:
                                        break
                                # creating list with REQUIRED enclosure information only
                                enclosure_lst = [enclosure_dct.get(param) for param in enclosure_params]
                            # blade enclosure section end
                            # vc enclosure section start
                            elif re.search(r'^ +ENCLOSURE INFORMATION$', line):
                                line = file.readline()
                                enclosure_total_dct = {}
                                collected['enclosure'] = True
                                while not re.search(r'^ +.+?INFORMATION', line):
                                    line = file.readline()
                                    if re.match(r'ID +: +enc\d+', line):
                                        enclosure_current_dct = {}
                                        while not re.search(r'Part Number',line):
                                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                            # name_value_pair_match
                                            if match_dct['name_value_pair']:
                                                result = match_dct['name_value_pair']
                                                enclosure_current_dct[result.group(1).strip()] = result.group(2).strip()
                                            line = file.readline()      
                                            if not line:
                                                break
                                        # rename Description key to Enclosure Type key for VC
                                        if enclosure_current_dct.get('Description'):
                                            enclosure_current_dct['Enclosure Type'] = enclosure_current_dct.pop('Description')
                                        # creating list with REQUIRED enclosure information only
                                        enclosure_current_lst = [enclosure_current_dct.get(param) for param in enclosure_params]
                                        enslosure_id = enclosure_current_dct['ID']
                                        enclosure_total_dct[enslosure_id] = enclosure_current_lst
                            # vc enslosure section end
                            # vc fabric connection section start
                            elif re.search(r'FABRIC INFORMATION', line):
                                info_type = 'Type VC'
                                print(info_type, end = " ")
                                info = info + " " + info_type
                                line = file.readline()
                                collected['vc'] = True
                                while not re.search(r'FC-CONNECTION INFORMATION', line):

                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                    # vc_port_match
                                    if match_dct['vc_port']:

                                        # vc_port = dsop.line_to_list(pattern_dct['vc_port'], line, *enclosure_lst)
                                        vc_port = dsop.line_to_list(pattern_dct['vc_port'], line)
                                        enslosure_id = vc_port[0]
                                        enclosure_lst = enclosure_total_dct[enslosure_id]
                                        vc_port = [*enclosure_lst, *vc_port]

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
                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                    # oa_ip_match
                                    if match_dct['oa_ip']:
                                        oa_ip = match_dct['oa_ip'].group(1)
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
                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                    # module_type_num_match
                                    if match_dct['module_type_num']:
                                        module_dct = {}
                                        module_lst= []
                                        module = match_dct['module_type_num']
                                        # interconnect module slot number
                                        module_slot = module.group(1)
                                        # interconnect module type (Ethernet, FC)
                                        module_type = module.group(2).rstrip()
                                        line = file.readline()
                                        # module_section_end_comp
                                        while not re.search(pattern_dct['module_section_end'], line):
                                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                            # name_value_pair_match
                                            if match_dct['name_value_pair']:
                                                result = match_dct['name_value_pair']
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
                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                    # blade_server_num_match
                                    if match_dct['blade_server_num']:
                                        blade_dct = {}
                                        blade_lst = []
                                        hba_lst = []
                                        result = match_dct['blade_server_num']
                                        blade_dct[result.group(1)] = result.group(2)
                                        # blade_num = result.group(2)
                                        # print("Blade number:", blade_num)
                                        line = file.readline()
                                        # server_section_end_comp
                                        while not re.search(pattern_dct['server_section_end'], line):
                                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                            # mezzanin hba section start
                                            # mezzanine_model_match
                                            if match_dct['mezzanine_description']:
                                                result = match_dct['mezzanine_description']
                                                hba_description = result.group(1)
                                                hba_model = result.group(2)
                                                line = file.readline()
                                                # mezzanine_wwn_comp
                                                while re.search(pattern_dct['mezzanine_wwn'], line):
                                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                                    # mezzanine_wwn_match
                                                    result = match_dct['mezzanine_wwn']
                                                    wwnp = result.group(1)
                                                    hba_lst.append([hba_description, hba_model, wwnp])
                                                    line = file.readline()
                                            # mezzanin hba section end
                                            # flex flb hba section start
                                            # flb_model_match and flex_ethernet_match
                                            elif match_dct['flb_description'] or match_dct['flex_ethernet']:
                                                if match_dct['flb_description']:
                                                    result = match_dct['flb_description']
                                                    flex_description = result.group(1)
                                                    if re.search(pattern_dct['flb_model'], line):
                                                        flex_model = re.search(pattern_dct['flb_model'], line).group(1)
                                                    else:
                                                        flex_model = None
                                                elif match_dct['flex_ethernet']:
                                                    result = match_dct['flex_ethernet']
                                                    flex_description = result.group(1)
                                                    flex_model = result.group(1)
                                                line = file.readline()
                                                # wwn_mac_line_comp
                                                while re.search(pattern_dct['wwn_mac_line'], line):
                                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                                    # flb_wwn_match
                                                    if match_dct['flb_wwn']:
                                                        result = match_dct['flb_wwn']
                                                        wwnp = result.group(1)
                                                        hba_lst.append([flex_description, flex_model, wwnp])

                                                    line = file.readline()
                                            # flex flb hba section end
                                            # blade server section start
                                            # blade_server_info_match
                                            elif match_dct['blade_server_info']:
                                                result = match_dct['blade_server_info']
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
        data_lst = dfop.list_to_dataframe(headers_lst, module_comprehensive_lst, blades_comprehensive_lst, blade_vc_comprehensive_lst)
        blade_module_df, blade_servers_df, blade_vc_df, *_ = data_lst  

        # blade_module_df = dfop.list_to_dataframe(module_comprehensive_lst, max_title, sheet_title_import='blades')
        # blade_servers_df = dfop.list_to_dataframe(blades_comprehensive_lst, max_title, sheet_title_import='blades', columns_title_import='blade_columns')
        # blade_vc_df = dfop.list_to_dataframe(blade_vc_comprehensive_lst, max_title, sheet_title_import='blades', columns_title_import='blade_vc_columns')
        # # saving data to csv file
        # data_lst = [blade_module_df, blade_servers_df, blade_vc_df]
        
        # save_data(report_constant_lst, data_names, *data_lst)
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)  

    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        # module_comprehensive_lst, blades_comprehensive_lst, blade_vc_comprehensive_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        # blade_module_df, blade_servers_df, blade_vc_df = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        # data_lst = [blade_module_df, blade_servers_df, blade_vc_df]

        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        blade_module_df, blade_servers_df, blade_vc_df = data_lst
        
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)

    return blade_module_df, blade_servers_df, blade_vc_df

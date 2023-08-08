"""Module to extract 3PAR parameters from inserv.config file"""

import os
import re

import utilities.data_structure_operations as dsop
import utilities.regular_expression_operations as reop



def storage_params_extract(storage_config, san_extracted_oceanstor_dct, 
                           system_params, system_params_add, fcport_params, host_params, 
                           pattern_dct, info):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    # file name
    configname = os.path.basename(storage_config)
    # search control dictionary. continue to check file until all parameters groups are found
    collected = {'system': False, 'ip_addr': False, 'fcport': False, "host": False, "fcinitiator": False}
    
    # initialize structures to store collected data for the current storage
    # dictionary to store all DISCOVERED parameters
    system_summary_dct = {}
    system_summary_lst = []
    config_datetime = None
    
    # number of extracted controllers ip addresses
    mgmt_ip_addr_number = 0
    # extracted ip addresses
    mgmt_ip_addr_lst = []
    
    # number of extracted controller groups
    # controller group is a single controller or storage (depend from config)
    fcport_ctrl_group_number = 0
    # extracted fcports
    fcport_lst = []
    
    # extracted hosts (id, name, os, wwn)
    host_lst = []
    # extracted hosts id, name, os
    host_id_name_lst = []
    # extracted hosts id, wwn
    host_id_fcinitiator_lst = []
    
    # configname which current config is duplicat of
    duplicated_configname = None
    
    with open(storage_config, encoding='utf-8', errors='ignore') as file:
        line = file.readline()
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            # profile and summary section start
            if re.search(pattern_dct['storage_profile'], line) and not collected['system']:
                info_system = "System " + re.search(pattern_dct['storage_profile'], line).group(1)
                config_datetime = re.search(pattern_dct['storage_profile'], line).group(2)
                print(info_system, end = " ")
                info = info + " " + info_system
                collected['system'] = True
                line = reop.extract_key_value_from_line(system_summary_dct, pattern_dct, line, file, 
                                                extract_pattern_name='parameter_value_pair', 
                                                stop_pattern_name='license_header')
                controllers_number = int(system_summary_dct['Number of total controllers'])
                sn = system_summary_dct['Product Serial Number']
                # check if storage config was extracted before (config duplication)
                if not sn in san_extracted_oceanstor_dct:
                    configname_wo_ext, _ = os.path.splitext(configname)
                    san_extracted_oceanstor_dct[sn] = configname_wo_ext
                else:
                    duplicated_configname = san_extracted_oceanstor_dct[sn]
                    system_summary_dct = {}
                    break
            # mgmt ip address section start
            elif re.search(pattern_dct['mgmt_eth_id'], line):
                # each controller has mgmt ip address
                mgmt_ip_addr_number += 1
                if mgmt_ip_addr_number == controllers_number:
                    collected['ip_addr'] = True
                line = reop.extract_value_from_line(mgmt_ip_addr_lst, pattern_dct, 
                                            line, file, 
                                            extract_pattern_name='ip_addr', 
                                            stop_pattern_name='gateway_mac')
            # controllers fcport section start (fcports are grouped by storage or controllers)
            # fcports grouped by storage are located sequentially in single section
            # fcports gruped by controllers are located in different sections
            elif re.search(pattern_dct['storage_grouped_fcports_header'], line) \
                or re.search(pattern_dct['controller_grouped_fcports_header'], line):
                # check if all controllers is extracted (for conroller grouped fcports)
                if re.search(pattern_dct['controller_grouped_fcports_header'], line):
                    fcport_ctrl_group_number += 1
                    if fcport_ctrl_group_number == controllers_number:
                        collected['fcport'] = True
                else:
                    collected['fcport'] = True
                while not re.search(pattern_dct['sas_eth_port_header'], line):
                    line = file.readline()
                    if not line:
                        break
                    # single fcport section start
                    if re.search(pattern_dct['port_id'], line):
                        fcport_dct = {}
                        line = reop.extract_key_value_from_line(fcport_dct, pattern_dct, line, file, 
                                                        extract_pattern_name='parameter_value_pair', 
                                                        stop_pattern_name='blank_or_sfpinfo', first_line_skip=False)
                        if fcport_dct:       
                            fcport_lst.append([configname] + [fcport_dct.get(param) for param in fcport_params])
            # hosts section start
            elif re.search(pattern_dct['host_header'], line):
                collected['host'] = True
                while not re.search(pattern_dct['hostgroup_or_power_header'], line):
                    line = file.readline()
                    if not line:
                        break
                    # host id, name, os section start
                    if re.search(pattern_dct['host_id_name_line'], line):
                
                        line = reop.extract_list_from_line(host_id_name_lst, pattern_dct, 
                                                    line, file, 
                                                    extract_pattern_name= 'host_id_name_line', 
                                                    stop_pattern_name='blank_line', 
                                                    first_line_skip=False, line_add_values=configname)
                    # single host section start
                    elif re.search(pattern_dct['host_id_header'], line):
                        host_details_dct = {}
                        hostport_wwn_lst = []
                        host_details_lst = []
                        # extract host os, name, os
                        line = reop.extract_key_value_from_line(host_details_dct, pattern_dct, line, file, 
                                                        extract_pattern_name='parameter_value_pair', 
                                                        stop_pattern_name='host_mapping_header')
                        # extract host wwns
                        line = reop.extract_value_from_line(hostport_wwn_lst, pattern_dct, line, file, 
                                                        extract_pattern_name='hostport_wwn', 
                                                        stop_pattern_name='blank_line')
                        # add host details as list for each wwn
                        if host_details_dct:            
                            host_details_lst = [configname] + [host_details_dct.get(param) for param in host_params]
                            for wwn in hostport_wwn_lst:
                                host_lst.append([*host_details_lst, wwn])
            # hosts fcinitiator section start
            elif re.search(pattern_dct['fcinitiator_header'], line):
                collected['fcinitiator'] = True
                while not re.search(pattern_dct['sfp_header'], line):
                    line = file.readline()
                    if not line:
                        break
                    if re.search(pattern_dct['host_id_fcinitiator_line'], line):
                        line = reop.extract_list_from_line(host_id_fcinitiator_lst, pattern_dct, 
                                                    line, file, 
                                                    extract_pattern_name= 'host_id_fcinitiator_line', 
                                                    stop_pattern_name='blank_line', 
                                                    first_line_skip=False, line_add_values=configname)
            else:
                line = file.readline()
                if not line:
                    break

        if system_summary_dct:
            ip_addr = ', '.join(mgmt_ip_addr_lst) if mgmt_ip_addr_lst else None
            system_summary_values = (configname, ip_addr, config_datetime)
            # adding additional parameters and values to the parameters dct
            dsop.update_dct(system_params_add, system_summary_values, system_summary_dct)                                                
            # creating list with REQUIRED parameters for the current system.
            # if no value in the dct for the parameter then None is added 
            # and appending this list to the list of all systems             
            system_summary_lst.append([system_summary_dct.get(param) for param in system_params])
        
        storage_oceanstore_lst = [system_summary_lst, fcport_lst, host_lst, host_id_name_lst, host_id_fcinitiator_lst]
        return storage_oceanstore_lst, info, duplicated_configname
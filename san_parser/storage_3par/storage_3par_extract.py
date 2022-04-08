"""Module to extract 3PAR parameters from inserv.config file"""

import os
import re

import utilities.data_structure_operations as dsop
import utilities.regular_expression_operations as reop


def storage_params_extract(config_3par, system_params, system_params_add, pattern_dct):
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
                line = reop.key_value_extract(showsys_dct, pattern_dct, line, file, 
                                                extract_pattern_name='parameter_value_pair', 
                                                stop_pattern_name='section_end')
            # showsys section end
            # port section start
            elif re.search(pattern_dct['showport_header'], line) and not collected['port']:
                collected['port'] = True
                line = reop.lines_extract(port_lst, pattern_dct, configname, 
                                            line, file, 
                                            extract_pattern_name= 'port_line', 
                                            stop_pattern_name='section_end', 
                                            first_line_skip=False)
            # port section end
            # host section start
            elif re.search(pattern_dct['showhost_header'], line) and not collected['host']:
                collected['host'] = True
                line = reop.lines_extract(host_lst, pattern_dct, configname, 
                                            line, file, 
                                            extract_pattern_name= 'host_line', 
                                            stop_pattern_name='section_end', 
                                            first_line_skip=False)
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
        dsop.update_dct(system_params_add, showsys_values, showsys_dct)                                                
        # creating list with REQUIRED parameters for the current system.
        # if no value in the dct for the parameter then None is added 
        # and appending this list to the list of all systems     
        showsys_lst.append([showsys_dct.get(param) for param in system_params])
    return showsys_lst, port_lst, host_lst

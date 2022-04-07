"""Module with functions to extract portcfgshow and sfpshow information for the switch context from the sshow file"""


import re

import utilities.data_structure_operations as dsop


def sfpshow_section_extract(sfpshow_lst, pattern_dct, 
                            switch_info_lst, sfp_params, sfp_params_add, 
                            line, file):
    """Function to extract sfpshow information for the current context from the config file"""
    
    while not re.search(pattern_dct['switchcmd_end'],line):
        line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        if match_dct['slot_port']:
            # dictionary to store all DISCOVERED switch ports information
            # collecting data only for the logical switch in current loop
            sfpshow_dct = {}
            _, slot_num, port_num = dsop.line_to_list(pattern_dct['slot_port'], line)
            # if switch has no slots then all ports have slot 0
            slot_num = '0' if not slot_num else slot_num
            while not re.match('\r?\n', line):
                line = file.readline()
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # power_match
                if match_dct['power']:
                    sfp_power_lst = dsop.line_to_list(pattern_dct['power'], line)
                    # cut off RX or TX Power
                    sfp_power_value_unit = sfp_power_lst[1:]
                    for v, k in zip(sfp_power_value_unit[::2], sfp_power_value_unit[1::2]):
                        if k == 'uWatts':
                            k = 'uW'
                        key = sfp_power_lst[0] + '_' + k
                        sfpshow_dct[key] = v
                # transceiver_match
                elif match_dct['transceiver']:
                    sfpshow_dct[match_dct['transceiver'].group(1).rstrip()] = match_dct['transceiver'].group(2).rstrip()
                # no_sfp_match
                elif match_dct['no_sfp']:
                        sfpshow_dct['Vendor Name'] = 'No SFP module'
                # not_available_match
                elif match_dct['info_na']:
                        sfpshow_dct[match_dct['info_na'].group(1).rstrip()] = match_dct['info_na'].group(2).rstrip()
                # sfp_info_match
                elif match_dct['sfp_info']:
                    sfpshow_dct[match_dct['sfp_info'].group(1).rstrip()] = match_dct['sfp_info'].group(2).rstrip()                                        
                if not line:
                    break
                
            # additional values which need to be added to the dictionary with all DISCOVERED parameters during current loop iteration
            # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column                                   
            sfpshow_port_values = [*switch_info_lst, slot_num, port_num]                                       
            # adding additional parameters and values to the sfpshow_dct
            dsop.update_dct(sfp_params_add, sfpshow_port_values, sfpshow_dct)               
            # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
            sfpshow_lst.append([sfpshow_dct.get(param) for param in sfp_params])
    return line


def portcfgshow_section_extract(portcfgshow_dct, pattern_dct,
                                switch_info_lst, portcfg_params, 
                                line, file):
    """Function to extract portcfgshow information for the current context from the config file"""

    while not re.search(pattern_dct['switchcmd_portcfgshow_end'],line):
        line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # 'slot_port_line_match'
        if match_dct['slot_port_line']:
            # dictionary to store all DISCOVERED switch ports information
            portcfgshow_tmp_dct = {}
            # extract slot and port numbers
            slot_num, port_nums_str = dsop.line_to_list(pattern_dct['slot_port_line'], line)
            port_nums_lst = port_nums_str.split()
            port_nums = len(port_nums_lst)
            # list with switch and slot information
            switch_info_slot_lst = switch_info_lst.copy()
            switch_info_slot_lst.append(slot_num)
            # adding switch and slot information for each port to dictionary
            for portcfg_param, switch_info_value in zip(portcfg_params[:7], switch_info_slot_lst):
                portcfgshow_tmp_dct[portcfg_param] = [switch_info_value for i in range(port_nums)]
            # adding port numbers to dictionary    
            portcfgshow_tmp_dct[portcfg_params[7]] = port_nums_lst                                
            while not re.match('\r?\n', line):
                line = file.readline()
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # portcfg_match
                if match_dct['portcfg']:
                    # extract param name and values for each port and adding to dictionary
                    param_name, param_values_str = dsop.line_to_list(pattern_dct['portcfg'], line)
                    portcfgshow_tmp_dct[param_name] = param_values_str.split()
                if not line:
                    break
            # saving portcfg information of REQUIRED parameters from dictionary with DISCOVERED parameters
            for portcfg_param in portcfg_params:
                portcfgshow_dct[portcfg_param].extend(portcfgshow_tmp_dct.get(portcfg_param, [None for i in range(port_nums)]))
    return line 

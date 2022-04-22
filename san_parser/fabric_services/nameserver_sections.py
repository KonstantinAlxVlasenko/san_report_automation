
"""Module with functions to extract connected devices information (fdmishow, nsshow, nscamshow, nsportshow)"""


import re

import utilities.data_structure_operations as dsop
import utilities.regular_expression_operations as reop


def san_device_ports_section_extract(san_device_ports_lst, pattern_dct, line, file, 
                                        switch_info_lst, params, params_add,
                                        device_start_pattern_name, device_stop_pattern_name,
                                        cmd_stop_pattern_name='switchcmd_end'):
    """Function to extract NameServer information (nsshow, nscamshow) and 
    Fabric-Device Management Interface (FDMI) information for all Host Bus Adapters
    (HBAs) and ports (fdmishow) for the current context from the config file"""

    # current switch nsshow, nscamshow or fdmi information
    switch_device_ports_lst = []

    while not re.search(pattern_dct[cmd_stop_pattern_name], line):
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        if match_dct[device_start_pattern_name]:
            # dictionary to store current port information
            san_device_ports_dct = {}
            # switch_info and port_id (PID + WWN for nameserver and PID for fdmishow)
            port_ids_lst = dsop.line_to_list(pattern_dct[device_start_pattern_name], line, *switch_info_lst[:6])
            # move cursor to one line down to get inside while loop
            line = file.readline()                                
            line = reop.extract_key_value_from_line(san_device_ports_dct, pattern_dct, line, file, 
                                                    extract_pattern_name='fdmi_port', stop_pattern_name=device_stop_pattern_name, 
                                                    first_line_skip=False)
            # adding switch and port_id information to the port dictionary
            dsop.update_dct(params_add, port_ids_lst, san_device_ports_dct)               
            # appending list with only REQUIRED port info
            san_device_ports_lst.append([san_device_ports_dct.get(param) for param in params])
            switch_device_ports_lst.append([san_device_ports_dct.get(param) for param in params[6:]])
        else:
            line = file.readline()
        if not line:
            break
    return line, switch_device_ports_lst


def nsshow_file_extract(nsshow_file, san_nsshow_manual_lst, pattern_dct, nsshow_params, nsshow_params_add):
    """Function to extract NameServer information from dedicated text file"""               
    
    with open(nsshow_file, encoding='utf-8', errors='ignore') as file:
        line = file.readline()
        while line:
            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
            # port_pid_match
            if match_dct['port_pid']:
                # dictionary to store all DISCOVERED switch ports information
                nsshow_port_dct = {}
                # current connected device wwnp
                pid = dsop.line_to_list(pattern_dct['port_pid'], line)
                # move cursor to one line down to get inside while loop
                line = file.readline()                                
                line = reop.extract_key_value_from_line(nsshow_port_dct, pattern_dct, line, file, 
                                extract_pattern_name='fdmi_port', stop_pattern_name='port_pid', 
                                first_line_skip=False)
                # adding additional parameters and values to the fdmi_dct
                dsop.update_dct(nsshow_params_add[6:], pid, nsshow_port_dct)               
                # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                san_nsshow_manual_lst.append([nsshow_port_dct.get(nsshow_param) for nsshow_param in nsshow_params])
            else:
                line = file.readline()
            if not line:
                break
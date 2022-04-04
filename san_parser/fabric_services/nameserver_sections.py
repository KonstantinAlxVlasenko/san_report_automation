
"""Module with functions to extract connected devices information (fdmishow, nsshow, nscamshow, nsportshow)"""


import re

import utilities.data_structure_operations as dsop


def fdmi_section_extract(fdmi_lst, pattern_dct,
                         switch_info_lst, fdmi_params, fdmi_params_add,
                         line, file):
    """Function to extract fdmishow information for the current context from the config file"""

    while not re.search(pattern_dct['local_database'], line):
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # wwnp_match
        if match_dct['wwpn']:
            # dictionary to store all DISCOVERED switch ports information
            # collecting data only for the logical switch in current loop
            fdmi_dct = {}
            # switch_info and current connected device wwnp
            switch_wwnp = dsop.line_to_list(pattern_dct['wwpn'], line, *switch_info_lst[:6])
            # move cursor to one line down to get inside while loop
            line = file.readline()                                
            # wwnp_local_comp
            while not re.search(pattern_dct['wwpn_local'], line):
                line = file.readline()
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # fdmi_port_match
                if match_dct['fdmi_port']:
                    fdmi_dct[match_dct['fdmi_port'].group(1).rstrip()] = match_dct['fdmi_port'].group(2).rstrip()                                       
                if not line:
                    break
            # adding additional parameters and values to the fdmi_dct
            dsop.update_dct(fdmi_params_add, switch_wwnp, fdmi_dct)               
            # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
            fdmi_lst.append([fdmi_dct.get(param, None) for param in fdmi_params])
        else:
            line = file.readline()
        if not line:
            break
    return line


def nsshow_section_extract(ns_lst, pattern_dct,
                           switch_info_lst, nsshow_params, nsshow_params_add,
                           line, file):
    """Function to extract NameServer information (nsshow or nscamshow) 
    for the current context from the config file"""

    while not re.search(pattern_dct['switchcmd_end'], line):
        # line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # port_pid__match
        if match_dct['port_pid']:
            # dictionary to store all DISCOVERED switch ports information
            # collecting data only for the logical switch in current loop
            nsshow_port_dct = {}
            # switch_info and current connected device wwnp
            switch_pid = dsop.line_to_list(pattern_dct['port_pid'], line, *switch_info_lst[:6])
            # move cursor to one line down to get inside while loop
            line = file.readline()                                
            # pid_switchcmd_end_comp
            while not re.search(pattern_dct['pid_switchcmd_end'], line):
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # nsshow_port_match
                if match_dct['fdmi_port']:
                    nsshow_port_dct[match_dct['fdmi_port'].group(1).rstrip()] = match_dct['fdmi_port'].group(2).rstrip()
                line = file.readline()
                if not line:
                    break
                    
            # adding additional parameters and values to the dct
            dsop.update_dct(nsshow_params_add, switch_pid, nsshow_port_dct)               
            # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
            ns_lst.append([nsshow_port_dct.get(nsshow_param, None) for nsshow_param in nsshow_params])
        else:
            line = file.readline()
        if not line:
            break
    return line


def nsportshow_section_extract(nsportshow_lst, pattern_dct,
                                switch_info_lst,
                                line, file):
    """Function to extract zoning_enforcement information (HARD WWN,  HARD PORT, etc) 
    for the current context from the config file"""

    while not re.search(pattern_dct['switchcmd_end'], line):
        line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # ns_portshow_match
        if match_dct['ns_portshow']:
            port_enforcement = dsop.line_to_list(pattern_dct['ns_portshow'], line, *switch_info_lst[:6])
            nsportshow_lst.append(port_enforcement)
        if not line:
            break
    return line  


def nsshow_file_extract(nsshow_file, nsshow_manual_lst, pattern_dct, nsshow_params, nsshow_params_add):
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
                # pid_switchcmd_end_comp
                while not re.search(pattern_dct['port_pid'], line):
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # nsshow_port_match
                    if match_dct['fdmi_port']:
                        nsshow_port_dct[match_dct['fdmi_port'].group(1).rstrip()] = match_dct['fdmi_port'].group(2).rstrip()
                    line = file.readline()
                    if not line:
                        break
                # adding additional parameters and values to the fdmi_dct
                dsop.update_dct(nsshow_params_add[6:], pid, nsshow_port_dct)               
                # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                nsshow_manual_lst.append([nsshow_port_dct.get(nsshow_param) for nsshow_param in nsshow_params])
            else:
                line = file.readline()
"""Module with functions to extract islshow, trunkshow, porttrunkarea and lsdbshow information for the switch context from sshow file"""

import re

import utilities.data_structure_operations as dsop


def islshow_section_extract(isl_lst, pattern_dct, switch_info_lst, line, file):
    """Function to extract islshow information for the current context from the config file"""

    while not re.search(pattern_dct['switchcmd_end'], line):
        line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # islshow_match
        if match_dct['islshow']:
            isl_port = dsop.line_to_list(pattern_dct['islshow'], line, *switch_info_lst[:-1])
            # portcfg parameters
            if isl_port[-1]:
                isl_port[-1] = isl_port[-1].replace(' ', ', ')
            # appending list with only REQUIRED port info for the current loop iteration 
            # to the list with all ISL port info
            isl_lst.append(isl_port)
        if not line:
            break
    return line


def trunkshow_section_extract(trunk_lst, pattern_dct, switch_info_lst, line, file):
    """Function to extract trunkshow information for the current context from the config file"""

    while not re.search(pattern_dct['switchcmd_end'], line):                             
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # trunkshow_match
        if match_dct['trunkshow']:
            trunk_port = dsop.line_to_list(pattern_dct['trunkshow'], line, *switch_info_lst[:-1])
            # if trunk line has trunk number then remove ":" from trunk number
            if trunk_port[9]:
                trunk_port[9] = trunk_port[9].strip(':')
                trunk_num = trunk_port[9]
            # if trunk line has no number then use number from previous line
            else:
                trunk_port[9] = trunk_num
            # appending list with only REQUIRED trunk info for the current loop iteration 
            # to the list with all trunk port info
            trunk_lst.append(trunk_port)
        line = file.readline()
        if not line:
            break
    return line   


def porttrunkarea_section_extract(porttrunkarea_lst, pattern_dct, switch_info_lst, line, file):
    """Function to extract porttrunkarea information for the current context from the config file"""

    while not re.search(pattern_dct['switchcmd_end'], line):
        line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # 'porttrunkarea_match'
        if match_dct['porttrunkarea']:
            porttrunkarea_port_lst = dsop.line_to_list(pattern_dct['porttrunkarea'], line, *switch_info_lst[:6])
            # for No_light ports port and slot numbers are '--'
            if porttrunkarea_port_lst[11] == '--':
                porttrunkarea_port_lst[10] = '--'
            # if switch has no slots than slot number is 0
            for idx in [6, 10]:                                   
                if not porttrunkarea_port_lst[idx]:
                    porttrunkarea_port_lst[idx] = str(0)
            porttrunkarea_lst.append(porttrunkarea_port_lst)                                                       
        if not line:
            break
    return line    


def lsdbshow_section_extract(lsdb_lst, pattern_dct, switch_info_lst, lsdb_params, line, file):
    """Function to extract lsdbshow (link cost) information for the current context from the config file"""

    while not re.search(pattern_dct['switchcmd_end'],line):  
        line = file.readline()
        if not line:
            break
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # lsdb_domain section start
        if match_dct['lsdb_domain']:
            # dictionary to store all DISCOVERED parameters
            lsdb_param_dct = {}
            # Domain ID described by this LSR. 
            # A (self) keyword after the domain ID indicates that LSR describes the local switch.
            domain_self_tag_lst = dsop.line_to_list(pattern_dct['lsdb_domain'], line)
            # lsdb_link_comp
            while not (re.search(pattern_dct['lsdb_link'],line) or re.search(pattern_dct['switchcmd_end'],line)):
                line = file.readline()
                if not line:
                    break
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # param extraction
                if match_dct['lsdb_param']:
                    lsdb_name = match_dct['lsdb_param'].group(1).rstrip()
                    lsdb_value = match_dct['lsdb_param'].group(2).rstrip()
                    lsdb_param_dct[lsdb_name] = lsdb_value
            # list with required params only in order
            lsdb_param_lst = [lsdb_param_dct.get(param_name) for param_name in lsdb_params]
        # lsdb_domain section end
        if match_dct['lsdb_link']:
            # extract link information
            lsdb_link_lst = dsop.line_to_list(pattern_dct['lsdb_link'], line)
            # add link information to the global list with current switch and lsdb information 
            lsdb_lst.append([*switch_info_lst[:6], *domain_self_tag_lst,*lsdb_param_lst, *lsdb_link_lst])
    return line
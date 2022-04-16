"""Module to extract fabric routing information"""


import itertools
import re

import utilities.data_structure_operations as dsop
import utilities.regular_expression_operations as reop


def goto_baseswitch_context_fid(ls_mode_on, line, file, fid):
    """Function to move cursor to the fid context 
    within section of the corresponding command if Logical switch mode is ON"""

    if ls_mode_on:
        while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
            line = file.readline()
            if not line:
                break
    return line 


def fcrfabricshow_section_extract(fcrfabric_lst, pattern_dct, fcrouter_info_lst,
                                    line, file):
    """Function to extract fcrfabricshow information 
    for the base switch context fid from the sshow file"""

    while not re.search(pattern_dct['switchcmd_end'], line):
        line = file.readline()
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # fc_router_match'
        if match_dct['fc_router']:                                   
            fcrouter_params_lst = dsop.line_to_list(pattern_dct['fc_router'], line)
            # check if line is empty                                    
            while not re.match('\r?\n', line):
                line = file.readline()
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # fcr_info_match
                if match_dct['fcr_info']:
                    fcrouter_name = match_dct['fcr_info'].group(1)
                # fcr_exports_match                                        
                if match_dct['fcr_exports']:
                    fcrfabric_lst.append(dsop.line_to_list(pattern_dct['fcr_exports'], line, 
                                                        *fcrouter_info_lst, fcrouter_name, 
                                                        *fcrouter_params_lst))                                            
                if not line:
                    break                                      
        if not line:
            break
    return line


def lsanzoneshow_section_extract(lsan_lst, pattern_dct, fcrouter_info_lst,
                                    line, file):
    """Function to extract lsanzoneshow information 
    for the base switch context fid from the sshow file"""

    while not re.search(pattern_dct['switchcmd_end'], line):
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # lsan_name_match
        if match_dct['lsan_name']:
            # switch_info and current connected device wwnp
            lsan_name = dsop.line_to_list(pattern_dct['lsan_name'], line)
            # move cursor to one line down to get inside while loop
            line = file.readline()
            line = reop.extract_list_from_line(lsan_lst, pattern_dct, line, file, 
                                                extract_pattern_name='lsan_members', stop_pattern_name='lsan_switchcmd_end', 
                                                first_line_skip=False, line_add_values=[*fcrouter_info_lst, *lsan_name])
        else:
            line = file.readline()
        if not line:
            break  
    return line


def fcrresourceshow_section_extract(fcrresource_lst, pattern_dct,
                                    fcrouter_info_lst, fcrresource_params,
                                    line, file):
    """Function to extract fcrresourceshow information 
    for the base switch context fid from the sshow file"""

    fcrresource_current_lst = []
    line = reop.extract_list_from_line(fcrresource_current_lst, pattern_dct,  
                                        line, file, 
                                        extract_pattern_name='fcrresourceshow')
    fcrresource_dct = {fcrresource[0]: fcrresource[1:] for fcrresource in fcrresource_current_lst}

    # each value of dictionary is list of two elements
    # itertools.chain makes flat tmp_lst list from all lists in dictionary
    fcrresource_chain_lst = list(itertools.chain(*[fcrresource_dct.get(param) 
                                        if fcrresource_dct.get(param) else [None, None] for param in fcrresource_params]))
    fcrresource_lst.append([*fcrouter_info_lst, *fcrresource_chain_lst])
    return line

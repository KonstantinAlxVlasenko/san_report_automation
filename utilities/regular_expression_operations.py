import re

import utilities.data_structure_operations as dsop


def goto_switch_context(ls_mode_on, line, file, switch_index):
    """Function to move cursor to the switch_index context 
    within section of the corresponding command if Logical switch mode is ON"""

    if ls_mode_on:
        while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
            line = file.readline()
            if not line:
                break
    return line


def lines_extract(global_filled_lst, pattern_dct, extract_pattern_name, info_lst, 
                    line, file, stop_pattern_name='switchcmd_end', save_local=False):
    """Function to extract values from line in text file 
    using regular expression line_pattern_name from pattern_dct. 
    info_lst contains values which are added to each extracted line list.
    Total line list (info_lst + extracted line list) is added to the global_filled_lst.
    If required to save current function call result to local_filled_lst 
    then save_local parameter should be True"""

    if save_local:
        # list to store current function call result
        local_filled_lst = []

    while not re.search(pattern_dct[stop_pattern_name], line):
        line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # if matched line found
        if match_dct[extract_pattern_name]:
            extracted_line_lst = dsop.line_to_list(pattern_dct[extract_pattern_name], line, *info_lst)
            global_filled_lst.append(extracted_line_lst)
            if save_local:
                local_filled_lst.append(extracted_line_lst)                                            
        if not line:
            break
    return line if not save_local else line, local_filled_lst


def key_value_extract(global_filled_dct, pattern_dct, extract_pattern_name, 
                        line, file, stop_pattern_name='switchcmd_end', save_local=False):
    """Function to extract key, values pairs from line in text file 
    using regular expression extracted_pattern_name from pattern_dct. 
    Key, values are added to the global_filled_dct.
    If required to save current function call result to local_filled_dct 
    then save_local parameter should be True"""
    
    if save_local:
        # list to store current function call result
        local_filled_dct = []
    
    while not re.search(pattern_dct[stop_pattern_name],line):
        line = file.readline()
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # name_value_pair_match
        if match_dct[extract_pattern_name]:
            extracted_key_value = match_dct[extract_pattern_name]
            global_filled_dct[extracted_key_value.group(1).strip()] = extracted_key_value.group(2).strip()
            if save_local:
                local_filled_dct[extracted_key_value.group(1).strip()] = extracted_key_value.group(2).strip()
        if not line:
            break
    return line if not save_local else line, local_filled_dct

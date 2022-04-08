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


def lines_extract(global_filled_lst, pattern_dct, extracted_line_add_values, 
                    line, file, 
                    extract_pattern_name, stop_pattern_name='switchcmd_end', 
                    save_local=False, first_line_skip=True):
    """Function to extract values from line in text file 
    using regular expression line_pattern_name from pattern_dct. 
    extracted_line_add_values contains constant values which are added 
    to the each extracted line list (single value or list of values).
    Total line list (extracted_line_add_values + extracted line list) is added to the global_filled_lst.
    If first_line_skip is False line change performed after line is read.
    If required to save current function call result to local_filled_lst 
    then save_local parameter should be True"""

    if save_local:
        # list to store current function call result
        local_filled_lst = []

    while not re.search(pattern_dct[stop_pattern_name], line):
        if first_line_skip:
            line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # if matched line found
        if match_dct[extract_pattern_name]:
            if isinstance(extracted_line_add_values, (tuple, list)):
                extracted_line_lst = dsop.line_to_list(pattern_dct[extract_pattern_name], line, *extracted_line_add_values)
            else:
                extracted_line_lst = dsop.line_to_list(pattern_dct[extract_pattern_name], line, extracted_line_add_values)
            global_filled_lst.append(extracted_line_lst)
            if save_local:
                local_filled_lst.append(extracted_line_lst)                                            
        if not first_line_skip:
            line = file.readline()
        if not line:
            break
    return line, local_filled_lst if save_local else line


def key_value_extract(global_filled_dct, pattern_dct,  
                        line, file, 
                        extract_pattern_name, stop_pattern_name='switchcmd_end', 
                        save_local=False, first_line_skip=True):
    """Function to extract key, values pairs from line in text file 
    using regular expression extract_pattern_name from pattern_dct. 
    Key, values are added to the global_filled_dct.
    If first_line_skip is False line change performed after line is read. 
    If required to save current function call result to local_filled_dct 
    then save_local parameter should be True"""
    
    if save_local:
        # list to store current function call result
        local_filled_dct = []
    
    while not re.search(pattern_dct[stop_pattern_name],line):
        if first_line_skip:
            line = file.readline()
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # name_value_pair_match
        if match_dct[extract_pattern_name]:
            extracted_key_value = match_dct[extract_pattern_name]
            key = extracted_key_value.group(1).strip()
            value = extracted_key_value.group(2).strip()
            if not value:
                value = None
            global_filled_dct[key] = value
            if save_local:
                local_filled_dct[key] = value
        if not first_line_skip:
            line = file.readline()
        if not line:
            break
    return line, local_filled_dct if save_local else line

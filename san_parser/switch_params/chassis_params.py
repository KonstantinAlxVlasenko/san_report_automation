"""Module to extract chassis parameters"""


import re
from itertools import chain

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.regular_expression_operations as reop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop
from san_automation_constants import DIRECTOR_TYPE


def chassis_params_extract(all_config_data, project_constants_lst):
    """Function to extract chassis parameters"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'chassis_params_collection')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:
        print('\nEXTRACTING CHASSIS PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        # number of switches to check
        switch_num = len(all_config_data)    
        # nested list(s) to store required values of the module in defined order for all switches in SAN
        san_chassis_params_lst = []
        san_slot_status_lst = []
        san_licenseport_lst = []
        san_chassisshow_lst = []

        pattern_dct, re_pattern_df = sfop.regex_pattern_import('chassis', max_title)
        chassis_params,  chassis_params_add,  chassisshow_params = dfop.list_from_dataframe(
            re_pattern_df, 'chassis_params', 'chassis_params_add', 'chassisshow_params')

        
        # all_confg_data format ([swtch_name, supportshow file, (ams_maps_log files, ...)])
        # checking each config set(supportshow file) for chassis level parameters
        for i, switch_config_data in enumerate(all_config_data):
            
            switch_name, *_ = switch_config_data
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} chassis parameters'
            print(info, end =" ")
            ch_params_lst = current_config_extract(san_chassis_params_lst, san_slot_status_lst, san_licenseport_lst, san_chassisshow_lst, 
                                                   pattern_dct, switch_config_data, chassis_params, chassis_params_add, chassisshow_params)
            meop.show_collection_status(ch_params_lst, max_title, len(info))
        
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'chassis_columns', 'chassis_slot_columns', 'licenseport_columns', 'chassisshow_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, san_chassis_params_lst, san_slot_status_lst, san_licenseport_lst, san_chassisshow_lst)
        chassis_params_df, slot_status_df, licenseport_df, chassisshow_df, *_ = data_lst
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)     
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        chassis_params_df, slot_status_df, licenseport_df, chassisshow_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return chassis_params_df, slot_status_df, licenseport_df, chassisshow_df


def current_config_extract(san_chassis_params_lst, san_slot_status_lst, san_licenseport_lst, san_chassisshow_lst,
                           pattern_dct, switch_config_data, chassis_params, chassis_params_add, chassisshow_params):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    # data unpacking from iter param
    switch_name, sshow_file, ams_maps_file = switch_config_data

    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'configshow': False, 'uptime_cpu': False, 'flash': False, 'memory': False, 
                    'dhcp': False, 'licenses': False, 'licenseport': False, 'vf_id': False, 
                    'slotshow': False, 'chassisshow': False}
    
    # dictionary to store all DISCOVERED chassis parameters
    # collecting data only for the chassis in the current loop
    chassis_params_dct = {}
    # lists to store parameters
    snmp_target_lst = list()
    syslog_lst = list()
    license_lst = list()
    vf_id_lst = list()
    chassisshow_lst = list()
    uptime, cpu_load, memory, flash = ('not found',)*4

    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # configshow section start
            if re.search(pattern_dct['switchcmd_configshow'], line) and not collected['configshow']:
                collected['configshow'] = True
                line = configshow_section_extract(chassis_params_dct, snmp_target_lst, syslog_lst, 
                                                    pattern_dct, line, file)
            # uptime section start
            elif re.search(pattern_dct['switchcmd_uptime'], line):
                collected['uptime_cpu'] = True
                uptime_cpu_load_lst = []
                line = reop.extract_list_from_line(uptime_cpu_load_lst, pattern_dct, line, file, 
                                                    extract_pattern_name='uptime_cpu')
                # uptime_cpu_load_lst is nested list
                uptime, cpu_load, *_ = chain.from_iterable(uptime_cpu_load_lst)
                if not uptime:
                    uptime = '0'
            # uptime section end
            # memory section start
            elif re.search(pattern_dct['chassiscmd_meminfo'], line):
                collected['memory'] = True
                memory_dct = {}
                line = reop.extract_key_value_from_line(memory_dct, pattern_dct, line, file, 
                                                        extract_pattern_name='memory')
                # free_mem + buffers > 5% from total memory
                # memory usage < 95% 
                memory = round((1 - (int(memory_dct['MemFree']) + int(memory_dct['Buffers']))/int(memory_dct['MemTotal']))*100)
                memory = str(memory)
            # memory section end                                
            # flash section start
            elif re.search(pattern_dct['chassiscmd_flash'], line):
                collected['flash'] = True
                flash_lst = []
                line = reop.extract_value_from_line(flash_lst, pattern_dct, line, file, 
                                                    extract_pattern_name='flash')
                flash, *_ = flash_lst
            # flash section end
            # ipaddrshow section start
            elif re.search(pattern_dct['switchcmd_ipaddrshow'], line):
                collected['dhcp'] = True
                line = reop.extract_key_value_from_line(chassis_params_dct, pattern_dct, line, file, 
                                                        extract_pattern_name='dhcp')
            # ipaddrshow section end
            # licenses section start
            elif re.search(pattern_dct['chassiscmd_licenseshow'], line):
                collected['licenses'] = True
                line = reop.extract_value_from_line(license_lst, pattern_dct, line, file,
                                                    extract_pattern_name='license',
                                                    stop_pattern_name='switchcmd_end')
            # licenses section end
            # licenses fos9x section start
            elif re.search(pattern_dct['chassiscmd_license_fos9x'], line):
                collected['licenses'] = True
                license_fos9x_lst = []
                line = reop.extract_list_from_line(license_fos9x_lst, pattern_dct, 
                                                    line, file,
                                                    extract_pattern_name='license_fos9x',
                                                    stop_pattern_name='switchcmd_end')
                license_fos9x_lst = dsop.flatten(license_fos9x_lst)
                license_lst.extend([lic for lic in license_fos9x_lst if lic])
            # licenses fos9x section end
            # licenseport section start
            elif re.search(pattern_dct['chassiscmd_licenseport'], line) or re.search(pattern_dct['chassiscmd_licenseshowport'], line):
                collected['licenseport'] = True
                line = reop.extract_list_from_line(san_licenseport_lst, pattern_dct, 
                                                    line, file,
                                                    extract_pattern_name='licenseport',
                                                    stop_pattern_name='switchcmd_end', line_add_values=[sshow_file, switch_name])
            # licenseport section end
            # LS indexes identification start
            elif re.search(pattern_dct['section_fabric'], line):
                collected['vf_id'] = True
                line = reop.extract_value_from_line(vf_id_lst, pattern_dct, line, file,                 
                                                    extract_pattern_name='current_context', 
                                                    stop_pattern_name='switchcmd_dom')
            # LS indexes identification end
            # slot_status section start   
            elif re.search(pattern_dct['chassiscmd_slotshow'], line) and not collected['slotshow']:
                collected['slotshow'] = True
                line = reop.extract_list_from_line(san_slot_status_lst, pattern_dct,  
                                                    line, file, 
                                                    extract_pattern_name='slot_status', line_add_values=[sshow_file, switch_name])
            # slot_status section end
            # director control section start
            # if switch is not a director it doesn't contain chassiscmd_slotshow pattern
            # to avoid complete check of sshow file    
            elif re.search(pattern_dct['switch_type'], line):
                switch_type = re.match(pattern_dct['switch_type'], line).group(1).strip()
                switch_type = int(switch_type)
                if not switch_type in DIRECTOR_TYPE:
                    collected['slotshow'] = True
            # director control section end
            # chassishsow section start
            elif re.search(pattern_dct['sscmd_chassisshow'], line) and not collected['chassisshow']:
                collected['chassisshow'] = True
                while not re.search(pattern_dct['switchcmd_end'], line):
                    if re.search(pattern_dct['chassisshow_slot_unit'], line):
                        chassisshow_unit_dct = {}
                        slot_unit_lst = dsop.line_to_list(pattern_dct['chassisshow_slot_unit'], line)
                        line = file.readline()
                        line = reop.extract_key_value_from_line(chassisshow_unit_dct, pattern_dct, line, file, 
                                                        extract_pattern_name='param_value_pair', 
                                                        stop_pattern_name='sscmd_chassisshow_unit_end', first_line_skip=False)
                        if chassisshow_unit_dct:
                            chassisshow_unit_lst = [chassisshow_unit_dct.get(chassisshow_param) for chassisshow_param in chassisshow_params]
                            chassisshow_unit_lst = [sshow_file, switch_name] + slot_unit_lst + chassisshow_unit_lst
                            # append chassis serial
                            chassisshow_unit_lst.append(None)
                            chassisshow_lst.append(chassisshow_unit_lst)
                    elif re.search(pattern_dct['chassis_factory_serial'], line):
                        chassis_serial = re.search(pattern_dct['chassis_factory_serial'], line).group(1)
                        line = file.readline()
                        # add chassis serial to all units and slots (replace None)
                        for slot_unit_lst in chassisshow_lst:
                            slot_unit_lst[-1] = chassis_serial
                        
                    else:
                        line = file.readline()
                san_chassisshow_lst.extend(chassisshow_lst)
            # chassishsow section end                                          

    # list to show collection status
    ch_params_lst = [chassis_params_dct.get(chassis_param) for chassis_param in chassis_params]
    
    # remove duplicates from list
    vf_id_lst = sorted(set(vf_id_lst))
    snmp_target_lst = sorted(set(snmp_target_lst))
    syslog_lst = sorted(set(syslog_lst))
    # additional values which need to be added to the chassis params dictionary
    chassis_params_add_constants = (sshow_file, ams_maps_file, switch_name, vf_id_lst, snmp_target_lst, 
                                    syslog_lst, uptime, cpu_load, memory, flash, license_lst)
    # adding additional parameters and values to the chassis_params_switch_dct
    dsop.update_dct(chassis_params_add, chassis_params_add_constants, chassis_params_dct)                                                
    # creating list with REQUIRED chassis parameters for the current chassis.
    # if no value in the switch_params_dct for the parameter then None is added
    san_chassis_params_lst.append([chassis_params_dct.get(chassis_param) for chassis_param in chassis_params])
    return ch_params_lst


def configshow_section_extract(chassis_params_dct, snmp_target_lst, syslog_lst, 
                                pattern_dct, line, file):
    """Function to extract chassis parameters from configshow section"""

    while not re.search(pattern_dct['switchcmd_configshow_end'],line):
        line = file.readline()
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()} 
        # 'chassis_param_match' pattern #0
        if match_dct['chassis_param']:
            chassis_params_dct[match_dct['chassis_param'].group(1).rstrip()] = match_dct['chassis_param'].group(2).rstrip()                            
        # 'snmp_target_match' pattern #1
        if match_dct['snmp_target'] and match_dct['snmp_target'].group(2) != '0.0.0.0':
            snmp_target_lst.append(match_dct['snmp_target'].group(2))
        # 'syslog_match' pattern #2
        if match_dct['syslog']:
            syslog_lst.append(match_dct['syslog'].group(2))                                         
        if not line:
            break
    return line
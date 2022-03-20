"""Module to extract chassis parameters"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop


def chassis_params_extract(all_config_data, project_constants_lst):
    """Function to extract chassis parameters"""
    
    # # report_steps_dct contains current step desciption and force and export tags
    # report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # # report_constant_lst contains information: 
    # # customer_name, project directory, database directory, max_title
    # *_, max_title = report_constant_lst

    # report_steps_dct, max_title, data_dependency_df, *_ = project_constants_lst

    project_steps_df, max_title, data_dependency_df, *_ = project_constants_lst

    
    # names to save data obtained after current module execution
    data_names = ['chassis_parameters']
    # service step information
    # print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')

    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:
        print('\nEXTRACTING CHASSIS PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        # number of switches to check
        switch_num = len(all_config_data)    
        # list to store only REQUIRED chassis parameters
        # collecting data for all chassis during looping 
        chassis_params_fabric_lst = []

        pattern_dct, re_pattern_df = sfop.regex_pattern_import('chassis', max_title)
        chassis_params,  chassis_params_add = dfop.list_from_dataframe(re_pattern_df, 'chassis_params', 'chassis_params_add')
        
        # all_confg_data format ([swtch_name, supportshow file, (ams_maps_log files, ...)])
        # checking each config set(supportshow file) for chassis level parameters
        for i, switch_config_data in enumerate(all_config_data):
            
            switch_name, *_ = switch_config_data
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} chassis parameters'
            print(info, end =" ")
            chassis_params_lst = current_config_extract(chassis_params_fabric_lst, pattern_dct, 
                                                                switch_config_data, chassis_params, chassis_params_add)
            if dsop.list_is_empty(chassis_params_lst):
                meop.status_info('no data', max_title, len(info))
            else:
                meop.status_info('ok', max_title, len(info))

        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'chassis_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, chassis_params_fabric_lst)
        chassis_params_fabric_df, *_ = data_lst
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)     
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        chassis_params_fabric_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return chassis_params_fabric_df


def current_config_extract(chassis_params_fabric_lst, pattern_dct, 
                            switch_config_data, chassis_params, chassis_params_add):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    # data unpacking from iter param
    switch_name, sshow_file, ams_maps_file = switch_config_data
    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'configshow': False, 'uptime_cpu': False, 'flash': False, 'memory': False, 'dhcp': False, 'licenses': False, 'vf_id': False}
    # dictionary to store all DISCOVERED chassis parameters
    # collecting data only for the chassis in current loop
    chassis_params_dct = {}
    # sets and list to store parameters which could be joined in one group 
    snmp_target_set = set()
    syslog_set = set()
    tz_lst = []
    licenses = []
    vf_id_set = set()

    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # configshow section start
            if re.search(r'^(/fabos/cliexec/|/bin/cat /var/log/)?configshow *-?(all)? *:$', line):
                # when section is found corresponding collected dict values changed to True
                collected['configshow'] = True
                while not re.search(r'^(\[Chassis Configuration End\])|(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                    line = file.readline()
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    # match_dct_ ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # match_keys ['chassis_param_match', 'snmp_target_match', 'syslog_match', 'tz_match', 'uptime_cpu_match', 'memory_match', 'flash_match'] 
                    # 'chassis_param_match' pattern #0
                    if match_dct['chassis_param']:
                        chassis_params_dct[match_dct['chassis_param'].group(1).rstrip()] = match_dct['chassis_param'].group(3)                            
                    # for snmp and syslog data addresses are added to the coreesponding sets to avoid duplicates
                    # 'snmp_target_match' pattern #1
                    if match_dct['snmp_target'] and match_dct['snmp_target'].group(2) != '0.0.0.0':
                        snmp_target_set.add(match_dct['snmp_target'].group(2))
                    # 'syslog_match' pattern #2
                    if match_dct['syslog']:
                        syslog_set.add(match_dct['syslog'].group(2))
                    # for timezone extracted data added to the list for later concatenation
                    # 'tz_match' pattern #3
                    if match_dct['tz']:
                        tz_lst.append(match_dct['tz'].group(2))                                             
                    if not line:
                        break
            # config section end                
            # uptime section start
            elif re.search(r'^(/fabos/cliexec/)?uptime *:$', line):
                collected['uptime_cpu'] = True
                uptime = None
                cpu_load = None
                while not re.search(r'^real [\w.]+$',line):
                    line = file.readline()
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # 'uptime_cpu_match' pattern #4 
                    if match_dct['uptime_cpu']:
                        uptime = match_dct['uptime_cpu'].group(1)
                        if not uptime:
                            uptime = '0'
                        cpu_load = match_dct['uptime_cpu'].group(2)
                    if not line:
                        break
            # uptime section end
            # memory section start
            elif re.search(r'^(/bin/)?(cat\s+)?/proc/meminfo\s*:$', line):
                collected['memory'] = True
                memory_dct = {}
                while not re.search(r'^real [\w.]+$',line):
                    line = file.readline()
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # 'memory_match' pattern #5                    
                    if match_dct['memory']:
                        memory_dct[match_dct['memory'].group(1)] = match_dct['memory'].group(2)
                    if not line:
                        break
                # free_mem + buffers > 5% from total memory
                # memory usage < 95% 
                memory = round((1 - (int(memory_dct['MemFree']) + int(memory_dct['Buffers']))/int(memory_dct['MemTotal']))*100)
                memory = str(memory)
            # memory section end                                
            # flash section start
            elif re.search(r'^(/bin/)?df\s*:$', line):
                collected['flash'] = True
                flash = None
                while not re.search(r'^real [\w.]+$',line):
                    line = file.readline()
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # 'flash_match' pattern #6                    
                    if match_dct['flash']:
                        flash = match_dct['flash'].group(1)
                    if not line:
                        break
            # flash section end
            # ipaddrshow section start
            if re.search(r'^(/fabos/link_bin/)?ipaddrshow *:$', line):
                collected['dhcp'] = True
                while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                    line = file.readline()
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # 'dhcp_match' pattern #7
                    if match_dct['dhcp']:
                        chassis_params_dct[match_dct['dhcp'].group(1).rstrip()] = match_dct['dhcp'].group(2)                                                      
                    if not line:
                        break  
            # ipaddrshow section end
            # licenses section start
            if re.search(r'^(/fabos/cliexec/)?licenseshow *:$', line):
                collected['licenses'] = True
                while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                    line = file.readline()
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # 'licenses_match' pattern #8
                    if match_dct['licence']:
                        licenses.append(match_dct['licence'].group(1))
                    elif re.match('^No licenses installed.$', line):
                        licenses = 'No licenses installed'                                                                            
                    if not line:
                        break  
            # licenses section end
            # LS indexes identification start
            if re.search(r'Section *: +SSHOW_FABRIC', line):
                collected['vf_id'] = True
                while not re.search(r'^(SWITCHCMD /fabos/cliexec/)?dom *:$|Non-VF', line):       
                    if re.search(r'CURRENT +CONTEXT +-- +(\d+) *, \d+', line):
                        id = re.match(r'CURRENT +CONTEXT +-- +(\d+) *, \d+', line).group(1)
                        vf_id_set.add(id)
                        line = file.readline()
                    else:
                        line = file.readline()
                        if not line:
                            break
            # LS indexes identification end                                      
    # additional values which need to be added to the chassis params dictionary
    # chassis_params_add order (configname, ams_maps_log, chassis_name, snmp_server, syslog_server, timezone_h:m, uptime, cpu_average_load, memory_usage, flash_usage, licenses)
    # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column
    vf_id_lst = list(vf_id_set)
    vf_id_lst.sort()
    chassis_params_values = (sshow_file, ams_maps_file, switch_name, vf_id_lst, snmp_target_set, syslog_set, tz_lst, uptime, cpu_load, memory, flash, licenses)
    
    # adding additional parameters and values to the chassis_params_switch_dct
    for chassis_param_add, chassis_param_value in zip(chassis_params_add,  chassis_params_values):
        if chassis_param_value:                
            if not isinstance(chassis_param_value, str):
                s = ':' if chassis_param_add == 'timezone_h:m' else ', '
                chassis_param_value = f'{s}'.join(chassis_param_value)
            chassis_params_dct[chassis_param_add] = chassis_param_value
    # creating list with REQUIRED chassis parameters for the current switch
    # if no value in the chassis_params_dct for the parameter then None is added
    chassis_params_lst = [chassis_params_dct.get(chassis_param, None) for chassis_param in chassis_params]  
    # appending this list to the list of all switches chassis_params_fabric_lst
    chassis_params_fabric_lst.append(chassis_params_lst)
    return chassis_params_lst
"""Module to extract chassis parameters"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop


def chassis_params_extract(all_config_data, report_creation_info_lst):
    """Function to extract chassis parameters"""
    
    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['chassis_parameters']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)

    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)

    if force_run:
        print('\nEXTRACTING CHASSIS PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        # number of switches to check
        switch_num = len(all_config_data)    
        # list to store only REQUIRED chassis parameters
        # collecting data for all chassis during looping 
        chassis_params_fabric_lst = []
        # data imported from init file to extract values from config file
        chassis_params, chassis_params_add, comp_keys, match_keys, comp_dct = sfop.data_extract_objects('chassis', max_title)
        
        # all_confg_data format ([swtch_name, supportshow file, (ams_maps_log files, ...)])
        # checking each config set(supportshow file) for chassis level parameters
        for i, switch_config_data in enumerate(all_config_data):
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

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} chassis parameters'
            print(info, end =" ")
            
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
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # match_keys ['chassis_param_match', 'snmp_target_match', 'syslog_match', 'tz_match', 'uptime_cpu_match', 'memory_match', 'flash_match'] 
                            # 'chassis_param_match'
                            if match_dct[match_keys[0]]:
                                chassis_params_dct[match_dct[match_keys[0]].group(1).rstrip()] = match_dct[match_keys[0]].group(3)                            
                            # for snmp and syslog data addresses are added to the coreesponding sets to avoid duplicates
                            # 'snmp_target_match'
                            if match_dct[match_keys[1]] and match_dct[match_keys[1]].group(2) != '0.0.0.0':
                                snmp_target_set.add(match_dct[match_keys[1]].group(2))
                            # 'syslog_match'
                            if match_dct[match_keys[2]]:
                                syslog_set.add(match_dct[match_keys[2]].group(2))
                            # for timezone extracted data added to the list for later concatenation
                            # 'tz_match'
                            if match_dct[match_keys[3]]:
                                tz_lst.append(match_dct[match_keys[3]].group(2))                                             
                            if not line:
                                break
                    # config section end                
                    # uptime section start
                    elif re.search(r'^(/fabos/cliexec/)?uptime *:$', line):
                        collected['uptime_cpu'] = True
                        while not re.search(r'^real [\w.]+$',line):
                            line = file.readline()
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # 'uptime_cpu_match'   
                            if match_dct[match_keys[4]]:
                                uptime = match_dct[match_keys[4]].group(1)
                                cpu_load = match_dct[match_keys[4]].group(2)
                            if not line:
                                break
                    # uptime section end
                    # memory section start
                    elif re.search(r'^(/bin/)?(cat\s+)?/proc/meminfo\s*:$', line):
                        collected['memory'] = True
                        memory_dct = {}
                        while not re.search(r'^real [\w.]+$',line):
                            line = file.readline()
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # 'memory_match'                      
                            if match_dct[match_keys[5]]:
                                memory_dct[match_dct[match_keys[5]].group(1)] = match_dct[match_keys[5]].group(2)
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
                        while not re.search(r'^real [\w.]+$',line):
                            line = file.readline()
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # 'flash_match'                      
                            if match_dct[match_keys[6]]:
                                flash = match_dct[match_keys[6]].group(1)
                            if not line:
                                break
                    # flash section end
                    # ipaddrshow section start
                    if re.search(r'^(/fabos/link_bin/)?ipaddrshow *:$', line):
                        collected['dhcp'] = True
                        while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                            line = file.readline()
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # 'dhcp_match'
                            if match_dct[match_keys[7]]:
                                chassis_params_dct[match_dct[match_keys[7]].group(1).rstrip()] = match_dct[match_keys[7]].group(2)                                                      
                            if not line:
                                break  
                    # ipaddrshow section end
                    # licenses section start
                    if re.search(r'^(/fabos/cliexec/)?licenseshow *:$', line):
                        collected['licenses'] = True
                        while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                            line = file.readline()
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # 'licenses_match'
                            if match_dct[match_keys[8]]:
                                licenses.append(match_dct[match_keys[8]].group(1))
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
            # and appending this list to the list of all switches chassis_params_fabric_lst
            chassis_params_fabric_lst.append([chassis_params_dct.get(chassis_param, None) for chassis_param in chassis_params])
                            
            meop.status_info('ok', max_title, len(info))

        # convert list to DataFrame
        chassis_params_fabric_df = dfop.list_to_dataframe(chassis_params_fabric_lst, max_title, sheet_title_import='chassis')
        # saving data to csv file
        data_lst = [chassis_params_fabric_df]
        # save_data(report_constant_lst, data_names, *data_lst)
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)     

    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        # chassis_params_fabric_df = verify_data(report_constant_lst, data_names, *data_lst)
        # data_lst = [chassis_params_fabric_df]

        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        chassis_params_fabric_df, *_ = data_lst

    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)

    return chassis_params_fabric_df

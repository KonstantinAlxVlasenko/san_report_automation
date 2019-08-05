import re
import pandas as pd
from files_operations import columns_import, status_info

"""Module to extract chassis parameters"""


def chassis_params_extract(all_config_data, max_title):
        
    print('\n\nEXTRACTING CHASSIS PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...')
    # extract chassis parameters values from init file
    chassis_params = columns_import('parameters', 'chassis_params', max_title)
    chassis_params_keys  = columns_import('parameters', 'chassis_params_add', max_title)
    
    # number of switches to check
    switch_num = len(all_config_data)
    
    # list to store REQUIRED chassis parameters
    chassis_params_fabric_lst = []
    
    # all_confg_data format ([swtch_name, supportshow file, (ams_maps_log files, ...)])
    for i, switch_config_data in enumerate(all_config_data):
        # dictionary to store DISCOVERED chassis parameters
        chassis_params_switch_dct = {}
        # sets and list to store parameters which could be joined in one group 
        snmp_target_set = set()
        syslog_set = set()
        tz_lst = []

        switch_name, sshow_file, _ = switch_config_data
        info = f'[{i+1} of {switch_num}]: {switch_name} chassis parameters check'
        print(info, end =" ")
        with open(sshow_file, encoding='utf-8', errors='ignore') as file:
            # Number of LS = 4, system.i2cTurboCnfg:1
            chassis_param_comp = re.compile(r'^([\w .-]+) ?(=|:) ?([\w. :/]+)$')
            # snmp.snmpv3TrapTarget.0.trapTargetAddr:10.99.245.222
            snmp_target_comp = re.compile(r'^(snmp.snmpv3TrapTarget.\d.trapTargetAddr):([\d.]+)$')
            # syslog.address.1:10.99.116.66
            syslog_comp = re.compile(r'^(syslog.address.\d):([\d.]+)$')
            # ts.tzh:3
            tz_comp = re.compile(r'^(ts.tz[hm]):(\d+)$')
            line = file.readline()
            while not re.search(r'^\[Configuration upload Information\]$', line):
                line = file.readline()
                if not line:
                    break
            while not re.search(r'^\[Chassis Configuration End\]$',line):
                line = file.readline()
                chassis_param_match = chassis_param_comp.match(line)
                snmp_target_match = snmp_target_comp.match(line)
                syslog_match = syslog_comp.match(line)
                tz_match = tz_comp.match(line)
                if chassis_param_match:
                    chassis_params_switch_dct[chassis_param_match.group(1).rstrip()] = chassis_param_match.group(3)
                # for snmp and syslog data addresses are added to the coreesponding sets to avoid duplicates
                if snmp_target_match and snmp_target_match.group(2) != '0.0.0.0':
                    snmp_target_set.add(snmp_target_match.group(2))
                if syslog_match:
                    syslog_set.add(syslog_match.group(2))
                # for timezone extracted data added to the list for later concatenation
                if tz_match:
                    tz_lst.append(tz_match.group(2))                
                if not line:
                    break
        
        
        # chassis_params_keys order ('configname', 'switchname', 'snmp_server', 'syslog_server', 'timezone_h:m')
        chassis_params_values = (sshow_file, switch_name, snmp_target_set, syslog_set, tz_lst)
        
        # adding additional parameters and values to the chassis_params_switch_dct
        for chassis_param_key, chassis_param_value in zip(chassis_params_keys,  chassis_params_values):
            if chassis_param_value:                
                if not isinstance(chassis_param_value, str):
                    s = ':' if chassis_param_key == 'timezone_h:m' else ', '
                    chassis_param_value = f'{s}'.join(chassis_param_value)
                chassis_params_switch_dct[chassis_param_key] = chassis_param_value

        # creating list with REQUIRED chassis parameters and adding this list to the chassis_params_fabric_lst
        chassis_params_fabric_lst.append([chassis_params_switch_dct.get(chassis_param, None) for chassis_param in chassis_params])
            
        status_info('ok', max_title, len(info))
        
    return chassis_params_fabric_lst
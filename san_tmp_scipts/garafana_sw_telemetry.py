# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 18:27:51 2023

@author: kavlasenko
"""

from prometheus_client import start_http_server, Gauge
from datetime import datetime, date
import time
import os
import re
from copy import copy, deepcopy

script_dir = r'E:\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
# from switch_telemetry_cls import BrocadeSwitchTelemetry
from switch_telemetry_httpx_cls import BrocadeSwitchTelemetry
# from switch_telemetry_httpx_async_cls import BrocadeSwitchTelemetry
# from switch_telemetry_parser_cls import BrocadeChassisParser
from switch_telemetry_chassis_parser_cls import BrocadeChassisParser

from switch_telemetry_sensor_parser_cls import BrocadeFRUParser
from switch_telemetry_maps_parser_cls import BrocadeMAPSParser
from switch_telemetry_switch_parser_cls import BrocadeSwitchParser
from switch_telemetry_heartbeat_cls import BrocadeRequestStatus
    
    
    
def get_error_message(sw_telemetry_dct: dict):
    if 'errors' in sw_telemetry_dct:
        errors_lst = sw_telemetry_dct['errors']['error']
        errors_msg_lst = [error.get('error-message') for error in errors_lst if error.get('error-message')]
        return ', '.join(errors_msg_lst)
    else:
        return None
    


print('\n')    
st = time.time()
san03_nord = BrocadeSwitchTelemetry(sw_ipaddress='10.202.18.120')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


print('\n')  
st = time.time()
san23_ost = BrocadeSwitchTelemetry(sw_ipaddress='10.221.5.178')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


print('\n')  
st = time.time()
san49_nord = BrocadeSwitchTelemetry(sw_ipaddress='10.213.16.22')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))



print('\n')  
st = time.time()
san47_ost = BrocadeSwitchTelemetry(sw_ipaddress='10.213.16.20')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))



print('\n')  
st = time.time()
o3_g630_003_vc01_f1 = BrocadeSwitchTelemetry(sw_ipaddress='10.231.4.103')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
print(o3_g630_003_vc01_f1)


print('\n')  
st = time.time()
n3_g620_005_vc5_f1 = BrocadeSwitchTelemetry(sw_ipaddress='10.213.16.50', secure_login=True)
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


print('\n')  
st = time.time()
ost_6510_07_f1 = BrocadeSwitchTelemetry(sw_ipaddress='10.221.5.200')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


st = time.time()
o3_g620_107_vc01_f1 = BrocadeSwitchTelemetry(sw_ipaddress='10.231.4.100', secure_login=True)
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))



print('\n')  
st = time.time()
o1_g620_003_vc5_f1 = BrocadeSwitchTelemetry(sw_ipaddress='10.213.16.90', secure_login=False)
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


get_error_message(san23_ost.dashboard_rule)
get_error_message(san03_nord.fdmi_hba)


severity_level = {'OK': 0, 
                  'WARNING': 1,  
                  'CRITICAL': 2}


gauge_ch_name = Gauge('chassis_name', 'Chassis name', ['chassis_name'])
gauge_sw_sn = Gauge('switch_sn', 'Switch serial number', ['switch_sn'])
gauge_sw_datetime = Gauge('switch_datetime', 'Swicth datetime', ['switch_datetime'])
gauge_sw_model = Gauge('switch_model', 'Switch model', ['switch_model'])
gauge_vf_mode = Gauge('vf_mode', 'Virtual Fabrics', ['chassis_name', 'vfmode'])
gauge_tz = Gauge('switch_tz', 'Time zone', ['switch_tz'])

gauge_ps_state = Gauge('ps_status', 'The operational state of the power supply', ['ps_id'])
gauge_ps_severity = Gauge('ps_severity', 'Severity level to a specified value of the power supply', ['ps_id'])

gauge_fan_speed = Gauge('fan_status', 'The current operational state of the fan', ['fan_id'])
gauge_fan_state = Gauge('fan_speed', 'The fan speed in RPM', ['fan_id'])
gauge_fan_severity = Gauge('fan_severity', 'The severity level to a specified value of the fan', ['fan_id'])

gauge_maps_policy = Gauge('maps_policy', 'Active MAPS policy', ['maps_policy'])
gauge_maps_actions = Gauge('maps_actions', 'MAPS actions', ['maps_actions'])
gauge_db_rule = Gauge('dashboard_rule', 'Triggered rules list for the last 7 days', ['category', 'name', 'triggered_count', 'time_stamp', 'repetition_count', 'element', 'value'])

gauge_cpu_usage = Gauge('cpu_usage', 'The percentage of CPU usage', ['cpu_usage'])
gauge_memory_usage = Gauge('memory_usage', 'The percentage of memory usage', ['memory_usage'])
gauge_total_memory_usage = Gauge('total_memory_usage', 'The total memory usage in kilobytes', ['total_memory_usage'])
gauge_flash_usage = Gauge('flash_usage', 'The percentage of flash usage', ['flash_usage'])

gauge_ssp_state = Gauge('ssp_state', 'Switch Status Policy report', ['ssp_state_type'])
gauge_license = Gauge('licenses', 'Licenses installed on the switch', ['license_feature'])


# sw_telemetry = ost_6510_07_f1
# sw_telemetry = copy(o3_g630_003_vc01_f1)
# sw_telemetry = copy(san03_nord)
# sw_telemetry = copy(n3_g620_005_vc5_f1)
# sw_telemetry = o3_g620_107_vc01_f1
sw_telemetry = san49_nord


ch_parser = BrocadeChassisParser(sw_telemetry)
fru_parser = BrocadeFRUParser(sw_telemetry)
maps_config = BrocadeMAPSParser(sw_telemetry)
maps_config2 = BrocadeMAPSParser(sw_telemetry)
sw_parser = BrocadeSwitchParser(sw_telemetry)
heartbeat = BrocadeRequestStatus(sw_telemetry)

heartbeat.request_status


ch_parser.ch_name
ch_parser.sw_license
ch_parser
ch_parser



current_chassis = sw_telemetry.chassis['Response']['chassis']
ch_name = current_chassis['chassis-user-friendly-name']
if current_chassis['vendor-serial-number']:
    sw_sn = current_chassis['vendor-serial-number']
else:
    switch_sn = current_chassis['serial-number']
sw_model = 'Brocade ' + current_chassis['product-name'].capitalize()
sw_datetime = current_chassis['date']
ch_wwn = current_chassis['chassis-wwn']


# virtual fabrics
vf_enabled = current_chassis['vf-enabled']
vf_mode_status = ['Enabled', 'Disabled']
if vf_enabled:
    vf_mode = 'Enabled'
    vf_status = 1
else:
    vf_mode = 'Disabled'
    vf_status = 0


# licenses
# license_lst = san03_nord.sw_license['Response']['license']
license_feature_lst = []
for lic in sw_telemetry.sw_license['Response']['license']:
    lic_feature = lic['features']['feature']
    
    if lic.get('expiration-date'):
        exp_date = datetime.strptime(lic['expiration-date'], '%m/%d/%Y').date()
        if date.today() > exp_date:
            gauge_license.labels(lic_feature).set(2)   
        else:
            gauge_license.labels(lic_feature).set(1)
    else:        
        gauge_license.labels(lic_feature).set(0)
        license_feature_lst.extend(lic_feature)





    
# find time-zone
if san03_nord.time_zone['Response']['time-zone'].get('name'):
    sw_tz = san03_nord.time_zone['Response']['time-zone'].get('name')
elif san03_nord.time_zone['Response']['time-zone'].get('gmt-offset-hours') and \
    san03_nord.time_zone['Response']['time-zone'].get('gmt-offset-minutes'):
        sw_tz = str(san03_nord.time_zone['Response']['time-zone'].get('gmt-offset-hours')) \
            + ':' + str(san03_nord.time_zone['Response']['time-zone'].get('gmt-offset-minutes'))
else:
    sw_tz = 'unknown'
    
# sensor
# power supply
ps_state = {'absent': 0,
            'ok': 1,
            'faulty': 2, 
            'predicting failure': 3, 
            'unknown': 4,
            'try reseating unit': 5}


def swap_dict(dct):
    return {y: x for x, y in dct.items()}

swap_dict(ps_state)

for ps in san03_nord.ps['Response']['power-supply']:
    print(ps['unit-number'], ps['operational-state'])
    ps_id = 'Power Supply #' + str(ps['unit-number'])
    if ps['operational-state'].lower() == 'ok':
        ps_severity = severity_level['OK']
    elif ps['operational-state'].lower() == 'faulty':
        ps_severity = severity_level['FAIL']
    else:
        ps_severity = severity_level['WARNING']
        
    gauge_ps_state.labels(ps_id).set(ps_state[ps['operational-state']])
    gauge_ps_severity.labels(ps_id).set(ps_severity)


# fan
fan_state = {'absent': 0, 
             'ok': 1, 
             'faulty': 2, 
             'below minimum': 3, 
             'above maximum': 4, 
             'unknown': 5, 
             'not ok': 6}

for fan in san03_nord.fan['Response']['fan']:
    print(fan['unit-number'], fan['speed'], fan['operational-state'])
    fan_id = 'Fan #' + str(fan['unit-number'])
    if int(fan['speed']) >= 14000:
        fan_severity = severity_level['WARNING']
    elif fan['operational-state'].lower() == 'ok':
        fan_severity = severity_level['OK']
    elif fan['operational-state'].lower() == 'faulty':
        fan_severity = severity_level['FAIL']
    else:
        fan_severity = severity_level['WARNING']
    
    
    
    gauge_fan_speed.labels(fan_id).set(fan['speed'])
    gauge_fan_state.labels(fan_id).set(fan_state[fan['operational-state']])
    gauge_fan_severity.labels(fan_id).set(fan_severity)
    


# maps policy, maps actions
for policy in san03_nord.maps_policy['Response']['maps-policy']:
    if policy['is-active-policy']:
        maps_policy = policy['name']
maps_actions = san03_nord.maps_config['Response']['maps-config']['actions']['action']
maps_actions = ', '.join(maps_actions)

gauge_maps_policy.labels(maps_policy).set(1)
gauge_maps_actions.labels(maps_actions).set(1)


# switch_status policy report
ssp_state_dct = san03_nord.ssp_report['Response']['switch-status-policy-report']
ssp_state_type_lst = ['switch-health', 
                                  'power-supply-health', 'fan-health', 'temperature-sensor-health', 
                                  'flash-health', 
                                  'marginal-port-health', 'faulty-port-health', 'missing-sfp-health', 'error-port-health', 
                                  'expired-certificate-health', 'airflow-mismatch-health']

ssp_state_value_dct = {'healthy': 0,  'unknown': 1, 'down': 2, 'marginal': 3}

for ssp_state_type in ssp_state_type_lst:
    ssp_state = ssp_state_dct[ssp_state_type]
    gauge_ssp_state.labels(ssp_state_type).set(ssp_state_value_dct[ssp_state])



# maps system resources
RESOURCE_WARNING_THRESHOLD = 90
RESOURCE_CRITICAL_THRESHOLD = 95

system_resource_dct = san03_nord.system_resources['Response']['system-resources']
resource_gauge_dct = {
    'cpu-usage': gauge_cpu_usage,
    'flash-usage': gauge_flash_usage,
    'memory-usage': gauge_memory_usage,
    'total-memory': gauge_flash_usage
    }

for resource, value in system_resource_dct.items():
    resource_gauge_dct[resource].labels(resource).set(value)
    

# dashboard rule
db_rule_container_lst = o3_g630_003_vc01_f1.dashboard_rule['Response']['dashboard-rule']

# db_rule_leaf_lst = ['category', 'name', 'triggered-count', 'time-stamp', 'repetition-count', 'objects']
db_rule_leaf_names = ['category', 'name', 'triggered-count', 'time-stamp', 'repetition-count']



for db_rule_container in db_rule_container_lst:
    db_rule_container_values = [db_rule_container[leaf_name] for leaf_name in db_rule_leaf_names]
    for object_leaf_value in db_rule_container['objects'].values():
        for element_value in object_leaf_value:
            current_element_container_values =  db_rule_container_values + element_value.split(':')
            # element, value = element_value.split(':')
            gauge_db_rule.labels(*current_element_container_values).set(1)
            print(current_element_container_values)
            
    # print(db_rule_violation.keys())

# # switch info
# fc_sw_key_lst = ['is-enabled-state', 'up-time', 'model', 'vf-id', 'fabric-user-friendly-name']
# fabric_sw_key_lst = ['domain-id', 'ip-address', 'principal', 'switch-user-friendly-name', 'path-count']
# fc_logical_sw_key_lst = ['base-switch-enabled',  'default-switch-status',  'fabric-id', 'logical-isl-enabled', 'port-member-list']

# switch_dct = {}


# fabric_sw_container_lst = sw_telemetry.fabric_switch['Response']['fabric-switch']
# if not get_error_message(sw_telemetry.fc_logical_switch):
#     fc_logical_sw_container_lst = sw_telemetry.fc_logical_switch['Response']['fibrechannel-logical-switch']
# else:
#     fc_logical_sw_container_lst = []

# for fabric_sw in fabric_sw_container_lst:
#     # print(fabric_sw)
#     print(fabric_sw['chassis-wwn'], ch_wwn)
#     if fabric_sw['chassis-wwn'] != ch_wwn:
#         continue
#     fos_version = fabric_sw['firmware-version']
#     sw_wwn = fabric_sw['name']
#     current_sw_dct = {key: fabric_sw[key] for key in fabric_sw_key_lst}
#     for fc_sw in sw_telemetry.fc_switch['Response']['fibrechannel-switch']:
#         if sw_wwn == fc_sw['name']:
#             sw_type = fc_sw['model']
#             current_fc_sw_vfid = fc_sw['vf-id']
#             current_fc_sw_dct = {key: fc_sw[key] for key in fc_sw_key_lst}
#             current_sw_dct.update(current_fc_sw_dct)
#     if not sw_telemetry.fc_logical_switch.get('error'):
#         fc_logical_sw_num = 0
#         for fc_logical_sw in fc_logical_sw_container_lst:
#             fc_logical_sw_num += 1
#             if sw_wwn == fc_logical_sw['switch-wwn']:
#                 current_logical_sw_dct = {key: fc_logical_sw[key] for key in fc_logical_sw_key_lst}
#                 current_logical_sw_dct['port-member-quantity'] = len(fc_logical_sw['port-member-list']['port-member'])
#                 current_sw_dct.update(current_logical_sw_dct)
#     if current_fc_sw_vfid == -1 and not sw_telemetry.fc_interface.get('error'):
#         current_sw_dct['port-member-quantity'] = len(sw_telemetry.fc_interface['Response']['fibrechannel'])
#     switch_dct[sw_wwn] = current_sw_dct



# switch info
fc_sw_key_lst = ['name', 'domain-id', 'user-friendly-name', 'is-enabled-state', 'up-time', 'principal', 'ip-address', 'model', 'firmware-version', 'vf-id', 'fabric-user-friendly-name', 'ag-mode']
fc_logical_sw_key_lst = ['base-switch-enabled',  'default-switch-status',  'fabric-id', 'logical-isl-enabled', 'port-member-list']

switch_dct = {}

fc_sw_container_lst = sw_telemetry.fc_switch['Response']['fibrechannel-switch']

if not get_error_message(sw_telemetry.fc_logical_switch):
    fc_logical_sw_container_lst = sw_telemetry.fc_logical_switch['Response']['fibrechannel-logical-switch']
else:
    fc_logical_sw_container_lst = []

for fc_sw in fc_sw_container_lst:
    fos_version = fc_sw['firmware-version']
    sw_wwn = fc_sw['name']
    current_sw_dct = {key: fc_sw[key] for key in fc_sw_key_lst}
    if not sw_telemetry.fc_logical_switch.get('error'):
        fc_logical_sw_num = 0
        for fc_logical_sw in fc_logical_sw_container_lst:
            fc_logical_sw_num += 1
            if sw_wwn == fc_logical_sw['switch-wwn']:
                current_logical_sw_dct = {key: fc_logical_sw[key] for key in fc_logical_sw_key_lst}
                current_logical_sw_dct['port-member-quantity'] = len(fc_logical_sw['port-member-list']['port-member'])
                current_sw_dct.update(current_logical_sw_dct)
    if fc_sw['vf-id'] == -1 and not sw_telemetry.fc_interface.get('error'):
        current_sw_dct['port-member-quantity'] = len(sw_telemetry.fc_interface['Response']['fibrechannel'])
    switch_dct[sw_wwn] = current_sw_dct



    

# create dictonary with port name as key and vf-id as value
sw_port_vfid_dct = {}
for fc_sw in switch_dct.values():
    if fc_sw.get('port-member-list'):
        vf_id = fc_sw['vf-id']
        sw_name = fc_sw['user-friendly-name']
        port_member_lst = fc_sw['port-member-list']['port-member']
        print(port_member_lst)
        current_sw_port_vfid_dct = {port: [sw_name, vf_id] for port in port_member_lst}
        sw_port_vfid_dct.update(current_sw_port_vfid_dct)



# fc_interface
port_type_dct = {0: 'Unknown',
                 7: 'E-Port',
                 10: 'G-Port',
                 11: 'U-Port',
                 15: 'F-Port',
                 16: 'L-Port',
                 17: 'FCoE Port',
                 19: 'EX-Port',
                 20: 'D-Port',
                 21: 'SIM Port',
                 22: 'AF-Port',
                 23: 'AE-Port',
                 25: 'VE-Port',
                 26: 'Ethernet Flex Port',
                 29: 'Flex Port',
                 30: 'N-Port',
                 32768: 'LB-Port'}



fc_interface_container_lst = sw_telemetry.fc_interface['Response']['fibrechannel']
port_status_lst = []
port_name_dct = {}
port_status_fields = ['name', 'user-friendly-name', 'fcid-hex', 'speed', 'auto-negotiate', 
                      'physical-state', 'port-type', 'neighbor', 'is-enabled-state']
# print(fc_interface_container_lst[0])
for fc_interface_container in fc_interface_container_lst:
    
    slot_port_number = fc_interface_container['name']
    slot_number, port_number = slot_port_number.split('/')
    port_fcid = re.search('0x(.+)', fc_interface_container['fcid-hex']).group(1)
    port_speed = int(fc_interface_container['speed']/1000_000_000)
    if fc_interface_container['auto-negotiate']:
        port_speed = 'N' + str(port_speed)
    else:
        port_speed = str(port_speed) + 'G'
    
    if fc_interface_container['neighbor']:
        port_wwn = ', '.join(fc_interface_container['neighbor']['wwn'])
    else:
        port_wwn = None
    # print(fc_interface_container['name'], port_wwn)
    
    if sw_port_vfid_dct:
        sw_name, vf_id = sw_port_vfid_dct[slot_port_number]
    else:
        sw_info_dct = list(switch_dct.values())[0]
        sw_name = sw_info_dct['user-friendly-name']
        vf_id = sw_info_dct['vf-id']
    
    port_name = fc_interface_container['user-friendly-name'].rstrip('.')
    port_name_dct[slot_port_number] = port_name
    port_status_current = [sw_name,
                           vf_id,
                           fc_interface_container['default-index'],
                           slot_port_number,
                           int(slot_number),
                           int(port_number),
                           port_name,
                           port_fcid, 
                           port_speed,
                           fc_interface_container['physical-state'].capitalize(),
                           port_type_dct[fc_interface_container['port-type']],
                           port_wwn,
                           fc_interface_container['pod-license-status'],
                           fc_interface_container['is-enabled-state']]
    
    port_status_lst.append(port_status_current)
    
port_status_vfid_sorted_lst = sorted(port_status_lst, key=lambda lst: (lst[1], lst[4], lst[5]))


'fcid-hex' 'name' 'neighbor'  'neighbor-node-wwn' 'physical-state' 'user-friendly-name' 'auto-negotiate'

['name', 'wwn', 'port-type', 'speed', 'max-speed', 'user-friendly-name', 'operational-status', 'enabled-state', 'is-enabled-state', 'auto-negotiate', 'isl-ready-mode-enabled', 
 'long-distance', 'trunk-port-enabled', 'vc-link-init', 'pod-license-status', 'default-index', 'fcid', 'fcid-hex', 'physical-state', 'persistent-disable', 'g-port-locked', 
 'e-port-disable', 'ex-port-enabled', 'npiv-enabled', 'npiv-pp-limit', 'rate-limit-enabled', 'qos-enabled', 'csctl-mode-enabled', 'port-autodisable-enabled', 'e-port-credit', 
 'd-port-enable', 'octet-speed-combo', 'compression-configured', 'compression-active', 'encryption-active', 'mirror-port-enabled', 'non-dfe-enabled', 'fec-enabled', 'fec-active', 
 'via-tts-fec-enabled', 'neighbor', 'target-driven-zoning-enable', 'npiv-flogi-logout-enabled', 'sim-port-enabled', 'f-port-buffers', 'fault-delay-enabled', 'credit-recovery-enabled', 
 'credit-recovery-active', 'rscn-suppression-enabled', 'los-tov-mode-enabled']


# switch_params_lst = ['domain-id', 'firmware-version', 'ip-address', 'name', 'principal', 'switch-user-friendly-name']

# logical_switch_params_lst = ['base-switch-enabled',  'default-switch-status',  'fabric-id', 'logical-isl-enabled', 'port-member-list', 'switch-wwn']


# san03_nord.fc_switch['Response']['fibrechannel-switch'][0].keys()


# ['name', 'domain-id', 'fcid', 'fcid-hex', 'user-friendly-name', 'enabled-state', 'is-enabled-state', 'operational-status', 'banner', 'up-time', 'domain-name', 'dns-servers', 'principal', 'ip-address', 'subnet-mask', 'model', 'firmware-version', 'ip-static-gateway-list', 'vf-id', 'fabric-user-friendly-name', 'ag-mode']




gauge_ch_name.labels(ch_name).set(1)
gauge_sw_sn.labels(switch_sn).set(1)
gauge_sw_model.labels(sw_model).set(1)
gauge_vf_mode.labels(ch_name, vf_mode).set(vf_status)
gauge_sw_datetime.labels(sw_datetime).set(1)
gauge_tz.labels(sw_tz).set(1)


san03_nord.fc_switch
san03_nord.fс_switch













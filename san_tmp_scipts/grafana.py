# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 19:07:04 2023

@author: kavlasenko
"""

import re
import requests
import time
from requests.auth import HTTPBasicAuth
from prometheus_client import start_http_server, Gauge




port_type_dct = {
    0: 'Unknown',
    7: 'E_Port',
    10: 'G_Port',
    11: 'U_Port (Default)',
    15: 'F_Port',
    16: 'L_Port',
    17: 'FCoE Port',
    19: 'EX_Port',
    20: 'D_Port',
    21: 'SIM Port',
    22: 'AF_Port',
    23: 'AE_Port',
    25: 'VE_Port',
    26: 'Ethernet Flex Port',
    29: 'Flex Port',
    30: 'N_Port',
    32768: 'LB_Port'
    }

# url = r'http://10.202.18.120/rest/running/brocade-fabric/fabric-switch'



# Accept = application/yang-data+json




ip_pattern = r'^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$'

class BrocadeSwitchTelemetry:
    
    USERNAME = 'rest_api'
    PASSWORD = 'REST-api2023'
    HEADERS = {
        'Accept': 'application/yang-data+json', 
        'Content-Type': 'application/yang-data+json'
        }
    
    def __init__(self, sw_ipddress):
        
        if re.search(ip_pattern, sw_ipddress):
            self._sw_ipaddress = sw_ipddress
        else:
            raise ValueError(f'ERROR: Incorrect IP address format "{sw_ipddress}"')
            
        self._chassis = self.get_sw_telemetry('brocade-chassis', 'chassis')
        self._fabric_switch = self.get_sw_telemetry('brocade-fabric', 'fabric-switch')
        self._fc_switch = self.get_sw_telemetry('brocade-fibrechannel-switch', 'fibrechannel-switch')
        self._fc_logical_switch = self.get_sw_telemetry('brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch')
        self._time_zone = self.get_sw_telemetry('brocade-time', 'time-zone')
        self._ps = self.get_sw_telemetry('brocade-fru', 'power-supply')
        self._fan = self.get_sw_telemetry('brocade-fru', 'fan')
        self._switch_status_policy_report = self.get_sw_telemetry('brocade-maps', 'switch-status-policy-report')
        self._system_resources = self.get_sw_telemetry('brocade-maps', 'system-resources')
        self._maps_policy = self.get_sw_telemetry('brocade-maps', 'maps-policy')
        self._maps_config = self.get_sw_telemetry('brocade-maps', 'maps-config')
        self._maps_config = self.get_sw_telemetry('brocade-maps', 'dashboard-rule')
        self._sw_license = self.get_sw_telemetry('brocade-license', 'license')
        self._fc_interface = self.get_sw_telemetry('brocade-interface', 'fibrechannel')
        self._fc_statistics = self.get_sw_telemetry('brocade-interface', 'fibrechannel-statistics')
        self._media_rdp = self.get_sw_telemetry('brocade-media', 'media-rdp')
            
        
    def create_restapi_url(self, module_name, module_type):
        url = r'http://' + self.sw_ipaddress + '/rest/running/' + module_name + '/' + module_type
        return url
    
    def get_sw_telemetry(self, module_name, module_type):

        url = self.create_restapi_url(module_name, module_type)
        # response = requests.get(url, auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD), headers=headers, params=params)
        
        response = requests.get(url, auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD), headers=self.HEADERS)
        print(module_name, module_type, response.status_code)
        return response.json()

    @property
    def sw_ipaddress(self):
        return self._sw_ipaddress

    @property
    def chassis(self):
        return self._chassis
    
    @property
    def fabric_switch(self):
        return self._fabric_switch
    
    @property
    def fc_switch(self):
        return self._fibrechannel_switch
    
    @property
    def fc_logical_switch(self):
        return self._fc_logical_switch
    
    @property
    def time_zone(self):
        return self._time_zone
    
    @property
    def ps(self):
        return self._ps
    
    @property
    def fan(self):
        return self._fan
    
    @property
    def switch_status_policy_report(self):
        return self._switch_status_policy_report
    
    @property
    def system_resources(self):
        return self._system_resources
    
    @property
    def maps_policy(self):
        return self._maps_policy
    
    @property
    def maps_config(self):
        return self._maps_config
    
    @property
    def dashboard_rule(self):
        return self._dashboard_rule
    
    @property
    def sw_license(self):
        return self._sw_license
    
    @property
    def fc_interface(self):
        return self._fc_interface
    
    @property
    def fc_statistics(self):
        return self._fc_statistics
    
    @property
    def media_rdp(self):
        return self._media_rdp
    
    # @property
    # def (self):
    #     return self.
    
    # @property
    # def (self):
    #     return self.
    

    # @chassis.setter
    # def chassis(self):
    #     module_name = 'brocade-chassis'
    #     module_type = 'chassis'
    #     self._chassis = self.get_sw_telemetry(module_name, module_type)


    # @property
    # def chassis(self):
    #     module_name = 'brocade-chassis'
    #     module_type = 'chassis'
    #     return self.get_sw_telemetry(module_name, module_type)


    # def get_chassis_telemetry(self):
    #     module_name = 'brocade-chassis'
    #     module_type = 'chassis'
    #     return self.get_sw_telemetry(module_name, module_type)


    
    # @property
    # def fabric_switch(self):
    #     module_name = 'brocade-fabric'
    #     module_type = 'fabric-switch'
    #     return self.get_sw_telemetry(module_name, module_type)
    

st = time.time()
san03_nord = BrocadeSwitchTelemetry(sw_ipddress='10.202.18.120')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


st = time.time()
san23_ost = BrocadeSwitchTelemetry(sw_ipddress='10.221.5.178')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


st = time.time()
san49_nord = BrocadeSwitchTelemetry(sw_ipddress='10.213.16.22')
elapsed_time = time.time() - st
print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


san03_nord.sw_ipaddress




san03_nord.chassis
san03_nord.fabric_switch

# san03_nord.create_restapi_url(module_name='www', module_type='qwqwq')

fabric_switch2 = san03_nord.get_fabric_switch_telemetry()
        




def create_url(sw_ipddress, module_name, module_type):
    url = r'http://' + sw_ipddress + '/rest/running/' + module_name + '/' + module_type
    return url



def get_telemetry(url, vf_id=None):

    headers = {'Accept': 'application/yang-data+json', 'Content-Type': 'application/yang-data+json'}
    params = {'vf-id': vf_id} if vf_id else {}
    response = requests.get(url, auth=HTTPBasicAuth(username, password), headers=headers, params=params)
    print(response.status_code)
    return response.json()



username = 'rest_api'
password = 'REST-api2023'

# sw_ipddress = '10.202.18.120'
sw_ipddress = '10.221.5.178'
vf_id = 128

# chassis_info
module_name = 'brocade-chassis'
module_type = 'chassis'
url = create_url(sw_ipddress, module_name, module_type)
chassis = get_telemetry(url)

# switch_info
module_name = 'brocade-fabric'
module_type = 'fabric-switch'
url = create_url(sw_ipddress, module_name, module_type)
fabric_switch = get_telemetry(url)



# fibrechannel_switch
module_name = 'brocade-fibrechannel-switch'
module_type = 'fibrechannel-switch'
url = create_url(sw_ipddress, module_name, module_type)
fibrechannel_switch = get_telemetry(url, vf_id)

# brocade-fibrechannel-logical-switch
module_name = 'brocade-fibrechannel-logical-switch'
module_type = 'fibrechannel-logical-switch'
url = create_url(sw_ipddress, module_name, module_type)
fibrechannel_logical_switch = get_telemetry(url)



# time zone
module_name = 'brocade-time'
module_type = 'time-zone'
url = create_url(sw_ipddress, module_name, module_type)
time_zone = get_telemetry(url)



# power supply
module_name = 'brocade-fru'
module_type = 'power-supply'
url = create_url(sw_ipddress, module_name, module_type)
power_supply = get_telemetry(url)

# fan
module_name = 'brocade-fru'
module_type = 'fan'
url = create_url(sw_ipddress, module_name, module_type)
fan = get_telemetry(url)

# sensor
module_name = 'brocade-fru'
module_type = 'sensor'
url = create_url(sw_ipddress, module_name, module_type)
sensor = get_telemetry(url)


# maps
# switch_status_policy_report
module_name = 'brocade-maps'
module_type = 'switch-status-policy-report'
url = create_url(sw_ipddress, module_name, module_type)
switch_status_policy_report = get_telemetry(url)

# system-resources
module_name = 'brocade-maps'
module_type = 'system-resources'
url = create_url(sw_ipddress, module_name, module_type)
system_resources = get_telemetry(url)

# maps-policy
module_name = 'brocade-maps'
module_type = 'maps-policy'
url = create_url(sw_ipddress, module_name, module_type)
maps_policy = get_telemetry(url)

# maps-config
module_name = 'brocade-maps'
module_type = 'maps-config'
url = create_url(sw_ipddress, module_name, module_type)
maps_config = get_telemetry(url)


# maps-config
module_name = 'brocade-maps'
module_type = 'dashboard-rule'
url = create_url(sw_ipddress, module_name, module_type)
dashboard_rule = get_telemetry(url)


# maps-config
module_name = 'brocade-maps'
module_type = 'dashboard-rule'
url = create_url(sw_ipddress, module_name, module_type)
dashboard_rule = get_telemetry(url)



# licenses
module_name = 'brocade-license'
module_type = 'license'
url = create_url(sw_ipddress, module_name, module_type)
sw_license = get_telemetry(url)

# brocade-interface/fibrechannel
# po
module_name = 'brocade-interface'
module_type = 'fibrechannel'
url = create_url(sw_ipddress, module_name, module_type)
fibrechannel_interface = get_telemetry(url, vf_id)


# fibrechannel-statistics
module_name = 'brocade-interface'
module_type = 'fibrechannel-statistics'
url = create_url(sw_ipddress, module_name, module_type)
fibrechannel_statistics = get_telemetry(url, vf_id)

# brocade-media
module_name = 'brocade-media'
module_type = 'media-rdp'
url = create_url(sw_ipddress, module_name, module_type)
media_rdp = get_telemetry(url, vf_id)


# brocade-media
module_name = 'brocade-media'
module_type = 'media-rdp_'
url = create_url(sw_ipddress, module_name, module_type)
fail = get_telemetry(url, vf_id)
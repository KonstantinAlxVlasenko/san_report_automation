# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 14:11:46 2024

@author: kavlasenko
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Dec 31 14:04:57 2023

@author: kavlasenko
"""

from ipaddress import ip_address
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectTimeout
from dataclasses import dataclass, field
from typing import ClassVar

@dataclass
class BrocadeSwitchTelemetry:
    
    USERNAME: ClassVar[str] = 'rest_api'
    PASSWORD: ClassVar[str] = 'REST-api2023'
    HEADERS: ClassVar[dict] = {
        'Accept': 'application/yang-data+json', 
        'Content-Type': 'application/yang-data+json'
        }
    REQUEST_TIMEOUT_ERROR: ClassVar[dict] = {'errors': {'error': [{'error-message': 'Request has timed out'}]}}
    # ipaddress: ip_address = field(repr=False)
    sw_ipaddress: ip_address
    secure_login: bool = False
    chassis: dict = field(init=False, repr=False)
    fabric_switch: dict = field(init=False, repr=False)
    fc_switch: dict = field(init=False, repr=False)
    fc_logical_switch: dict = field(init=False, repr=False)
    ts_timezone: dict = field(init=False, repr=False)
    fru_ps: dict = field(init=False, repr=False)
    fru_fan: dict = field(init=False, repr=False)
    fru_sensor: dict = field(init=False, repr=False)
    ssp_report: dict = field(init=False, repr=False)
    system_resources: dict = field(init=False, repr=False)
    maps_policy: dict = field(init=False, repr=False)
    maps_config: dict = field(init=False, repr=False)
    dashboard_rule: dict = field(init=False, repr=False)
    sw_license: dict = field(init=False, repr=False)
    fc_interface: dict = field(init=False, repr=False)
    fc_statistics: dict = field(init=False, repr=False)
    media_rdp: dict = field(init=False, repr=False)
    fdmi_hba: dict = field(init=False, repr=False)
    fdmi_port: dict = field(init=False, repr=False)
    fc_nameserver: dict = field(init=False, repr=False)

    
    def __post_init__(self) -> None:
        self.sw_ipaddress = ip_address(self.sw_ipaddress)
        self.chassis = self._get_sw_telemetry('brocade-chassis', 'chassis')
        self.fabric_switch = self._get_sw_telemetry('brocade-fabric', 'fabric-switch')
        self.fc_switch = self._get_sw_telemetry('brocade-fibrechannel-switch', 'fibrechannel-switch')
        self.fc_logical_switch = self._get_sw_telemetry('brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch')
        self.ts_timezone = self._get_sw_telemetry('brocade-time', 'time-zone')
        self.fru_ps = self._get_sw_telemetry('brocade-fru', 'power-supply')
        self.fru_fan = self._get_sw_telemetry('brocade-fru', 'fan')
        self.fru_sensor = self._get_sw_telemetry('brocade-fru', 'sensor')
        self.ssp_report = self._get_sw_telemetry('brocade-maps', 'switch-status-policy-report')
        self.system_resources = self._get_sw_telemetry('brocade-maps', 'system-resources')
        self.maps_policy = self._get_sw_telemetry('brocade-maps', 'maps-policy')
        self.maps_config = self._get_sw_telemetry('brocade-maps', 'maps-config')
        self.dashboard_rule = self._get_sw_telemetry('brocade-maps', 'dashboard-rule')
        self.sw_license = self._get_sw_telemetry('brocade-license', 'license')
        self.fc_interface = self._get_sw_telemetry('brocade-interface', 'fibrechannel')
        self.fc_statistics = self._get_sw_telemetry('brocade-interface', 'fibrechannel-statistics')
        self.media_rdp = self._get_sw_telemetry('brocade-media', 'media-rdp')
        self.fdmi_hba = self._get_sw_telemetry('brocade-fdmi', 'hba')
        self.fdmi_port = self._get_sw_telemetry('brocade-fdmi', 'port')
        self.fc_nameserver = self._get_sw_telemetry('brocade-name-server', 'fibrechannel-name-server')
        self._get_container_error_message()
        
    def _create_restapi_url(self, module_name: str, module_type: str):
        
        login_protocol = ('https' if self.secure_login else 'http') + r'://'
        url = login_protocol + str(self.sw_ipaddress) + '/rest/running/' + module_name + '/' + module_type
        return url

    
    def _get_sw_telemetry(self, module_name: str, module_type: str):

        url = self._create_restapi_url(module_name, module_type)
        try:        
            response = requests.get(url, auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD), headers=self.HEADERS, timeout=61)
        except ConnectTimeout:
            return self.REQUEST_TIMEOUT_ERROR
        print(module_name, module_type, response.status_code)
        return response.json()


    def _get_container_error_message(self):
        
        print('Checking errors')
        container_lst = [self.chassis, self.fabric_switch, self.fc_switch, self.fc_logical_switch,
                         self.ts_timezone, self.fru_ps, self.fru_fan, self.fru_sensor, self.ssp_report,
                         self.system_resources, self.maps_policy, self.maps_config, 
                         self.dashboard_rule, self.sw_license, 
                         self.fc_interface, self.fc_statistics, self.media_rdp, 
                         self.fdmi_hba, self.fdmi_port, self.fc_nameserver]
        
        for container in container_lst:
        
            if 'errors' in container:
                errors_lst = container['errors']['error']
                errors_msg_lst = [error.get('error-message') for error in errors_lst if error.get('error-message')]
                if errors_msg_lst:
                    container['error-message'] = ', '.join(errors_msg_lst)
                else:
                    container['error-message'] = 'No error message found'
            else:
                container['error-message'] = None


san03_nord_dcls = BrocadeSwitchTelemetry(sw_ipaddress='10.202.18.120')
print(san03_nord_dcls)

san03_nord_dcls.chassis = {'fake': 'yes'}

    
    # def __init__(self, sw_ipaddress: ip_address, secure_login=False):
        
    #     self._sw_ipaddress = ip_address(sw_ipaddress)
    #     self._secure_login = secure_login
    #     self._chassis = self.get_sw_telemetry('brocade-chassis', 'chassis')
    #     self._fabric_switch = self.get_sw_telemetry('brocade-fabric', 'fabric-switch')
    #     self._fc_switch = self.get_sw_telemetry('brocade-fibrechannel-switch', 'fibrechannel-switch')
    #     self._fc_logical_switch = self.get_sw_telemetry('brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch')
    #     self._time_zone = self.get_sw_telemetry('brocade-time', 'time-zone')
    #     self._ps = self.get_sw_telemetry('brocade-fru', 'power-supply')
    #     self._fan = self.get_sw_telemetry('brocade-fru', 'fan')
    #     self._sensor = self.get_sw_telemetry('brocade-fru', 'sensor')
    #     self._ssp_report = self.get_sw_telemetry('brocade-maps', 'switch-status-policy-report')
    #     self._system_resources = self.get_sw_telemetry('brocade-maps', 'system-resources')
    #     self._maps_policy = self.get_sw_telemetry('brocade-maps', 'maps-policy')
    #     self._maps_config = self.get_sw_telemetry('brocade-maps', 'maps-config')
    #     self._dashboard_rule = self.get_sw_telemetry('brocade-maps', 'dashboard-rule')
    #     self._sw_license = self.get_sw_telemetry('brocade-license', 'license')
    #     self._fc_interface = self.get_sw_telemetry('brocade-interface', 'fibrechannel')
    #     self._fc_statistics = self.get_sw_telemetry('brocade-interface', 'fibrechannel-statistics')
    #     self._media_rdp = self.get_sw_telemetry('brocade-media', 'media-rdp')
    #     self._fdmi_hba = self.get_sw_telemetry('brocade-fdmi', 'hba')
    #     self._fdmi_port = self.get_sw_telemetry('brocade-fdmi', 'port')
    #     self._fc_nameserver = self.get_sw_telemetry('brocade-name-server', 'fibrechannel-name-server')
    #     self.get_container_error_message()
            
        
    # def create_restapi_url(self, module_name: str, module_type: str):
        
    #     login_protocol = ('https' if self.secure_login else 'http') + r'://'
    #     url = login_protocol + self.sw_ipaddress + '/rest/running/' + module_name + '/' + module_type
    #     return url
    
    # def get_sw_telemetry(self, module_name: str, module_type: str):

    #     url = self.create_restapi_url(module_name, module_type)
    #     try:        
    #         response = requests.get(url, auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD), headers=self.HEADERS, timeout=61)
    #     except ConnectTimeout:
    #         return self.REQUEST_TIMEOUT_ERROR
    #     print(module_name, module_type, response.status_code)
    #     return response.json()

    # @property
    # def sw_ipaddress(self):
    #     return str(self._sw_ipaddress)
    
    # @property
    # def secure_login(self):
    #     return self._secure_login    

    # @property
    # def chassis(self):
    #     """The complete details of the chassis."""
    #     return self._chassis
    
    # @property
    # def fabric_switch(self):
    #     """The list of configured switches in the fabric."""
    #     return self._fabric_switch
    
    # @property
    # def fc_switch(self):
    #     """Switch state parameters."""
    #     return self._fc_switch
    
    # @property
    # def fc_logical_switch(self):
    #     """The logical switch state parameters of all configured logical switches."""
    #     return self._fc_logical_switch
    
    # @property
    # def time_zone(self):
    #     """The time zone parameters."""
    #     return self._time_zone
    
    # @property
    # def ps(self):
    #     """The details about the power supply units."""
    #     return self._ps
    
    # @property
    # def fan(self):
    #     """The details about the fan units"""
    #     return self._fan
    
    # @property
    # def sensor(self):
    #     return self._sensor
    
    # @property
    # def ssp_report(self):
    #     """The Switch Status Policy report container. 
    #     The SSP report provides the overall health status of the switch."""
    #     return self._ssp_report
    
    # @property
    # def system_resources(self):
    #     """The system resources (such as CPU, RAM, and flash memory usage) container. 
    #     Note that usage is not real time and may be delayed up to 2 minutes."""
    #     return self._system_resources
    
    # @property
    # def maps_policy(self):
    #     """The MAPS policy container.
    #     This container enables you to view monitoring policies.
    #     A MAPS policy is a set of rules that define thresholds for measures and actions to take when a threshold is triggered.
    #     When you enable a policy, all of the rules in the policy are in effect. A switch can have multiple policies."""
    #     return self._maps_policy
    
    # @property
    # def maps_config(self):
    #     """The MAPS configuration container (MAPS actions)."""
    #     return self._maps_config
    
    # @property
    # def dashboard_rule(self):
    #     """A list of dashboards container. 
    #     The dashboard enables you to view the events or rules triggered 
    #     and the objects on which the rules were triggered over a specified period of time.
    #     You can view a triggered rules list for the last 7 days."""
    #     return self._dashboard_rule
    
    # @property
    # def sw_license(self):
    #     """The container for licenses installed on the switch."""
    #     return self._sw_license
    
    # @property
    # def fc_interface(self):
    #     """FC interface-related configuration and operational state."""
    #     return self._fc_interface
    
    # @property
    # def fc_statistics(self):
    #     """Statistics for all FC interfaces on the device."""
    #     return self._fc_statistics
    
    # @property
    # def media_rdp(self):
    #     """SFP transceivers media data container. 
    #     The summary includes information that describes the SFP capabilities, 
    #     interfaces, manufacturer, and other information."""
    #     return self._media_rdp
    
    # @property
    # def fdmi_hba(self):
    #     """A detailed view of the Fabric Device Management Interface (FDMI).
    #     List of HBA attributes registered with FDMI."""
    #     return self._fdmi_hba
    
    # @property
    # def fdmi_port(self):
    #     """A detailed view of the Fabric Device Management Interface (FDMI).
    #     A list of HBA port attributes registered with FDMI."""
    #     return self._fdmi_port
    
    # @property 
    # def fc_nameserver(self):
    #     """Name Server container"""
    #     return self._fc_nameserver
    
    # def get_container_error_message(self):
        
    #     print('Checking errors')
    #     container_lst = [self.chassis, self.fabric_switch, self.fc_switch, self.fc_logical_switch,
    #                      self.time_zone, self.ps, self.fan, self.sensor, self.ssp_report,
    #                      self.system_resources, self.maps_policy, self.maps_config, 
    #                      self.dashboard_rule, self.sw_license, 
    #                      self.fc_interface, self.fc_statistics, self.media_rdp, 
    #                      self.fdmi_hba, self.fdmi_port, self.fc_nameserver]
        
    #     for container in container_lst:
        
    #         if 'errors' in container:
    #             errors_lst = container['errors']['error']
    #             errors_msg_lst = [error.get('error-message') for error in errors_lst if error.get('error-message')]
    #             if errors_msg_lst:
    #                 container['error-message'] = ', '.join(errors_msg_lst)
    #             else:
    #                 container['error-message'] = 'No error message found'
    #         else:
    #             container['error-message'] = None
    

# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 16:35:03 2024

@author: kavlasenko
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 15:35:40 2024

@author: kavlasenko
"""

from typing import Any
import time
import httpx
import asyncio

    
import sys
from ipaddress import ip_address
import requests
from requests.auth import HTTPBasicAuth



BASE_URL = "https://httpbin.org"


def fetch_get(client: httpx.Client) -> Any:
    response = client.get(f"{BASE_URL}/get")
    return response.json()


def fetch_post(client: httpx.Client) -> Any:
    data_to_post = {"key": "value"}
    response = client.post(f"{BASE_URL}/post", json=data_to_post)
    return response.json()


def fetch_put(client: httpx.Client) -> Any:
    data_to_put = {"key": "updated_value"}
    response = client.put(f"{BASE_URL}/put", json=data_to_put)
    return response.json()


def fetch_delete(client: httpx.Client) -> Any:
    response = client.delete(f"{BASE_URL}/delete")
    return response.json()


def main() -> None:
    # record the starting time
    start = time.perf_counter()

    with httpx.Client() as client:
        # GET
        print("GET:", fetch_get(client))

        # POST
        print("POST:", fetch_post(client))

        # PUT
        print("PUT:", fetch_put(client))

        # DELETE
        print("DELETE:", fetch_delete(client))

    # record the ending time
    end = time.perf_counter()
    print(f"Time taken: {end - start:.2f} seconds.")
    


class BrocadeSwitchTelemetry:
    
    USERNAME = 'rest_api'
    PASSWORD = 'REST-api2023'
    HEADERS = {
        'Accept': 'application/yang-data+json', 
        'Content-Type': 'application/yang-data+json',
        }
    
    # HEADERS = {
    #     'Accept': 'application/yang-data+json', 
    #     'Content-Type': 'application/yang-data+json',
    #     'Authorization': 'Custom_Basic cmVzdF9hcGk6eHh4OmRjOWZmYmFhNTA0MWMxNDIyYjRhMDdjMWRlNDI4OGI4NDgwNjMxZmE2ZmY3M2ZmNjU3Y2IwYzg0MDVjNmM3Nzk='
    #     }
    # REQUEST_TIMEOUT_ERROR = {'errors': {'error': [{'error-message': 'Request has timed out'}]}}
    # REQUEST_CONNECTION_ERROR = {'errors': {'error': [{'error-message': 'Failed to establish a new connection'}]}}
    VF_MODE_RETRIEVE_ERROR = {'errors': {'error': [{'error-message': 'VF mode has not been retreived'}]}}
    VF_ID_RETRIEVE_ERROR = {'errors': {'error': [{'error-message': 'VF IDs has not been retreived'}]}}
    
    def __init__(self, sw_ipaddress: ip_address, secure_login=False):
        
        self._sw_ipaddress = ip_address(sw_ipaddress)
        self._secure_login = secure_login
        
        self._extract_ch_unique_telemetry()
        
        
        # self._chassis = {}
        # self._fc_logical_switch = {}
        # self._ts_timezone = {}
        # self._fru_ps = {}
        # self._fru_fan = {}
        # self._fru_sensor = {}
        # self._ssp_report = {}
        # self._system_resources = {}
        # self._sw_license = {}

        
        # self._ch_unique_containers =[
        #     [self._chassis, ('brocade-chassis', 'chassis')],
        #     [self._fc_logical_switch, ('brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch')],
        #     [self._ts_timezone, ('brocade-time', 'time-zone')],
        #     [self._fru_ps, ('brocade-fru', 'power-supply')],
        #     [self._fru_fan, ('brocade-fru', 'fan')],
        #     [self._fru_sensor, ('brocade-fru', 'sensor')],
        #     [self._ssp_report, ('brocade-maps', 'switch-status-policy-report')],
        #     [self._system_resources, ('brocade-maps', 'system-resources')],
        #     [self._sw_license, ('brocade-license', 'license')]
        #     ]
        
        # # with httpx.Client(verify=False) as client:
        # #     for container, (module_name, module_type) in self._ch_unique_containers:
        # #         container.update(self._get_sw_telemetry(client, module_name, module_type))
        # #         BrocadeSwitchTelemetry._get_container_error_message(container)
                
                
        # async with httpx.AsyncClient() as client:
            
        #     tasks = [self._get_sw_telemetry(client, module_name, module_type) 
        #              for container, *(module_name, module_type) in self._ch_unique_containers]
            
        #     results = await asyncio.gather(*tasks)
        
        # print(results[0])
            
        
        # self._vf_enabled = self._check_vfmode_on()
        # self._vfid_lst = self._get_vfid_list()
        
        
        # self._fabric_switch = {}
        # self._fc_switch = {}
        # self._maps_policy = {}
        # self._maps_config = {}
        # self._dashboard_rule = {}
        # self._fc_interface = {}
        # self._fc_statistics = {}
        # self._media_rdp = {}
        # self._fdmi_hba = {}
        # self._fdmi_port = {}
        # self._fc_nameserver = {}

        # self._vf_unique_containers = [
        #     [self._fabric_switch, ('brocade-fabric', 'fabric-switch')],
        #     [self._fc_switch, ('brocade-fibrechannel-switch', 'fibrechannel-switch')],
        #     [self._maps_policy, ('brocade-maps', 'maps-policy')],
        #     [self._maps_config, ('brocade-maps', 'maps-config')],
        #     [self._dashboard_rule, ('brocade-maps', 'dashboard-rule')],
        #     [self._fc_interface, ('brocade-interface', 'fibrechannel')],
        #     [self._fc_statistics, ('brocade-interface', 'fibrechannel-statistics')],
        #     [self._media_rdp, ('brocade-media', 'media-rdp')],
        #     [self._fdmi_hba, ('brocade-fdmi', 'hba')],
        #     [self._fdmi_port, ('brocade-fdmi', 'port')],
        #     [self._fc_nameserver, ('brocade-name-server', 'fibrechannel-name-server')]
        #     ]
        
        # with httpx.Client(verify=False) as client:
        
        #     # vf mode value is not retrieved
        #     if self._vf_enabled is None:
        #         # ch_container_error = BrocadeSwitchTelemetry._get_container_error_message(self._chassis)
        #         for container, _ in self._vf_unique_containers:
        #             container[-2] = BrocadeSwitchTelemetry.VF_MODE_RETRIEVE_ERROR
        #             container[-2]['status-code'] = None
        #             # container[-2] = self._chassis
        #             BrocadeSwitchTelemetry._get_container_error_message(container[-2])
            
        #     # vf mode disabled
        #     elif not self._vf_enabled:
        #         for container, (module_name, module_type) in self._vf_unique_containers:
        #             container[-1] = self._get_sw_telemetry(client, module_name, module_type)
        #             BrocadeSwitchTelemetry._get_container_error_message(container[-1])
            
        #     # vf mode is enabled but vf ids was not extracted
        #     elif self._vfid_lst is None:
        #         # fc_logical_sw_container_error = BrocadeSwitchTelemetry._get_container_error_message(self._fc_logical_switch)
        #         for container, _ in self._vf_unique_containers:
        #             container[-3] = BrocadeSwitchTelemetry.VF_ID_RETRIEVE_ERROR
        #             container[-3]['status-code'] = None
        #             # container[-3] = self._fc_logical_switch
        #             BrocadeSwitchTelemetry._get_container_error_message(container[-3])
                
        #     # vf mode is enabled with single virtual switch
        #     elif self._vfid_lst and len(self._vfid_lst) == 1:
        #         vf_id = self._vfid_lst[0]
        #         for container, (module_name, module_type) in self._vf_unique_containers:
        #             container[vf_id] = self._get_sw_telemetry(client, module_name, module_type)
        #             BrocadeSwitchTelemetry._get_container_error_message(container[vf_id])
                
        #      # vf mode is enabled with multiple virtual switches   
        #     elif self._vfid_lst and len(self._vfid_lst) > 1:
                
        #         for vf_id in self._vfid_lst:
        #             for container, (module_name, module_type) in self._vf_unique_containers:
        #                 container[vf_id] = self._get_sw_telemetry(client, module_name, module_type, vf_id)
        #                 BrocadeSwitchTelemetry._get_container_error_message(container[vf_id])
                
            
        
    async def _create_restapi_url(self, module_name: str, module_type: str):
        
        login_protocol = ('https' if self.secure_login else 'http') + r'://'
        url = login_protocol + self.sw_ipaddress + '/rest/running/' + module_name + '/' + module_type
        return url
    
    async def _get_sw_telemetry(self, client: httpx.AsyncClient, module_name: str, module_type: str, vf_id: int=None):

        url = self._create_restapi_url(module_name, module_type)
        params = {'vf-id': vf_id} if vf_id else {}
        
        try:
            
            response = await client.get(url, 
                                    auth=HTTPBasicAuth(BrocadeSwitchTelemetry.USERNAME, BrocadeSwitchTelemetry.PASSWORD),
                                    params=params,
                                    headers=BrocadeSwitchTelemetry.HEADERS,
                                    timeout=31)
            
            current_telemetry = response.json()
            current_telemetry['status-code'] = response.status_code
            
            print(response.status_code)
            return current_telemetry
        
        except (Exception) as error:
            # print ("Error", error)
            current_telemetry ={'errors': {'error': [{'error-message': str(error)}]}}
            current_telemetry['status-code'] = None
            return current_telemetry
        
        
        
    async def _extract_ch_unique_telemetry(self):
        
        
        
        self._chassis = {}
        self._fc_logical_switch = {}
        self._ts_timezone = {}
        self._fru_ps = {}
        self._fru_fan = {}
        self._fru_sensor = {}
        self._ssp_report = {}
        self._system_resources = {}
        self._sw_license = {}

        
        self._ch_unique_containers =[
            [self._chassis, ('brocade-chassis', 'chassis')],
            [self._fc_logical_switch, ('brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch')],
            [self._ts_timezone, ('brocade-time', 'time-zone')],
            [self._fru_ps, ('brocade-fru', 'power-supply')],
            [self._fru_fan, ('brocade-fru', 'fan')],
            [self._fru_sensor, ('brocade-fru', 'sensor')],
            [self._ssp_report, ('brocade-maps', 'switch-status-policy-report')],
            [self._system_resources, ('brocade-maps', 'system-resources')],
            [self._sw_license, ('brocade-license', 'license')]
            ]
        
        # with httpx.Client(verify=False) as client:
        #     for container, (module_name, module_type) in self._ch_unique_containers:
        #         container.update(self._get_sw_telemetry(client, module_name, module_type))
        #         BrocadeSwitchTelemetry._get_container_error_message(container)
                
                
        async with httpx.AsyncClient() as client:
            
            tasks = [self._get_sw_telemetry(client, 'brocade-chassis', 'chassis'),
                      self._get_sw_telemetry(client, 'brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch'),
                      ]
            
            # tasks = [self._get_sw_telemetry(client, module_name, module_type) for ]
            
            results = await asyncio.gather(*tasks)
        
        print(results[0])
        
        
        # except requests.Timeout:
        #     print('UUUUUUUUUUUUUUUUUUUUUUUU')
        #     exit()
            
        # except OSError as e:
        #     print(e)
        #     sys.exit()
            
        # except TimeoutError:
        #     print('ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ')
        #     exit()
        
        # except (ConnectTimeout, TimeoutError):
        #     print('!!!!!!!!!!!!!!!!!!!!!!!!!')
        #     print(response.status_code)
        #     return self.REQUEST_TIMEOUT_ERROR
        # except (ConnectionError, NewConnectionError, MaxRetryError):
        #     print('YYYYYYYYYYYYYYYYYYYY')
        #     print(response.status_code)
        #     return self.REQUEST_CONNECTION_ERROR
        # print(module_name, module_type, response.status_code)
        # return response.json()
    
    
    def _check_vfmode_on(self) -> str:
        """
        Function extracts leaf value from the chassis container.
        
        :param1 leaf_name: container leaf name
        :returns: chassis container leaf value or error message
        """
        
        if self.chassis.get('Response'):
            return self.chassis['Response']['chassis']['vf-enabled']
        
        


    
    def _get_vfid_list(self):
        
        
        if self.fc_logical_switch.get('Response'):
            vfid_lst = []
            container = self.fc_logical_switch['Response']['fibrechannel-logical-switch']
            for logical_sw in container:
                vfid_lst.append(logical_sw['fabric-id'])
            return vfid_lst
        
    
    def __repr__(self):

        return f"{self.__class__.__name__} ip_address: {self.sw_ipaddress}"
    


    @staticmethod
    def _get_container_error_message(container):
        
        # print('Checking errors')
   
        if 'errors' in container:
            errors_lst = container['errors']['error']
            errors_msg_lst = [error.get('error-message') for error in errors_lst if error.get('error-message')]
            if errors_msg_lst:
                container['error-message'] = ', '.join(errors_msg_lst)
            else:
                container['error-message'] = 'No error message found'
        else:
            container['error-message'] = None    



    @property
    def sw_ipaddress(self):
        return str(self._sw_ipaddress)
    
    @property
    def secure_login(self):
        return self._secure_login    

    @property
    def chassis(self):
        """The complete details of the chassis."""
        return self._chassis
    
    @property
    def fabric_switch(self):
        """The list of configured switches in the fabric."""
        return self._fabric_switch
    
    @property
    def fc_switch(self):
        """Switch state parameters."""
        return self._fc_switch
    
    @property
    def fc_logical_switch(self):
        """The logical switch state parameters of all configured logical switches."""
        return self._fc_logical_switch
    
    @property
    def ts_timezone(self):
        """The time zone parameters."""
        return self._ts_timezone
    
    @property
    def fru_ps(self):
        """The details about the power supply units."""
        return self._fru_ps
    
    @property
    def fru_fan(self):
        """The details about the fan units"""
        return self._fru_fan
    
    @property
    def fru_sensor(self):
        return self._fru_sensor
    
    @property
    def ssp_report(self):
        """The Switch Status Policy report container. 
        The SSP report provides the overall health status of the switch."""
        return self._ssp_report
    
    @property
    def system_resources(self):
        """The system resources (such as CPU, RAM, and flash memory usage) container. 
        Note that usage is not real time and may be delayed up to 2 minutes."""
        return self._system_resources
    
    @property
    def maps_policy(self):
        """The MAPS policy container.
        This container enables you to view monitoring policies.
        A MAPS policy is a set of rules that define thresholds for measures and actions to take when a threshold is triggered.
        When you enable a policy, all of the rules in the policy are in effect. A switch can have multiple policies."""
        return self._maps_policy
    
    @property
    def maps_config(self):
        """The MAPS configuration container (MAPS actions)."""
        return self._maps_config
    
    @property
    def dashboard_rule(self):
        """A list of dashboards container. 
        The dashboard enables you to view the events or rules triggered 
        and the objects on which the rules were triggered over a specified period of time.
        You can view a triggered rules list for the last 7 days."""
        return self._dashboard_rule
    
    @property
    def sw_license(self):
        """The container for licenses installed on the switch."""
        return self._sw_license
    
    @property
    def fc_interface(self):
        """FC interface-related configuration and operational state."""
        return self._fc_interface
    
    @property
    def fc_statistics(self):
        """Statistics for all FC interfaces on the device."""
        return self._fc_statistics
    
    @property
    def media_rdp(self):
        """SFP transceivers media data container. 
        The summary includes information that describes the SFP capabilities, 
        interfaces, manufacturer, and other information."""
        return self._media_rdp
    
    @property
    def fdmi_hba(self):
        """A detailed view of the Fabric Device Management Interface (FDMI).
        List of HBA attributes registered with FDMI."""
        return self._fdmi_hba
    
    @property
    def fdmi_port(self):
        """A detailed view of the Fabric Device Management Interface (FDMI).
        A list of HBA port attributes registered with FDMI."""
        return self._fdmi_port
    
    @property 
    def fc_nameserver(self):
        """Name Server container"""
        return self._fc_nameserver
    
    
    @property 
    def vf_enabled(self):
        """Virtual Fabrics mode enabled flag"""
        return self._vf_enabled
    
    
    @property 
    def vfid_lst(self):
        """List of Virtual Fabric IDs"""
        return self._vfid_lst
    
    

            
    def get_container_error_message(self):
        
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
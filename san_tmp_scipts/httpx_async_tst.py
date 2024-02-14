# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 17:09:21 2024

@author: kavlasenko
"""

from typing import Any
import time
import asyncio
import httpx

import sys
from ipaddress import ip_address
import requests
from requests.auth import HTTPBasicAuth


USERNAME = 'rest_api'
PASSWORD = 'REST-api2023'
HEADERS = {
    'Accept': 'application/yang-data+json', 
    'Content-Type': 'application/yang-data+json',
    }
    


# def create_restapi_url(sw_ipaddress, secure_login, module_name: str, module_type: str):
    
#     login_protocol = ('https' if secure_login else 'http') + r'://'
#     url = login_protocol + sw_ipaddress + '/rest/running/' + module_name + '/' + module_type
#     return url

async def get_sw_telemetry(client: httpx.AsyncClient, title, sw_ipaddress, secure_login, module_name: str, module_type: str, vf_id: int=None):

    # url = create_restapi_url(module_name, module_type)
    
    
    login_protocol = ('https' if secure_login else 'http') + r'://'
    url = login_protocol + sw_ipaddress + '/rest/running/' + module_name + '/' + module_type
    
    params = {'vf-id': vf_id} if vf_id else {}
    
    try:
        
        response = await client.get(url, 
                                auth=HTTPBasicAuth(USERNAME, PASSWORD),
                                params=params,
                                headers=HEADERS,
                                timeout=31)
        
        current_telemetry = response.json()
        current_telemetry['status-code'] = response.status_code
        
        print(title, vf_id, response.status_code)
        # print(response.json())
        print('-----------------------', '\n')
        return current_telemetry
    
    except (Exception) as error:
        # print ("Error", error)
        current_telemetry ={'errors': {'error': [{'error-message': str(error)}]}}
        current_telemetry['status-code'] = None
        return current_telemetry
    
    
    
async def extract_ch_unique_telemetry(sw_ipaddress, secure_login=False):
    

    
    
    chassis = {}
    fc_logical_switch = {}
    ts_timezone = {}
    fru_ps = {}
    fru_fan = {}
    fru_sensor = {}
    ssp_report = {}
    system_resources = {}
    sw_license = {}

    
    ch_unique_containers =[
        [chassis, ('brocade-chassis', 'chassis')],
        [fc_logical_switch, ('brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch')],
        [ts_timezone, ('brocade-time', 'time-zone')],
        [fru_ps, ('brocade-fru', 'power-supply')],
        [fru_fan, ('brocade-fru', 'fan')],
        [fru_sensor, ('brocade-fru', 'sensor')],
        [ssp_report, ('brocade-maps', 'switch-status-policy-report')],
        [system_resources, ('brocade-maps', 'system-resources')],
        [sw_license, ('brocade-license', 'license')]
        ]
    

            
            
    async with httpx.AsyncClient() as client:
        
        # tasks = [get_sw_telemetry(client, sw_ipaddress, secure_login, 'brocade-chassis', 'chassis'),
        #          get_sw_telemetry(client, sw_ipaddress, secure_login, 'brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch'),
        #           ]
        
        # tasks = [get_sw_telemetry(client, title, sw_ipaddress, secure_login, module[0], module[1]) for title, module in ch_unique_containers]
        
        
        tasks = [
            get_sw_telemetry(client, 'chassis', sw_ipaddress, secure_login, 'brocade-chassis', 'chassis'),
            get_sw_telemetry(client, 'fc_logical_switch', sw_ipaddress, secure_login, 'brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch'),
            get_sw_telemetry(client, 'ts_timezone', sw_ipaddress, secure_login, 'brocade-time', 'time-zone'),
            get_sw_telemetry(client, 'fru_ps', sw_ipaddress, secure_login, 'brocade-fru', 'power-supply'),
            get_sw_telemetry(client, 'fru_fan', sw_ipaddress, secure_login, 'brocade-fru', 'fan'),
            get_sw_telemetry(client, 'fru_sensor', sw_ipaddress, secure_login, 'brocade-fru', 'sensor'),
            get_sw_telemetry(client, 'ssp_report', sw_ipaddress, secure_login, 'brocade-maps', 'switch-status-policy-report'),
            get_sw_telemetry(client, 'system_resources', sw_ipaddress, secure_login, 'brocade-maps', 'system-resources'),
            get_sw_telemetry(client, 'sw_license', sw_ipaddress, secure_login, 'brocade-license', 'license'),
            ]
        
        results = await asyncio.gather(*tasks)
    
    for result in results:
        print(result)
        
        
        
        

    vfid_lst = [128 , 10]
    
    
    fabric_switch = {}
    fc_switch = {}
    maps_policy = {}
    maps_config = {}
    dashboard_rule = {}
    fc_interface = {}
    fc_statistics = {}
    media_rdp = {}
    fdmi_hba = {}
    fdmi_port = {}
    fc_nameserver = {}

    vf_unique_containers = [
        ['fabric_switch', ('brocade-fabric', 'fabric-switch')],
        ['fc_switch', ('brocade-fibrechannel-switch', 'fibrechannel-switch')],
        ['maps_policy', ('brocade-maps', 'maps-policy')],
        ['maps_config', ('brocade-maps', 'maps-config')],
        ['dashboard_rule', ('brocade-maps', 'dashboard-rule')],
        ['fc_interface', ('brocade-interface', 'fibrechannel')],
        ['fc_statistics', ('brocade-interface', 'fibrechannel-statistics')],
        ['media_rdp', ('brocade-media', 'media-rdp')],
        ['fdmi_hba', ('brocade-fdmi', 'hba')],
        ['fdmi_port', ('brocade-fdmi', 'port')],
        ['fc_nameserver', ('brocade-name-server', 'fibrechannel-name-server')]
        ]
    
    
    # vf_tasks = [(name, module[0], module[1], vf_id) for name, module in vf_unique_containers  for vf_id in vfid_lst ]
    
    print('-------------------------------------------------')
    
    async with httpx.AsyncClient() as client:
    
        # vf_tasks = []
        # for vf_id in vfid_lst:
        # vf_tasks = [get_sw_telemetry(client, title, sw_ipaddress, secure_login, module[0], module[1], vf_id) for title, module in vf_unique_containers  for vf_id in vfid_lst ]
        
        # vf_tasks = [
        #     get_sw_telemetry(client, title, sw_ipaddress, secure_login, module[0], module[1], vf_id, 
        #     ]
        
                            
        vf_tasks = [

            get_sw_telemetry(client, 'fabric_switch', sw_ipaddress, secure_login, 'brocade-fabric', 'fabric-switch', 10),
            get_sw_telemetry(client, 'fc_switch', sw_ipaddress, secure_login, 'brocade-fibrechannel-switch', 'fibrechannel-switch', 10),
            get_sw_telemetry(client, 'maps_policy', sw_ipaddress, secure_login, 'brocade-maps', 'maps-policy', 10),
            get_sw_telemetry(client, 'maps_config', sw_ipaddress, secure_login, 'brocade-maps', 'maps-config', 10),
            get_sw_telemetry(client, 'dashboard_rule', sw_ipaddress, secure_login, 'brocade-maps', 'dashboard-rule', 10),
            get_sw_telemetry(client, 'fc_interface', sw_ipaddress, secure_login, 'brocade-interface', 'fibrechannel', 10),
            get_sw_telemetry(client, 'fc_statistics', sw_ipaddress, secure_login, 'brocade-interface', 'fibrechannel-statistics', 10),
            get_sw_telemetry(client, 'media_rdp', sw_ipaddress, secure_login, 'brocade-media', 'media-rdp', 10),
            get_sw_telemetry(client, 'fdmi_hba', sw_ipaddress, secure_login, 'brocade-fdmi', 'hba', 10),
            get_sw_telemetry(client, 'fdmi_port', sw_ipaddress, secure_login, 'brocade-fdmi', 'port', 10),
            get_sw_telemetry(client, 'fc_nameserver', sw_ipaddress, secure_login, 'brocade-name-server', 'fibrechannel-name-server', 10)
            ]                    
        

        
        # vf_tasks.extend(current_vf_tasks)
        results = await asyncio.gather(*vf_tasks)
    # for result in results:
    #     if result.get('errors'):
    #         print(result['errors']['error'])
                
                
    async with httpx.AsyncClient() as client:
    
        # vf_tasks = []
        # for vf_id in vfid_lst:
        # vf_tasks = [get_sw_telemetry(client, title, sw_ipaddress, secure_login, module[0], module[1], vf_id) for title, module in vf_unique_containers  for vf_id in vfid_lst ]
        
        # vf_tasks = [
        #     get_sw_telemetry(client, title, sw_ipaddress, secure_login, module[0], module[1], vf_id, 
        #     ]
        
                            
        vf_tasks = [

            get_sw_telemetry(client, 'fabric_switch', sw_ipaddress, secure_login, 'brocade-fabric', 'fabric-switch', 128),
            get_sw_telemetry(client, 'fc_switch', sw_ipaddress, secure_login, 'brocade-fibrechannel-switch', 'fibrechannel-switch', 128),
            get_sw_telemetry(client, 'maps_policy', sw_ipaddress, secure_login, 'brocade-maps', 'maps-policy', 128),
            get_sw_telemetry(client, 'maps_config', sw_ipaddress, secure_login, 'brocade-maps', 'maps-config', 128),
            get_sw_telemetry(client, 'dashboard_rule', sw_ipaddress, secure_login, 'brocade-maps', 'dashboard-rule', 128),
            get_sw_telemetry(client, 'fc_interface', sw_ipaddress, secure_login, 'brocade-interface', 'fibrechannel', 128),
            get_sw_telemetry(client, 'fc_statistics', sw_ipaddress, secure_login, 'brocade-interface', 'fibrechannel-statistics', 128),
            get_sw_telemetry(client, 'media_rdp', sw_ipaddress, secure_login, 'brocade-media', 'media-rdp', 128),
            get_sw_telemetry(client, 'fdmi_hba', sw_ipaddress, secure_login, 'brocade-fdmi', 'hba', 128),
            get_sw_telemetry(client, 'fdmi_port', sw_ipaddress, secure_login, 'brocade-fdmi', 'port', 128),
            get_sw_telemetry(client, 'fc_nameserver', sw_ipaddress, secure_login, 'brocade-name-server', 'fibrechannel-name-server', 128),
            ]                    
        

        
        # vf_tasks.extend(current_vf_tasks)
        results = await asyncio.gather(*vf_tasks)
    # for result in results:
    #     if result.get('errors'):
    #         print(result['errors']['error'])
        
        
        
if __name__ == "__main__":
    # start = time.perf_counter()
    st = time.time()
    asyncio.run(extract_ch_unique_telemetry(sw_ipaddress='10.202.18.120'))
    end = time.perf_counter()
    # print(f"Time taken: {end - start:.2f} seconds.")
    elapsed_time = time.time() - st
    print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))


# chassis = {}
# fc_logical_switch = {}
# ts_timezone = {}
# fru_ps = {}
# fru_fan = {}
# fru_sensor = {}
# ssp_report = {}
# system_resources = {}
# sw_license = {}

# ch_unique_containers =[
#     [chassis, ('brocade-chassis', 'chassis')],
#     [fc_logical_switch, ('brocade-fibrechannel-logical-switch', 'fibrechannel-logical-switch')],
#     [ts_timezone, ('brocade-time', 'time-zone')],
#     [fru_ps, ('brocade-fru', 'power-supply')],
#     [fru_fan, ('brocade-fru', 'fan')],
#     [fru_sensor, ('brocade-fru', 'sensor')],
#     [ssp_report, ('brocade-maps', 'switch-status-policy-report')],
#     [system_resources, ('brocade-maps', 'system-resources')],
#     [sw_license, ('brocade-license', 'license')]
#     ]

# [print(module[0], module[1]) for _, module in ch_unique_containers]
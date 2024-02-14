# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 16:57:29 2024

@author: kavlasenko
"""


import os

script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
# from switch_telemetry_cls import BrocadeSwitchTelemetry



from typing import List, Dict, Union
from datetime import datetime

from switch_telemetry_httpx_cls import BrocadeSwitchTelemetry

class BrocadeRequestStatus:
    """
    Class used to create request status dictionaries for all http telemetry requests.


    
    Attributes:
        sw_telemetry: set of switch telemetry retrieved from the switch
        date: date sw_telemetry verification perpormed
        time: time sw_telemetry verification perpormed
        request_status: dictionary with request details for all http reqests in sw_telemetry
    """
    
    # dictionary with status ID for the corresponding status
    TELEMETRY_STATUS_ID = {'OK': 1,  
                           'WARNING': 2, 
                           'FAIL': 3}
    
    # errors which are considered to be OK status
    IGNORED_ERRORS = ['VF feature is not enabled', 
                      'No Rule violations found']
    
    
    def __init__(self, sw_telemetry: BrocadeSwitchTelemetry):
        """
        Args:
            sw_telemetry: set of switch telemetry retrieved from the switch
        """
        
        self._sw_telemetry = sw_telemetry
        self._date = datetime.now().strftime("%d/%m/%Y")
        self._time = datetime.now().strftime("%H:%M:%S")
        self._request_status = self._get_request_status()
        
        
    def _get_request_status(self) -> List[Dict[str, Union[str, int]]]:
        """
        Method verifies telemetry request status for all http requests.
        
        Returns:
            List of dictionaries.
            Dictionary contains details which telemetry is retrieved, vf_id if applicable, 
            datetime request was performed and its status.
        """
        
        # list to store request status dictionaries
        request_status_lst = []
        
        # check request status for the telemetry with url wo vf_id
        for current_sw_telemetry, (module, container) in self.sw_telemetry._ch_unique_containers:
            request_status_lst.append(
                BrocadeRequestStatus._create_status_dct(current_sw_telemetry, module, container)
                )
                
        # check request status for the telemetry with url w vf_id        
        for current_sw_telemetry, (module, container) in self.sw_telemetry._vf_unique_containers:
            for vf_id, current_vf_telemetry in current_sw_telemetry.items():
                request_status_lst.append(
                    BrocadeRequestStatus._create_status_dct(current_vf_telemetry, module, container, vf_id)
                    )
        return request_status_lst
    
    
    @staticmethod
    def _create_status_dct(telemetry_dct: Dict[str, Union[str, int]], 
                           module: str, container: str, 
                           vf_id=None) -> Dict[str, Union[str, int]]:
        """
        Method creates request status details dictionary of the request result telemetry_dct
        for the module and container name.
        
        
        Args:
            telemetry_dct: dictionary with request result
            module: requested module name
            container: requested container name
            vf_id: virtual ID used to perform telemetry reqest
            
        Returns:
            Request status dictionary for the request result telemetry_dct.
            Dictionary keys are module name, container name, retrieve datetime, status and vf_id.
        """
        
        # retrive values from the telemetry_dct
        request_keys = ['date', 'time', 'status-code', 'error-message', 'vf-id']
        telemetry_status_dct = {key: telemetry_dct.get(key) for key in request_keys}
        # add module name, container name and vf_id
        telemetry_status_dct['vf-id'] = vf_id
        telemetry_status_dct['module'] = module
        telemetry_status_dct['container'] = container
        # verify request status
        status = BrocadeRequestStatus._get_container_status(telemetry_dct)
        # add status and its id
        telemetry_status_dct['status'] = status
        telemetry_status_dct['status-id'] =  BrocadeRequestStatus.TELEMETRY_STATUS_ID[status]
        return telemetry_status_dct
    
    
    @staticmethod
    def _get_container_status(container: Dict[str, Union[str, int]]) -> str:
        """
        Method verifies request response, error-message and resonse status-code.
        
        Args:
            container: container with switch telemetry
            
        Returns:
            Response result status ('OK', 'WARNING', 'FAIL')
        """
    
        # if response contains non-empty data 
        if container.get('Response'):
            return 'OK'
        # if error-message is in the ignore list
        elif container.get('error-message') in BrocadeRequestStatus.IGNORED_ERRORS:
            return 'OK'
        elif container.get('status-code'):
            if container['status-code'] in [401]: # Unauthorized access
                return 'FAIL'
            else:
                return 'WARNING'
        else:
            return 'FAIL'
        
        
    def __repr__(self):
        return f"{self.__class__.__name__} ip_address: {self.sw_telemetry.sw_ipaddress}"
    
    

    @property
    def sw_telemetry(self):
        return self._sw_telemetry

                
    @property
    def date(self):
        return self._date
    
    
    @property
    def time(self):
        return self._time  
    
    
    @property
    def request_status(self):
        return self._request_status  


    
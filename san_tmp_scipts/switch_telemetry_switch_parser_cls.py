# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 13:36:35 2024

@author: kavlasenko
"""

import re
from typing import List, Dict, Union, Tuple
from collections import defaultdict





class BrocadeSwitchParser:
    
    
    

    
    
    FC_SWITCH_LEAF = ['name', 'domain-id', 'user-friendly-name', 'is-enabled-state', 
                      'up-time', 'principal', 'ip-address', 'model', 'firmware-version', 
                      'vf-id', 'fabric-user-friendly-name', 'ag-mode']
    
    FC_LOGICAL_SWITCH_LEAF = ['base-switch-enabled',  'default-switch-status',  
                              'fabric-id', 'logical-isl-enabled', 'port-member-list']
    
    FABRIC_SWITCH_LEAF = ['domain-id', 'fcid-hex', 'chassis-wwn', 'name', 'ip-address', 
                          'fcip-address', 'principal', 'chassis-user-friendly-name', 
                          'switch-user-friendly-name', 'path-count', 'firmware-version']



    
    
    def __init__(self, sw_telemetry):
        
        self._sw_telemetry = sw_telemetry
        self._fc_switch, self._vfid_name = self._get_fc_switch_value()
        self._fabric = self._get_fabric_switch_value()

        
    def _get_fc_switch_value(self) -> Tuple[Dict[str, Dict[str, Union[str, int]]], Dict[int, Dict[str, str]]]:
        """
        Function retrieves each virtual switch parameters and 
        information which swithes are in the fabric with the current vf_id switch
        from the fc_switch, fc_logical_switch and fabric_switch containers for each logical switch.
        
        Returns:
            Switch parameters. Dictionary of dictionaries.
            External dictionary keys are switch wwn.
            Nested dictionary keys are switch paramaeters names.
            If switch virtual fabric mode if disabled then logical switch related parameters are None.
            
            Vf_id details. Dictionary of dictionaries.
            External dictionary keys are logical switch vf-ids (if VF mode is disabled then vf-id is -1).
            Nested dictionary keys are fabric_name and switch_name with values for each switch vf-id .
            
            If maps configuration was not retrived the maps-policy, maps-actions contain error-message.
        """
        

        # dictonary with switch parameters
        fc_switch_dct = {}
        # dictonary with fabric_name and switch_name for each vf_id
        vfid_naming_dct = {}
        
        
        if self.sw_telemetry.fc_logical_switch.get('Response'):
            fc_logical_sw_container_lst = self.sw_telemetry.fc_logical_switch['Response']['fibrechannel-logical-switch']
        else:
            fc_logical_sw_container_lst = []
            
        # fc_logical_sw_num = len(fc_logical_sw_container_lst)
        
        # print(fc_logical_sw_container_lst)
            
        
        for vf_id, fc_switch_telemetry in self.sw_telemetry.fc_switch.items():
            if fc_switch_telemetry.get('Response'):
                fc_sw_container_lst = fc_switch_telemetry['Response']['fibrechannel-switch']
                

                    
                
                
                for fc_sw in fc_sw_container_lst:
                    # fos_version = fc_sw['firmware-version']
                    sw_wwn = fc_sw['name']
                    
                    vfid_naming_dct[vf_id] = {'switch-name': fc_sw['user-friendly-name'], 'fabric-name': fc_sw['fabric-user-friendly-name']}
                    current_sw_dct = {key: fc_sw[key] for key in BrocadeSwitchParser.FC_SWITCH_LEAF}
                    if current_sw_dct.get('ip-address'):
                        current_sw_dct['ip-address'] =  ', '.join(current_sw_dct['ip-address']['ip-address'])
                    if fc_logical_sw_container_lst:
                        
                        for fc_logical_sw in fc_logical_sw_container_lst:
                            
                            if sw_wwn == fc_logical_sw['switch-wwn']:
                                current_logical_sw_dct = {key: fc_logical_sw[key] for key in BrocadeSwitchParser.FC_LOGICAL_SWITCH_LEAF}
                                current_logical_sw_dct['port-member-list'] = current_logical_sw_dct['port-member-list']['port-member']
                                
                                current_logical_sw_dct['port-member-quantity'] = len(fc_logical_sw['port-member-list']['port-member'])
                                current_sw_dct.update(current_logical_sw_dct)
                    
                        
                    if fc_sw['vf-id'] == -1 and not self.sw_telemetry.fc_interface.get('error'):
                        current_sw_dct['port-member-quantity'] = len(self.sw_telemetry.fc_interface[vf_id]['Response']['fibrechannel'])
                        
                    none_dct = {key: current_sw_dct.get(key) for key in BrocadeSwitchParser.FC_LOGICAL_SWITCH_LEAF if not current_sw_dct.get(key)}
                    current_sw_dct.update(none_dct)
                        
                    fc_switch_dct[sw_wwn] = current_sw_dct
                    
            elif fc_switch_telemetry.get('error-message') and fc_switch_telemetry.get('status-code'):
                current_sw_dct = {key: None for key in BrocadeSwitchParser.FC_SWITCH_LEAF + BrocadeSwitchParser.FC_LOGICAL_SWITCH_LEAF}
                current_sw_dct['user-friendly-name'] = fc_switch_telemetry.get('error-message')
                fc_switch_dct['unknown'] = current_sw_dct
                vfid_naming_dct[vf_id] = {'switch-name': None, 'fabric-name': None}
                
                
                    
        return fc_switch_dct, vfid_naming_dct
    
    
    def _get_fabric_switch_value(self):
        
        
        fabric_dct = {}

        
        for vf_id, fabric_telemetry in self.sw_telemetry.fabric_switch.items():
            if fabric_telemetry.get('Response'):
                fabric_container_lst = fabric_telemetry['Response']['fabric-switch']
                current_fabric_lst = []
                # print(vf_id, fabric_name)
                
                # print(vf_id, fabric_container)
                for fc_sw in fabric_container_lst:
                    current_sw_dct = {key: fc_sw[key] for key in BrocadeSwitchParser.FABRIC_SWITCH_LEAF}
                    current_sw_dct['vf-id'] = vf_id
                    current_sw_dct['fabric-name'] = self.vfid_name[vf_id]['fabric-name']
                    current_fabric_lst.append(current_sw_dct)
                    fabric_dct[vf_id] = current_fabric_lst
            elif fabric_telemetry.get('error-message') and fabric_telemetry.get('status-code'):

                current_sw_dct = {key: '' for key in BrocadeSwitchParser.FABRIC_SWITCH_LEAF}
                current_sw_dct['vf-id'] = vf_id
                current_sw_dct['fabric-name'] = ''
                current_sw_dct['name'] = fabric_telemetry['error-message']
                fabric_dct[vf_id] = [current_sw_dct]
        return fabric_dct
    
    
    
    def __repr__(self):

        return f"{self.__class__.__name__} ip_address: {self.sw_telemetry.sw_ipaddress}"
        


    @property
    def sw_telemetry(self):
        return self._sw_telemetry
    
    
    @property
    def fc_switch(self):
        return self._fc_switch
    
    
    @property
    def vfid_name(self):
        return self._vfid_name
    
    @property
    def fabric(self):
        return self._fabric
    
    # @property
    # def ssp_report(self):
    #     return self._ssp_report
    
    # @property
    # def system_resources(self):
    #     return self._system_resources
    
    # @property
    # def dashboard_rule(self):
    #     return self._dashboard_rule
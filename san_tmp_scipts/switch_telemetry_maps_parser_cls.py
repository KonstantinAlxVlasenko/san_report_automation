# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 17:35:13 2024

@author: kavlasenko
"""

import re
from typing import List, Dict, Union
from collections import defaultdict


class BrocadeMAPSParser:
    
    
    
    SSP_LEAFS = ['switch-health', 
                 'power-supply-health', 'fan-health', 'temperature-sensor-health', 
                 'flash-health', 
                 'marginal-port-health', 'faulty-port-health', 'missing-sfp-health', 'error-port-health', 
                 'expired-certificate-health', 'airflow-mismatch-health']
    
    _ssp_state = {'healthy': 1,  
                 'unknown': 2, 
                 'marginal': 3, 
                 'down': 4}
    
    SSP_STATE = defaultdict(lambda: BrocadeMAPSParser._ssp_state['unknown'])
    SSP_STATE.update(_ssp_state)
    
    
    DB_RULE_LEAFS = ['category', 'name', 'triggered-count',  'time-stamp', 'repetition-count', 'object-element', 'object-value', 'severity']
    # DB_RULE_OBJECT_KEYS = []
    DB_RULE_IGNORE = ['CLEAR', 'UNQUAR', 'STATE_IN', 'STATE_ON', 'STATE_UP', 'BALANCED']

    
    
    def __init__(self, sw_telemetry):
        
        self._sw_telemetry = sw_telemetry
        # self._fru_ps = self._get_ps_leaf_value()
        # self._fru_fan = self._get_fan_leaf_value()
        # self._ps = self._get_fru_leaf_value(fru_type=FRUType.ps)
        # self._fan = self._get_fru_leaf_value(fru_type=FRUType.fan)
        self._maps_config = self._get_maps_config_value()
        self._ssp_report = self._get_ssp_report_value()
        self._system_resources = self._get_system_resource_values()
        self._dashboard_rule = self._get_dashboard_rule_value()
        
        
    def _get_maps_config_value(self) -> Dict[int, Union[str, int]]:
        """
        Function retrieves active maps policy and maps actions
        from the maps_policy and maps_config containers for each logical switch.
        
        Returns:
            Dictionary of dictionaries.
            External dictionary keys are logical switch vf-ids (if VF mode is disabled then vf-id is -1).
            Nested dictionary keys are vf-id, maps-policy, maps-actions.
            If maps configuration was not retrived the maps-policy, maps-actions contain error-message.
        """
        

        maps_config_dct = {}
        # parsing maps-policy for each logical switch
        for vf_id, maps_policy_telemetry in self.sw_telemetry.maps_policy.items():
            if maps_policy_telemetry.get('Response'):
                # check all maps policies and find single active policy
                active_policy = [vf_maps_policy['name'] 
                                 for vf_maps_policy in maps_policy_telemetry['Response']['maps-policy'] 
                                 if vf_maps_policy['is-active-policy']]
                active_policy = active_policy[0] if active_policy else None
                
                # maps_config_dct[vf_id] = {'vf-id': vf_id, 'maps-policy': active_policy}
            else:
                active_policy = maps_policy_telemetry['error-message']
            # add active policy or error-message dictionary to the maps configuration dictionary
            maps_config_dct[vf_id] = {'vf-id': vf_id, 'maps-policy': active_policy}
         
        # parsing maps-actions for each logical switch
        for vf_id, maps_config_telemetry in self.sw_telemetry.maps_config.items():
            if maps_config_telemetry.get('Response'):
                maps_actions = maps_config_telemetry['Response']['maps-config']['actions']['action']
                maps_actions = ', '.join(maps_actions)
            else:
                maps_actions = maps_config_telemetry['Response']['error-message']
            # create empty nested dictionary if logical switch is not in the maps configuration dictionary
            if not vf_id in maps_config_dct:
                maps_config_dct[vf_id] = {}
            # update maps configuration dictionary for the current vf-id with the its maps-actions
            maps_config_dct[vf_id]['maps-actions'] = maps_actions
        return maps_config_dct
    
    
    
    def _get_ssp_report_value(self) -> List[Dict[str, Union[str, int]]]:
        """
        Function extracts Switch Status Policy report values from the ssp_report container.
        The SSP report provides the overall health status of the switch.
        
        Returns:
            List of dictionaries.
            Dictionary keys are object name, its operational status and status id.
            If ssp_report container was not retrived from the switch ssp_report contain error-message
            for each object name.
        """        
        
        ssp_report_lst = []
        
        if self.sw_telemetry.ssp_report.get('Response'):
            container = self.sw_telemetry.ssp_report['Response']['switch-status-policy-report']
            for ssp_leaf in BrocadeMAPSParser.SSP_LEAFS:
                state = container[ssp_leaf]
                ssp_report_lst.append({'name': ssp_leaf,
                                       'operationa-state': state.upper(),
                                       'operational-state-id': BrocadeMAPSParser.SSP_STATE[state]})
        else:
            for ssp_leaf in BrocadeMAPSParser.SSP_LEAFS:
                error = self.sw_telemetry.ssp_report['error-message']
                error = " (" + error + ")" if error else ''
                state = 'unknown'
                ssp_report_lst.append({'name': ssp_leaf,
                                       'operationa-state': state.upper() + error,
                                       'operational-state-id': BrocadeMAPSParser.SSP_STATE[state]})
        return ssp_report_lst
    
    
    def _get_system_resource_values(self) -> Dict[str, Union[int, str]]:
        """
        Function extracts system resources (such as CPU, RAM, and flash memory usage) values from the system_resources container.
        Note that usage is not real time and may be delayed up to 2 minutes.
        
        Returns:
            Dictionary with system resource name as keys and its usage as values.
            If system_resources container was not retrived from the switch result dictionary contains error-message.
        """        
        
        if self.sw_telemetry.system_resources.get('Response'):
            system_resources_dct = self.sw_telemetry.system_resources['Response']['system-resources'].copy()
        else:
            system_resources_dct = {'cpu-usage': -1,
                              'flash-usage': -1,
                              'memory-usage': -1,
                              'total-memory': -1}
        system_resources_dct['error-message'] = self.sw_telemetry.system_resources['error-message']
        return system_resources_dct
                
             
    def _get_dashboard_rule_value(self) -> Dict[int, List[Dict[str, Union[str, int]]]]:
        """
        Function retrieves the MAPS events or rules triggered and the objects on which the rules were triggered 
        over a specified period of time for each logical switch.
        
        Severity level:
            0 - no event triggired or retrieved
            1 - information that event condition is cleared 
            2 - warning that event condition detected
            
        Virtual Fabric ID:
            -1: VF mode is disabled
            -2: VF mode is unknown (chassis container was not retrieved)
            -3: VF IDs rae unknown (fc_logical_switch container was not retrieved)
            1-128: VF mode is enabled
        
        Returns:
            Dictionary of lists.
            External dictionary keys are logical switch vf-ids (if VF mode is disabled then vf-id is -1).
            Nested lists contain dictionaries. 
            Dictionary keys are category, rule name, time-stamp, triggered times, object, severity etc of the event.
            If dashboard rules were not retrieved or no events were triggered then
            nested list contains single dictionary with the error-message.
        """
        

        dashboard_rule_dct = {}
        # parsing triggered events for for each logical switch
        for vf_id, dashboard_rule_telemetry in self.sw_telemetry.dashboard_rule.items():
            # list of the triggered events for the current logical switch with vf_id
            dashboard_rule_dct[vf_id] = []
            if dashboard_rule_telemetry.get('Response'):
                # list of dictionaries. each dictionary is the triggered event
                container = dashboard_rule_telemetry['Response']['dashboard-rule']
                # cheking each event
                for db_rule in container:
                    db_rule_name = db_rule['name']
                    # check if rule name contains any event from the ignored list (means that event condition is cleared)
                    db_rule_ignore_flag = any([bool(re.search(f'.+?_{ignored_pattern}$', db_rule_name)) 
                                               for ignored_pattern in BrocadeMAPSParser.DB_RULE_IGNORE])
                    # event severity level (if event is in the ignored group then severity is 1 otherwise 2)
                    db_rule_severity = 1 if db_rule_ignore_flag else 2
                    # create dictionary containing triggered event details
                    current_db_rule_dct = {leaf: db_rule.get(leaf) for leaf in BrocadeMAPSParser.DB_RULE_LEAFS}
                    current_db_rule_dct['severity'] = db_rule_severity
                    print(db_rule_name, db_rule_ignore_flag)
                    # triggered event might containt single or miltiple objects that violated the rule (ports for example)
                    for object_item in db_rule['objects']['object']:
                        # the object format is as follows: <element>:<value>
                        # for example, 'F-Port 10:90'
                        object_element, object_value = object_item.split(':')
                        current_db_rule_dct['object-element'] = object_element
                        current_db_rule_dct['object-value'] = object_value
                        # add event to the list for the each object
                        dashboard_rule_dct[vf_id].append(current_db_rule_dct)
                        print(object_element, object_value)
            else:
                # if dashboard rules were not retrieved or no events were triggered add error-message
                # to the event dictionary with severity 0
                current_db_rule_dct = {leaf: '' for leaf in BrocadeMAPSParser.DB_RULE_LEAFS}
                error_msg = dashboard_rule_telemetry['error-message']
                current_db_rule_dct['name'] = error_msg
                current_db_rule_dct['category'] = error_msg
                current_db_rule_dct['severity'] = 0
                dashboard_rule_dct[vf_id].append(current_db_rule_dct)
        return dashboard_rule_dct


    @property
    def sw_telemetry(self):
        return self._sw_telemetry
    
    @property
    def maps_config(self):
        return self._maps_config
    
    @property
    def ssp_report(self):
        return self._ssp_report
    
    @property
    def system_resources(self):
        return self._system_resources
    
    @property
    def dashboard_rule(self):
        return self._dashboard_rule
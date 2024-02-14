# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 17:45:30 2024

@author: kavlasenko
"""


from enum import Enum

class FRUType(Enum):
    ps = "power-supply"
    fan = "fan"



class BrocadeFRUParser:
    
    
    PS_STATE = {'absent': 0,
                'ok': 1,
                'predicting failure': 2, 
                'unknown': 3,
                'try reseating unit': 4,
                'faulty': 5}
    
    
    FAN_STATE = {'absent': 0, 
                 'ok': 1, 
                 'below minimum': 2, 
                 'above maximum': 3, 
                 'unknown': 4, 
                 'not ok': 5,
                 'faulty': 6}

    
    
    def __init__(self, sw_telemetry):
        
        self._sw_telemetry = sw_telemetry
        self._fru_ps = self._get_ps_leaf_value()
        self._fru_fan = self._get_fan_leaf_value()
        # self._ps = self._get_fru_leaf_value(fru_type=FRUType.ps)
        # self._fan = self._get_fru_leaf_value(fru_type=FRUType.fan)
        
        
    def _get_ps_leaf_value(self):
        """
        Function extracts leaf values from the FRU Power Supply container.
        
        Returns:
            List of PS [ps id, ps operational state, ps state id] 
            or error message
        """
        
        ps_lst = []

        if self.sw_telemetry.fru_ps.get('Response'):
            container = self.sw_telemetry.fru_ps['Response']['power-supply']
            for ps in container:
                print(ps['unit-number'], ps['operational-state'])
                ps_id = 'Power Supply #' + str(ps['unit-number'])
                ps_state = ps['operational-state']
                # ps_lst.append([ps_id, ps_state.upper(), BrocadeFRUParser.PS_STATE.get(ps_state)])
                ps_lst.append({'unit-number': ps_id, 
                               'operational-state': ps_state.upper(), 
                               'operational-state-id': BrocadeFRUParser.PS_STATE.get(ps_state)})
        else:
            ps_id = self.sw_telemetry.fru_ps['error-message']
            ps_state = 'unknown'
            # ps_lst.append([ps_id, ps_state.upper(), BrocadeFRUParser.PS_STATE.get(ps_state)])
            ps_lst.append({'unit-number': ps_id, 
                           'operational-state': ps_state.upper(), 
                           'operational-state-id': BrocadeFRUParser.PS_STATE.get(ps_state)})

        return ps_lst
        


    def _get_fan_leaf_value(self) -> list:
        """
        Function extracts leaf values from the FRU FAN container.
        
        Returns:
            List of FAN [fan id, fan airflow, fan operational state, fan state id, fan speed] 
            or error message
        """
        
        
        fan_lst = []
        if self.sw_telemetry.fru_fan.get('Response'):
            container = self.sw_telemetry.fru_fan['Response']['fan']
            for fan in container:
                print(fan['unit-number'], fan['speed'], fan['operational-state'])
                fan_id = 'Fan #' + str(fan['unit-number'])
                fan_airflow = fan['airflow-direction']
                fan_state = fan['operational-state']
                fan_speed = fan['speed']
                # fan_lst.append([fan_id, fan_airflow, fan_state.upper(), BrocadeFRUParser.FAN_STATE.get(fan_state), fan_speed])
                fan_lst.append({'unit-number': fan_id, 
                                'airflow-direction': fan_airflow, 
                                'operational-state': fan_state.upper(), 
                                'operational-state-id': BrocadeFRUParser.FAN_STATE.get(fan_state), 
                                'speed': fan_speed})
                
                
        else:
            fan_id = self.sw_telemetry.fru_fan['error-message']
            fan_state = 'unknown'
            # fan_lst.append([fan_id, 'Not applicable', fan_state.upper(), BrocadeFRUParser.FAN_STATE.get(fan_state), 'Not applicable'])
            
            fan_lst.append({'unit-number': fan_id, 
                            'airflow-direction': 'Not applicable', 
                            'operational-state': fan_state.upper(), 
                            'operational-state-id': BrocadeFRUParser.FAN_STATE.get(fan_state), 
                            'speed': 'Not applicable'})
        return fan_lst




    # def _get_fru_leaf_value(self, fru_type: FRUType) -> list:
        
        
    #     fru_lst = []
    #     # print(fru_type)
    #     # print(fru_type.name)
        
        
        
    #     fru_value = fru_type.value
        
    #     if fru_value == 'power-supply':
    #         container = self.sw_telemetry.ps
    #         fru_title = 'Power Supply'
    #     elif fru_value == 'fan':
    #         container = self.sw_telemetry.fan
    #         fru_title = 'Fan'
        
    #     if container.get('Response'):
            

            
            
    #         for fru in container['Response'][fru_value]:
    #             print(fru['unit-number'], fru.get('speed'), fru['operational-state'])
    #             fru_id = fru_title + ' #' + str(fru['unit-number'])
    #             fru_state = fru['operational-state']
    #             if fru_value == 'power-supply':
    #                 fru_lst.append([fru_id, fru_state.upper(), BrocadeFRUParser.PS_STATE.get(fru_state)])
    #             elif fru_value == 'fan':
    #                 fan_airflow = fru['airflow-direction']
    #                 fan_speed = fru['speed']
    #                 fru_lst.append([fru_id, fan_airflow, fru_state.upper(), BrocadeFRUParser.FAN_STATE.get(fru_state), fan_speed])
    #     else:
    #         fru_id = container['error-message']
    #         fru_state = 'unknown'
    #         if fru_value == 'power-supply':
    #             fru_lst.append([fru_id, fru_state.upper(), BrocadeFRUParser.PS_STATE.get(fru_state)])
    #         elif fru_value == 'fan':
            
    #             fru_lst.append([fru_id, 'Not applicable', fru_state.upper(), BrocadeFRUParser.FAN_STATE.get(fru_state), 'Not applicable'])
    #     return fru_lst





    @property
    def sw_telemetry(self):
        return self._sw_telemetry    


    @property
    def fru_ps(self):
        return self._fru_ps
    

    @property
    def fru_fan(self):
        return self._fru_fan
    
    
    # @property
    # def (self):
    #     return self._
    
    
    # @property
    # def (self):
    #     return self._
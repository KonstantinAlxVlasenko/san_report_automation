# -*- coding: utf-8 -*-
"""
Created on Sat Jan 27 12:58:09 2024

@author: kavlasenko
"""

class BrocadeChassisParser:
    
    
    def __init__(self, sw_telemetry):
        
        self._sw_telemetry = sw_telemetry
        self._ch_name = self.ch_name(sw_telemetry)
    
    
    @property
    def sw_telemetry(self):
        return self._sw_telemetry
    
    @property
    def ch_name(self):
        ch_name = self.sw_telemetry.chassis['Response']['chassis']['chassis-user-friendly-name']
        return ch_name
    
    # current_chassis = sw_telemetry.chassis['Response']['chassis']
    # ch_name = current_chassis['chassis-user-friendly-name']
    # if current_chassis['vendor-serial-number']:
    #     sw_sn = current_chassis['vendor-serial-number']
    # else:
    #     switch_sn = current_chassis['serial-number']
    # sw_model = 'Brocade ' + current_chassis['product-name'].capitalize()
    # sw_datetime = current_chassis['date']
    # ch_wwn = current_chassis['chassis-wwn']
    

    # @property
    # def sw_ipaddress(self):
    #     return str(self._sw_ipaddress)
    
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 15:19:38 2024

@author: kavlasenko
"""

from datetime import datetime, date
from typing import List, Dict, Union


class BrocadeChassisParser:
    

    CHASSIS_LEAFS = ['chassis-user-friendly-name', 'chassis-wwn', 'serial-number', 
                     'vendor-serial-number', 'product-name', 'date', 'vf-enabled', 'vf-supported', 'max-blades-supported']
    
    
    def __init__(self, sw_telemetry):
        
        self._sw_telemetry = sw_telemetry
        self._chassis = self._get_chassis_value()
        if self.chassis:
            self._get_switch_value()
            self._get_vf_mode()
            self._get_timezone_value()
        # self._ch_name = self._get_ch_leaf_value(leaf_name='chassis-user-friendly-name')
        # self._ch_wwn = self._get_ch_leaf_value(leaf_name='chassis-wwn')
        # self._sw_factory_sn = self._get_ch_leaf_value(leaf_name='serial-number')
        # self._sw_vendor_sn = self._get_ch_leaf_value(leaf_name='vendor-serial-number')
        # self._sw_model = self._get_ch_leaf_value(leaf_name='product-name')
        # self._sw_datetime = self._get_ch_leaf_value(leaf_name='date')
        # self._vf_enabled = self._get_ch_leaf_value(leaf_name='vf-enabled')
        # if self.sw_telemetry.chassis.get('Response'):
        #     self.sw_sn = self.sw_vendor_sn if self.sw_vendor_sn else self.sw_factory_sn
        #     self._sw_model = 'Brocade ' + self._sw_model.capitalize()
        # else:
        #     self.sw_sn = self.sw_vendor_sn
        self._sw_license = self._get_lic_leaf_value()
        # self._ts_timezone = self._get_timezone_leaf()
        

    def _get_chassis_value(self) -> Dict[str, Union[str, int]]:
        
        
        
        if self.sw_telemetry.chassis.get('Response'):
            container = self.sw_telemetry.chassis['Response']['chassis']
            chassis_dct = {key: container[key] for key in BrocadeChassisParser.CHASSIS_LEAFS}
            if chassis_dct.get('product-name'):
                chassis_dct['product-name'] = 'Brocade ' + chassis_dct['product-name'].capitalize()
            chassis_dct['switch-serial-number'] = \
                chassis_dct['vendor-serial-number'] if chassis_dct.get('vendor-serial-number') else chassis_dct.get('serial-number')
        else:
            chassis_dct = {}
            
        return chassis_dct


    def _get_vf_mode(self):
        
        if self.chassis['vf-supported'] is False:
            self.chassis['virtual-fabrics'] = 'Not Applicable'
            self.chassis['ls-number'] = 0
        else: 
            if self.chassis['vf-enabled'] is True:
                self.chassis['virtual-fabrics'] = 'Enabled'
            elif self.chassis['vf-enabled'] is False:
                self.chassis['virtual-fabrics'] = 'Disabled'
                self.chassis['ls-number'] = 0
            else:
                self.chassis['virtual-fabrics'] = None


    def _get_switch_value(self):


        for fc_switch_telemetry in self.sw_telemetry.fc_switch.values():
            if fc_switch_telemetry.get('Response'):
                
                fc_sw_container_lst = fc_switch_telemetry['Response']['fibrechannel-switch']
                
                for fc_sw in fc_sw_container_lst:
                    self.chassis['firmware-version'] = fc_sw['firmware-version']
                    self.chassis['model'] = fc_sw['model']
        self.chassis['ls-number'] = len(self.sw_telemetry.fc_switch.items())
                    


       
        
    def _get_lic_leaf_value(self) -> List[Dict[str, Union[str, int]]]:
        """
        Function extracts leaf values from the licnense container.
        
        License status:
            0 - No expiration date
            1 - Expiration date has not arrived
            2 - Expiration date has arrived
        
        Returns:
            List of licenses [license title, expiration date, license status (expired or not)] 
            or error message
        """
        
        license_feature_lst = []
        if self.sw_telemetry.sw_license['Response']:
            container = self.sw_telemetry.sw_license['Response']['license']
            for lic_leaf in container:
                lic_feature = ', '.join(lic_leaf['features']['feature'])
                
                if lic_leaf.get('expiration-date'):
                    exp_date_str = lic_leaf['expiration-date']
                    exp_date = datetime.strptime(exp_date_str, '%m/%d/%Y').date()
                    if date.today() > exp_date:
                        lic_status = 2
                    else:
                        lic_status = 1
                else:        
                    lic_status = 0
                    exp_date_str = 'No expiration date'
                # license_feature_lst.append([lic_feature, exp_date_str, lic_status])
                license_feature_lst.append({'feature': lic_feature, 
                                            'expiration-date': exp_date_str, 
                                            'license-status-id': lic_status})
                
                
        else:
            lic_feature = self.sw_telemetry.sw_license['error-message']
            # license_feature_lst.append([lic_feature, 'Not applicable', 0])
            license_feature_lst.append({'feature': lic_feature, 
                                        'expiration-date': exp_date_str, 
                                        'license-status-id': lic_status})
            
            
        return license_feature_lst
            
    
    def _get_timezone_value(self) -> str:
        # find time-zone
        
        if self.sw_telemetry.ts_timezone['Response']:
            container = self.sw_telemetry.ts_timezone['Response']['time-zone']
        
            if container.get('name'):
                ts_tz = container.get('name')
            elif container.get('gmt-offset-hours') is not None \
                and container.get('gmt-offset-minutes') is not None:
                    ts_tz = str(container.get('gmt-offset-hours')) \
                        + ':' + str(container.get('gmt-offset-minutes'))
            else:
                ts_tz = 'unknown'
        else:
            ts_tz = None
        
        self.chassis['time-zone'] = ts_tz
        
    
    @property
    def sw_telemetry(self):
        return self._sw_telemetry
    
    
    @property
    def chassis(self):
        return self._chassis
    
    # @property
    # def ch_name(self):
    #     return self._ch_name


    # @property
    # def ch_wwn(self):
    #     return self._ch_wwn


    # @property
    # def sw_factory_sn(self):
    #     return self._sw_factory_sn
    
    
    # @property
    # def sw_vendor_sn(self):
    #     return self._sw_vendor_sn
    
    
    # @property
    # def sw_model(self):
    #     return self._sw_model
    
    
    # @property
    # def sw_datetime(self):
    #     return self._sw_datetime
    
    
    # @property
    # def vf_enabled(self):
    #     return self._vf_enabled
    
    
    @property
    def sw_license(self):
        return self._sw_license
    
    
    @property
    def ts_timezone(self):
        return self._ts_timezone
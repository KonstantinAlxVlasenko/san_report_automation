import utilities.dataframe_operations as dfop
import pandas as pd

def find_enclosure_pair_switch(switch_pair_df):
    """Function to find pair switches in single enclosure"""
    
    enclosure_fabric_columns = ['Fabric_name', 'Fabric_label', 'Enclosure']
    enclosure_switch_columns = ['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']
    
    if switch_pair_df['Device_Location'].isna().all():
        return switch_pair_df
    
    # extract enclosure name
    switch_pair_df['Enclosure'] = switch_pair_df['Device_Location'].str.extract(r'^Enclosure (.+) bay')
    fabric_labels_lst = switch_pair_df['Fabric_label'].unique().tolist()
    switch_pair_enclosure_filled_total_df = pd.DataFrame()
    
    for fabric_label in fabric_labels_lst:
        
        mask_fabric_label = switch_pair_df['Fabric_label'] == fabric_label
        # switches for the all fabric_labels except the one which are being checked for paired switches
        switch_pair_location_df = switch_pair_df.loc[~mask_fabric_label].copy()
        # join all switchName and switchWwn for the switches in the verified fabric labels and merge both DataFrames
        switch_pair_location_name_df = switch_pair_location_df.groupby(by=['Fabric_name', 'Enclosure'])['switchName'].agg(lambda x: ', '.join(x)).to_frame()
        switch_pair_location_wwn_df = switch_pair_location_df.groupby(by=['Fabric_name', 'Enclosure'])['switchWwn'].agg(lambda x: ', '.join(x)).to_frame()
        switch_pair_location_df = switch_pair_location_name_df.merge(switch_pair_location_wwn_df, how='left', left_index=True, right_index=True)
        # switchName and switchWwn columns in verified fabric labels renamed to pair switchName and pair switchWwn 
        switch_pair_location_df.rename(columns={'switchName': 'switchName_pair_in_enclosure', 'switchWwn': 'switchWwn_pair_in_enclosure'}, inplace=True)
        switch_pair_location_df.reset_index(inplace=True)
        # Fabric_label assigned to value of fabric label being checked for pair switches
        switch_pair_location_df['Fabric_label'] = fabric_label
        # switchpair DataFrame for fabric label being checked
        switch_pair_enclosure_filled_current_df = switch_pair_df.loc[mask_fabric_label]
        # add switchName and switchWwn of the paired switches
        switch_pair_enclosure_filled_current_df = dfop.dataframe_fillna(switch_pair_enclosure_filled_current_df, switch_pair_location_df, join_lst=enclosure_fabric_columns, filled_lst=enclosure_switch_columns)
        # concatenate switchPair DataFrames for all fabric labels
        if switch_pair_enclosure_filled_total_df.empty:
            switch_pair_enclosure_filled_total_df = switch_pair_enclosure_filled_current_df.copy()
        else:
            switch_pair_enclosure_filled_total_df = pd.concat([switch_pair_enclosure_filled_total_df, switch_pair_enclosure_filled_current_df])
    # add switchName and switchWwn of the paired switches to the final result DataFrame
    switch_pair_df = dfop.dataframe_fillna(switch_pair_df, switch_pair_enclosure_filled_total_df, join_lst=enclosure_fabric_columns, filled_lst=enclosure_switch_columns)
    return switch_pair_df
    
    
def find_zero_device_connected_enclosure_sw_pair(switch_pair_df):
    """Function to add enclosure switch pair for switches with no connected devices"""
    
    if dfop.verify_columns_in_dataframe(switch_pair_df, columns=['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']):
    
        mask_zero_device_connected = switch_pair_df['Connected_device_number'] == 0
        mask_sw_pair_empty = switch_pair_df[['switchName_pair', 'switchWwn_pair']].isna().all(axis=1)
        mask_enclosure_sw_notna = switch_pair_df[['switchName_pair_in_enclosure', 'switchWwn_pair_in_enclosure']].notna().all(axis=1)
        
        # fill switchName, switchWwn and pairing type for switches with no device connected and which are in Blade or Synergy enclosures
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'switchName_pair'] = \
            switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'switchName_pair_in_enclosure']
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'switchWwn_pair'] = \
            switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'switchWwn_pair_in_enclosure']
        switch_pair_df.loc[mask_zero_device_connected & mask_sw_pair_empty & mask_enclosure_sw_notna, 'Switch_pairing_type'] = 'enclosure'
    return switch_pair_df
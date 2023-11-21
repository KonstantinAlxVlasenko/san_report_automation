"""Module to add notes to storage connection statistics based on connection details in DataFrame"""

import utilities.dataframe_operations as dfop

def add_notes(storage_connection_statistics_df, storage_ports_df):
    """Function to add notes to storage_connection_statistics_df DataFrame"""

    def verify_connection_symmetry(series, fabric_names_lst):
        """Function to check if ports are connected symmetrically in each Fabric_name and across san.
        Symmetrically connected storage means that each fabric in san have same amount of ports connected. 
        Ports are grouped based on the levels: 
        storage, controllers, slots across each controller, slots across storage"""

        # Fabric_names where storage port connection is unsymmtrical (port quantity differs across the Fabric_labels)
        unsymmetric_fabric_name_lst = []
        # Fabric_names where storage ports are connected to the single fabric_label (nonredundant connection)
        single_fabric_name_lst = []
        # list of Fabric_names where storage is connected at least to one of the Fabric_labels
        detected_fabric_name_lst = []
        # list of Fabrics (concatenation of Fabric_name and Fabric_label) where connection is detected to any
        # of the Fabric_labels of Fabric_name. To be symmetrically connected across san all ports quantity 
        # values have to be equal in all Fabrics of this list
        symmetric_fabric_lst = []
        unsymmetric_fabric_str = ''
        unsymmetric_san_str = ''
        single_fabric_str = ''
        
        if series['Group_type'] in ['storage', 'controller', 'controller_slot', 'slot']:
            for fabric_name in fabric_names_lst:
                # find all fabrics in current fabric_name
                existing_fabric_columns = [column for column in series.index if fabric_name in column]
                # find fabrics to which storage is connected in current fabric_name
                connected_fabric_columns = [column for column in existing_fabric_columns if series[column] != 0]
                # if storage is connected to any of the fabrics in current fabric_name
                if connected_fabric_columns:
                    detected_fabric_name_lst.append(fabric_name)
                    symmetric_fabric_lst.extend(existing_fabric_columns)
                    # if storage is connected to less fabrics then there is in current fabric_name
                    # or if storage connected to all fabrics but ports quantity differs
                    if connected_fabric_columns != existing_fabric_columns:
                        single_fabric_name_lst.append(fabric_name)
                    elif series[connected_fabric_columns].nunique() != 1:
                        unsymmetric_fabric_name_lst.append(fabric_name)

            # if asymetric fabric_names was found
            if unsymmetric_fabric_name_lst:
                unsymmetric_fabric_str = 'unsymmetric_connection'
                # add asymetic fabric_names if storage connected to more then one fabric_names
                if len(detected_fabric_name_lst) > 1:
                    unsymmetric_fabric_str = unsymmetric_fabric_str + ' in ' + ', '.join(unsymmetric_fabric_name_lst)
            # if single fabric_label connected fabric_names was found
            if single_fabric_name_lst:
                single_fabric_str = 'single_fabric_connection'
                # add nonredundant fabric_names if storage connected to more then one fabric_names
                if len(detected_fabric_name_lst) > 1:
                    single_fabric_str = single_fabric_str + ' in ' + ', '.join(single_fabric_name_lst) 
            # if storage connected to more than one fabric_names and 
            # port quantity is not equal across all fabrics which storage is connected to (san)
            if len(detected_fabric_name_lst) > 1 and series[symmetric_fabric_lst].nunique() != 1:
                unsymmetric_san_str =  'unsymmetric_san_connection'
            # join fabric and san unsymmetrical notes
            notes = [unsymmetric_fabric_str, single_fabric_str, unsymmetric_san_str]
            if any(notes):
                notes = [note for note in notes if note]
                return ', '.join(notes)
            

    def verify_port_parity(series, fabric_names_lst):
        """Function to check if ports with odd and even indexes are connected to single
        Fabric_label in Fabric_name"""
        
        # list of Fabric_names where storage is connected at least to one of the Fabric_labels
        detected_fabric_names_lst = []
        # list of Fabric_names where port parity is not observed
        broken_parity_fabric_name_lst = []
        broken_parity_fabric_str = ''
        
        if series['Group_type'] in ['port_parity']:
            for fabric_name in fabric_names_lst:
                # find all fabrics in current fabric_name
                existing_fabric_columns = [column for column in series.index if fabric_name in column]
                # find fabrics to which storage is connected in current fabric_name
                connected_fabric_columns = [column for column in existing_fabric_columns if series[column] != 0]
                # if storage is connected to any of the fabrics in current fabric_name
                if connected_fabric_columns:
                    detected_fabric_names_lst.append(fabric_name)
                    # port parity group is connected to more then one fabrics in the current fabric_name
                    if len(connected_fabric_columns) != 1:
                        broken_parity_fabric_name_lst.append(fabric_name)
            
            if broken_parity_fabric_name_lst:
                broken_parity_fabric_str = 'multiple fabrics connection'
                # add broken port parity fabric_names if storage connected to more then one fabric_names
                if len(detected_fabric_names_lst) > 1:
                    broken_parity_fabric_str = broken_parity_fabric_str + ' in ' + ', '.join(broken_parity_fabric_name_lst)
                return broken_parity_fabric_str

    
    def verify_virtual_port_login(storage_connection_statistics_df):
        """Function to verify if virtual port(s) exist behind storage physical port"""

        storage_connection_statistics_df['Physical_virtual_unique_quantity'] = \
            storage_connection_statistics_df.groupby(by=['deviceSubtype', 'Device_Host_Name', 'Group_type', 'Group_level'])['All'].transform('nunique')
        
        mask_virtual_port_absence = storage_connection_statistics_df['Physical_virtual_unique_quantity'] == 1
        mask_virtual_port = storage_connection_statistics_df['Physical_virtual_unique_quantity'] > 1
        mask_physical_virtual = storage_connection_statistics_df['FLOGI'] == 'physical_virtual'
        dfop.column_to_object(storage_connection_statistics_df, 'Virtual_port_note')
        storage_connection_statistics_df.loc[mask_virtual_port & mask_physical_virtual, 'Virtual_port_note'] = 'virtual_port_login'

        # storage_connection_statistics_df['Virtual_port_note'] = storage_connection_statistics_df['Physical_virtual_unique_quantity'].where(mask_virtual_port_absence, 'virtual_port_login')
        storage_connection_statistics_df.drop(columns=['Physical_virtual_unique_quantity'], inplace=True)
        return  storage_connection_statistics_df
    
    # add note if all storage ports with the same index connected to single fabric
    # column Fabric contains combination of Fabric_name and Fabric_label columns  
    fabric_lst = storage_ports_df['Fabric'].unique()
    mask_port_level = storage_connection_statistics_df['Group_type'].isin(['port'])
    # if value in All column is equal to value in one of fabrics columns then all ports with current index connected to single fabric
    mask_port_fabric_connection = storage_connection_statistics_df[fabric_lst].isin(storage_connection_statistics_df['All']).any(axis=1)
    dfop.column_to_object(storage_connection_statistics_df, 'Port_note')
    storage_connection_statistics_df.loc[mask_port_level & ~mask_port_fabric_connection, 'Port_note'] = 'multiple fabrics connection'

    # symmetry and port parity connection are verified for each Fabric_name
    fabric_names_lst = storage_ports_df['Fabric_name'].unique()
    # add symmetric connection note
    storage_connection_statistics_df['Symmetric_note'] = \
        storage_connection_statistics_df.apply(lambda series: verify_connection_symmetry(series, fabric_names_lst), axis = 1)
    # add port parity connection note
    storage_connection_statistics_df['Port_parity_note'] = \
        storage_connection_statistics_df.apply(lambda series: verify_port_parity(series, fabric_names_lst), axis = 1)
    # add virtual port login note
    storage_connection_statistics_df = verify_virtual_port_login(storage_connection_statistics_df)
    return storage_connection_statistics_df
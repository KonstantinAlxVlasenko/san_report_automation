"""Module to get DataFrame information"""

import re

def verify_columns_in_dataframe(df, columns):
    """Function to verify if columns are in DataFrame"""

    if not isinstance(columns, list):
        columns = [columns]
    return set(columns).issubset(df.columns)


def find_columns(df, column_pattern):
    """Find df columns corresponding to pattern"""
    
    return [column for column in df.columns if re.search(column_pattern, column)]
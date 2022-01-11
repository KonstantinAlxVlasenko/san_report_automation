"""Module to perform manipulations with data structures except DataFrames and Series"""


def line_to_list(re_object, line, *args):
    """
    Function to extract values from line with regex object 
    and combine values with other optional data into list
    """

    values, = re_object.findall(line)
    if isinstance(values, tuple) or isinstance(values, list):
        values_lst = [value.rstrip() if value else None for value in values]
    else:
        values_lst = [values.rstrip()]
    return [*args, *values_lst]


def update_dct(keys, values, dct, char = ', '):
    """Function to add param_add:value pairs
    to the dictionary with discovered parameters
    """

    for key, value in zip(keys, values):
        if value:                
            if isinstance(value, set) or isinstance(value, list):
                value = f'{char}'.join(value)
            dct[key] = value
    return dct


def list_is_empty(lst):
    """Function to check if nested list is empty. None considered to be empty value"""

    return all(map(list_is_empty, lst)) if isinstance(lst, list) else True if lst is None else False
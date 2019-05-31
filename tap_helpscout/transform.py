import re
import os
import json

# Remove all _links nodes from json
def remove_links(d):
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [remove_links(v) for v in d]
    return {k: remove_links(v) for k, v in d.items()
            if k not in {'_links'}}


# Removed unwanted _embedded nodes
def remove_embedded(d):
    if '_embedded' in d:
        if 'emails' in d['_embedded']: del d['_embedded']['emails']
        if 'websites' in d['_embedded']: del d['_embedded']['websites']
        if 'chats' in d['_embedded']: del d['_embedded']['chats']
        if 'phones' in d['_embedded']: del d['_embedded']['phones']
        if 'social_profiles' in d['_embedded']: del d['_embedded']['social_profiles']
    return d

# Convert camelCase to snake_case
# Reference: https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
def convert(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


# Convert keys in json array
def convert_array(a):
    new_arr = []
    for i in a:
        if isinstance(i,list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(j):
    out = {}
    for k in j:
        new_k = convert(k)
        if isinstance(j[k],dict):
            out[new_k] = convert_json(j[k])
        elif isinstance(j[k],list):
            out[new_k] = convert_array(j[k])
        else:
            out[new_k] = j[k]
    return out

# Flatten nested dict elements of json
# Reference: https://stackoverflow.com/questions/51359783/python-flatten-multilevel-json
def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict: # flatten dict elements
            for a in x:
                flatten(x[a], name.replace('_embedded_','') + a + '_')
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

# Command to format (flatten, convert, and remove) json elements
# data = flatten_json(convert_json(remove_embedded(remove_links(response['_embedded'][path]))))


# Not sure this is needed
# Any date-times values can either be a string or a null.
# If null, parsing the date results in an error.
# Instead, removing the attribute before parsing ignores this error.
def remove_empty_date_times(item, schema):
    fields = []
    for key in schema['properties']:
        subschema = schema['properties'][key]
        if subschema.get('format') == 'date-time':
            fields.append(key)
    for field in fields:
        if item.get(field) is None:
            del item[field]


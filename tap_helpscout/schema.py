import os
import json

PKS = {
    'conversations': ['id'],
    'conversation_threads': ['id'],
    'customers': ['id'],
    'mailboxes': ['id'],
    'mailbox_fields': ['id'],
    'mailbox_folders': ['id'],
    'users': ['id'],
    'workflows': ['id']
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():

    schemas = {}
    field_metadata = {}

    schemas_path = get_abs_path('schemas')

    file_names = [f for f in os.listdir(schemas_path)
                  if os.path.isfile(os.path.join(schemas_path, f))]

    for file_name in file_names:
        stream_name = file_name[:-5]
        with open(os.path.join(schemas_path, file_name)) as data_file:
            schema = json.load(data_file)

        schemas[stream_name] = schema
        primary_key = PKS[stream_name]

        metadata = []
        for prop in schema['properties'].items():
            if prop[0] in primary_key:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', prop[0]]
            })
        field_metadata[stream_name] = metadata

    return schemas, field_metadata

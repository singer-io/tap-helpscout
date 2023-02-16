import re


# Convert camelCase to snake_case
def convert(name):
    reg_sub = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", reg_sub).lower()


# Convert keys in json array
def convert_array(arr):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(this_json):
    out = {}
    for key in this_json:
        new_key = convert(key)
        if isinstance(this_json[key], dict):
            out[new_key] = convert_json(this_json[key])
        elif isinstance(this_json[key], list):
            out[new_key] = convert_array(this_json[key])
        else:
            out[new_key] = this_json[key]
    return out


# Remove all _links and _embedded nodes from json
def remove_embedded_links(this_json):
    if not isinstance(this_json, (dict, list)):
        return this_json
    if isinstance(this_json, list):
        return [remove_embedded_links(vv) for vv in this_json]
    return {kk: remove_embedded_links(vv) for kk, vv in this_json.items() if kk not in {"_embedded", "_links"}}


# Copy path/_embedded sub-nodes up to path
def denest_embedded_nodes(this_json, path=None):
    if path is None:
        return this_json
    i = 0
    nodes = ["attachments", "address", "chats", "emails", "phones", "social_profiles", "websites", "properties"]
    for record in this_json[path]:
        if "_embedded" in record:
            for node in nodes:
                if node in record["_embedded"]:
                    this_json[path][i][node] = this_json[path][i]["_embedded"][node]
        i = i + 1
    return this_json


# Add updated_at (bookmark) to conversations based on max of 2 other date-times
def transform_conversations(this_json, path=None):
    if path is None:
        return this_json
    i = 0
    for record in this_json[path]:
        user_updated_at = record.get("user_updated_at")
        customer_waiting_since = record.get("customer_waiting_since", {}).get("time")
        # Get max date, even if None
        max_date = [user_updated_at, customer_waiting_since]
        updated_at = max(i for i in max_date if i is not None)

        this_json[path][i]["updated_at"] = updated_at
        i = i + 1
    return this_json


def transform_ratings(this_json, path):
    if path is None:
        return this_json

    for record in this_json[path]:
        if 'id' in record:
            record["conversation_id"] = record["id"]
            record.pop("id")

        record["thread_id"] = record["threadid"]
        record.pop("threadid")

    return this_json


def transform_team_users(this_json, path):
    for record in this_json[path]:
        record["user_id"] = record["id"]

    return this_json


# Run all transforms: de-nests _embedded, removes _embedded/_links, and
# Converts camelCase to snake_case for field_name keys.
def transform_json(this_json, path, stream_name):
    de_nested_json = denest_embedded_nodes(this_json, path)
    no_links_json = remove_embedded_links(de_nested_json)
    converted_json = convert_json(no_links_json)
    if stream_name == "conversations":
        return transform_conversations(converted_json, path)
    elif stream_name == "happiness_ratings_report":
        return transform_ratings(converted_json, path)
    elif stream_name == "team_members":
        return transform_team_users(converted_json, path)
    return converted_json

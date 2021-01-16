import prov
from prov.serializers.provjson import ProvJSONSerializer
from prov.model import ProvDocument, PROV_TYPE, Namespace, NamespaceManager
import logging
import datetime

# Local modules
from schema import schema_manager

# HuBMAP commons
from hubmap_commons import globus_groups

# Set logging fromat and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

HUBMAP_NAMESPACE = 'hubmap'

"""
Build the provenance document based on the W3C PROV-DM
https://www.w3.org/TR/prov-dm/

Parameters
----------
normalized_provenance_dict : dict
    The processed dict that contains the complete entity properties

Returns
-------
str
    A JSON string representation of the provenance document
"""
def get_provenance_history(normalized_provenance_dict):
    logger.debug(normalized_provenance_dict)

    prov_doc = ProvDocument()

    prov_doc.add_namespace(HUBMAP_NAMESPACE, 'https://hubmapconsortium.org/')
  
    # A bit validation
    if 'relationships' not in normalized_provenance_dict:
        raise LookupError(f'Missing "relationships" key from the normalized_provenance_dict for Entity of uuid: {uuid}')

    if 'nodes' not in normalized_provenance_dict:
        raise LookupError(f'Missing "nodes" key from the normalized_provenance_dict for Entity of uuid: {uuid}')
    
    nodes_dict = {}

    # Pack the nodes into a dictionary using the uuid as key
    for node in normalized_provenance_dict['nodes']:
        nodes_dict[node['uuid']] = node
    
    # Loop through the relationships and build the provenance document
    for rel_dict in normalized_provenance_dict['relationships']:
        # (Activity) - [ACTIVITY_OUTPUT] -> (Entity)
        if rel_dict['rel_data']['type'] == 'ACTIVITY_OUTPUT':
            activity_uuid = rel_dict['fromNode']['uuid']
            entity_uuid = rel_dict['toNode']['uuid']
        # (Entity) - [ACTIVITY_INPUT] -> (Activity)
        elif rel_dict['rel_data']['type'] == 'ACTIVITY_INPUT':
            entity_uuid = rel_dict['fromNode']['uuid']
            activity_uuid = rel_dict['toNode']['uuid']

        activity_node = nodes_dict[activity_uuid]
        entity_node = nodes_dict[entity_uuid]

        # Get the agent information from the entity node
        agent_record = get_agent_record(entity_node)

        # Build the agent uri
        created_by_user_email_prov_key = f'{HUBMAP_NAMESPACE}:created_by_user_email'
        agent_id = str(agent_record[created_by_user_email_prov_key]).replace('@', '-')
        agent_id = str(agent_id).replace('.', '-')

        created_by_user_sub_prov_key = f'{HUBMAP_NAMESPACE}:created_by_user_sub'
        if created_by_user_sub_prov_key in agent_record:
            agent_id = agent_record[created_by_user_sub_prov_key]

        agent_uri = build_uri(HUBMAP_NAMESPACE, 'agent', agent_id)

        # Only add the same agent once
        # Multiple entities can be associated to the same agent
        agent = prov_doc.get_record(agent_uri)
        if len(agent) == 0:
            doc_agent = prov_doc.agent(agent_uri, agent_record)
        else:
            doc_agent = agent[0]

        # Organization
        # Get the organization information from the entity node
        org_record = get_organization_record(entity_node)

        # Build the organization uri
        group_uuid_prov_key = f'{HUBMAP_NAMESPACE}:uuid'
        org_uri = build_uri(HUBMAP_NAMESPACE, 'organization', org_record[group_uuid_prov_key])

        # Only add the same organization once
        # Multiple entities can be associated to different agents who are from the same organization
        org = prov_doc.get_record(org_uri)
        if len(org) == 0:
            doc_org = prov_doc.agent(org_uri, org_record)
        else:
            doc_org = org[0]

        # Build the activity record         
        activity_attributes = {
            'prov:type': 'Activity'
        }

        for key in activity_node:
            prov_key = f'{HUBMAP_NAMESPACE}:{key}'
            activity_attributes[prov_key] = activity_node[key]

        activity_timestamp_json = get_json_timestamp(int(activity_node['created_timestamp']))

        # Build the activity uri
        activity_uri = build_uri(HUBMAP_NAMESPACE, 'activities', activity_node['uuid'])
        
        # Add the activity to prov_doc
        # In our case, prov:startTime is the same as prov:endTime
        activity = prov_doc.get_record(activity_uri)
        if len(activity) == 0:
            doc_activity = prov_doc.activity(activity_uri, activity_timestamp_json, activity_timestamp_json, activity_attributes)
        else:
            doc_activity = activity[0]    
        
        # Attributes to be added to the PROV document
        entity_attributes = {
            'prov:type': 'Entity'
        }

        # The schema yaml doesn't handle Lab
        if entity_node['entity_type'] == 'Lab':
            final_entity_node = entity_node
        else:
            # Normalize the result based on schema and skip `label` attribute
            attributes_to_exclude = ['label']
            final_entity_node = schema_manager.normalize_entity_result_for_response(entity_node, attributes_to_exclude)

        for key in final_entity_node:
            # Entity property values can be list, skip
            # And list is unhashable type when calling `prov_doc.entity()`
            if not isinstance(final_entity_node[key], list):
                prov_key = f'{HUBMAP_NAMESPACE}:{key}'
                entity_attributes[prov_key] = final_entity_node[key]
    
        entity_uri = build_uri(HUBMAP_NAMESPACE, 'entities', entity_node['uuid'])

        # Only add once
        if len(prov_doc.get_record(entity_uri)) == 0:
            prov_doc.entity(entity_uri, entity_attributes)


        # (Activity) - [ACTIVITY_OUTPUT] -> (Entity)
        if rel_dict['rel_data']['type'] == 'ACTIVITY_OUTPUT':
            # Relationship: the entity wasGeneratedBy the activity
            prov_doc.wasGeneratedBy(entity_uri, activity_uri)
        # (Entity) - [ACTIVITY_INPUT] -> (Activity)
        elif rel_dict['rel_data']['type'] == 'ACTIVITY_INPUT':
            # Relationship: the activity used the entity
            prov_doc.used(activity_uri, entity_uri)

            # Relationship: the agent actedOnBehalfOf the org
            prov_doc.actedOnBehalfOf(doc_agent, doc_org, doc_activity)

    # Format into json string based on the PROV-JSON Serialization
    # https://www.w3.org/Submission/prov-json/
    serialized_json = prov_doc.serialize() 

    return serialized_json


####################################################################################################
## Helper Functions
####################################################################################################


"""
Build the uri

Parameters
----------
prefix : str
    The prefix
uri_type : str
    The type of the prov: agent, activities, entities
identifier : str
    The unique identifier

Returns
-------
str
    The uri string
"""
def build_uri(prefix, uri_type, identifier):
    return f"{prefix}:{str(uri_type)}/{str(identifier)}"

"""
Build the timestamp json

Parameters
----------
int_timestamp : int
    The timestamp in integer form

Returns
-------
json
    The timestamp json
"""
def get_json_timestamp(int_timestamp):
    date = datetime.datetime.fromtimestamp(int_timestamp / 1e3)
    json_date = date.strftime("%Y-%m-%dT%H:%M:%S")
    return json_date

"""
Build the agent - person record

Parameters
----------
node_dict : dict
    The entity dict

Returns
-------
dict
    The prov dict for person 
"""
def get_agent_record(node_dict):
    created_by_user_displayname_prov_key = f'{HUBMAP_NAMESPACE}:created_by_user_displayname'
    created_by_user_email_prov_key = f'{HUBMAP_NAMESPACE}:created_by_user_email'
    created_by_user_sub_prov_key = f'{HUBMAP_NAMESPACE}:created_by_user_sub'

    # All agents share this same PROV_TYPE
    agent_dict = {
        PROV_TYPE: 'prov:Person',
        created_by_user_displayname_prov_key: 'hubmap:userDisplayName',
        created_by_user_email_prov_key: 'hubmap:userEmail',
        created_by_user_sub_prov_key: 'hubmap:userUUID'
    }

    # Add to agent_dict if exists in node_dict
    if 'created_by_user_displayname' in node_dict:
        agent_dict[created_by_user_displayname_prov_key] = node_dict['created_by_user_displayname']
    
    if 'created_by_user_email' in node_dict:
        agent_dict[created_by_user_email_prov_key] = node_dict['created_by_user_email']
    
    if 'created_by_user_sub' in node_dict:
        agent_dict[created_by_user_sub_prov_key] = node_dict['created_by_user_sub']

    return agent_dict

"""
Build the agent - organization record

Parameters
----------
node_dict : dict
    The entity dict

Returns
-------
dict
    The prov dict for organization 
"""
def get_organization_record(node_dict):
    logger.debug("=========node_dict")
    logger.debug(node_dict)

    group = {}

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']
    groups_by_name_dict = globus_groups_info['by_name']

    # All organizations share this same PROV_TYPE
    # uuid and displayname are default values
    group_uuid_prov_key = f'{HUBMAP_NAMESPACE}:uuid'
    # Return displayname (no dash, space separated) instead of name (dash-connected)
    group_name_prov_key = f'{HUBMAP_NAMESPACE}:displayname'

    org_dict = {
        PROV_TYPE: 'prov:Organization',
        group_uuid_prov_key: 'hubmap:groupUUID',
        group_name_prov_key: 'hubmap:groupName'
    }

    if 'group_uuid' in node_dict:
        group_uuid = node_dict['group_uuid']
        if group_uuid not in groups_by_id_dict:
            raise LookupError(f'Cannot find group with uuid: {group_uuid}')

        if 'group_name' not in node_dict:
            group = groups_by_id_dict[group_uuid]
    elif 'group_name' in node_dict:
        group_name = node_dict['group_name']
        if group_name in groups_by_name_dict:
            group = groups_by_name_dict[group_name]
        # Handle the case where the group_uuid is incorrectly stored in the group_name field
        elif group_name in groups_by_id_dict:
            group = groups_by_id_dict[group_name]
        else:
            msg = f"Unable to find group of name: {group_name}"
            logger.error(msg)
            logger.debug(node_dict)
            raise LookupError(msg)
    else:
        msg = f"Both 'group_uuid' and 'group_name' are missing from Entity uuid: {node_dict['uuid']}"
        logger.error(msg)
        logger.debug(node_dict)

    # By now we have the group information available
    group_uuid_prov_key = f'{HUBMAP_NAMESPACE}:uuid'
    # Return displayname (no dash, space separated) instead of name (dash-connected)
    group_name_prov_key = f'{HUBMAP_NAMESPACE}:displayname'

    # Overwrite the default values in org_dict if the attributes exist in group
    if 'uuid' in group:
        org_dict[group_uuid_prov_key] = group['uuid']

    if 'displayname' in group:
        org_dict[group_name_prov_key] = group['displayname']

    return org_dict

from prov.model import ProvDocument, PROV
import logging
import datetime


logger = logging.getLogger(__name__)

HUBMAP_NAMESPACE = 'hubmap'

"""
Build the provenance document based on the W3C PROV-DM
https://www.w3.org/TR/prov-dm/

Parameters
----------
uuid : str
    The UUID of the associated entity
normalized_provenance_dict : dict
    The dict that contains all the normalized entity properties defined by the schema yaml

Returns
-------
str
    A JSON string representation of the provenance document
"""
def get_provenance_history(uuid, normalized_provenance_dict, auth_helper_instance):
    prov_doc = ProvDocument()
    # The 'prov' prefix is build-in namespace, no need to redefine here
    prov_doc.add_namespace(HUBMAP_NAMESPACE, 'https://hubmapconsortium.org/')
  
    # A bit validation
    if 'relationships' not in normalized_provenance_dict:
        raise LookupError(f'Missing "relationships" key from the normalized_provenance_dict for Entity of uuid: {uuid}')

    if 'nodes' not in normalized_provenance_dict:
        raise LookupError(f'Missing "nodes" key from the normalized_provenance_dict for Entity of uuid: {uuid}')
    
    # Pack the nodes into a dictionary using the uuid as key
    nodes_dict = {}
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
        
        activity_uri = None
        entity_uri = None

        # Skip Lab nodes for agent and organization
        if entity_node['entity_type'] != 'Lab':
            # Get the agent information from the entity node
            agent_record = get_agent_record(entity_node)

            # Use 'created_by_user_sub' as agent ID if presents
            # Otherwise, fall back to use email by replacing @ and .
            created_by_user_sub_prov_key = f'{HUBMAP_NAMESPACE}:userUUID'
            created_by_user_email_prov_key = f'{HUBMAP_NAMESPACE}:userEmail'
            if created_by_user_sub_prov_key in agent_record:
                agent_id = agent_record[created_by_user_sub_prov_key]
            elif created_by_user_email_prov_key in agent_record:
                agent_id = str(agent_record[created_by_user_email_prov_key]).replace('@', '-')
                agent_id = str(agent_id).replace('.', '-')
            else:
                msg = f"Both 'created_by_user_sub' and 'created_by_user_email' are missing form entity of uuid: {entity_node['uuid']}"
                logger.error(msg)
                raise LookupError(msg)

            # Build the agent uri
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
            group_uuid_prov_key = f'{HUBMAP_NAMESPACE}:groupUUID'
            org_uri = build_uri(HUBMAP_NAMESPACE, 'organization', org_record[group_uuid_prov_key])

            # Only add the same organization once
            # Multiple entities can be associated to different agents who are from the same organization
            org = prov_doc.get_record(org_uri)
            if len(org) == 0:
                doc_org = prov_doc.agent(org_uri, org_record)
            else:
                doc_org = org[0]

            # Build the activity uri
            activity_uri = build_uri(HUBMAP_NAMESPACE, 'activities', activity_node['uuid'])
            
            # Register activity if not already registered
            activity = prov_doc.get_record(activity_uri)
            if len(activity) == 0:
                # Shared attributes to be added to the PROV document       
                activity_attributes = {
                    'prov:type': 'Activity'
                }

                # Convert the timestampt integer to datetime string
                # Note: in our case, prov:startTime is the same as prov:endTime
                activity_time = timestamp_to_datetime(activity_node['created_timestamp'])

                # Add prefix to all other attributes
                for key in activity_node:
                    prov_key = f'{HUBMAP_NAMESPACE}:{key}'
                    # Use datetime string instead of timestamp integer
                    if key == 'created_timestamp':
                        activity_attributes[prov_key] = activity_time
                    else:
                        activity_attributes[prov_key] = activity_node[key]

                # Register activity
                doc_activity = prov_doc.activity(activity_uri, activity_time, activity_time, activity_attributes)
                
                # Relationship: the agent actedOnBehalfOf the org
                prov_doc.actedOnBehalfOf(doc_agent, doc_org, doc_activity)
            else:
                doc_activity = activity[0]    
            
            # Build the entity uri
            entity_uri = build_uri(HUBMAP_NAMESPACE, 'entities', entity_node['uuid'])

            # Register entity is not already registered
            if len(prov_doc.get_record(entity_uri)) == 0:
                # Shared attributes to be added to the PROV document
                entity_attributes = {
                    'prov:type': 'Entity'
                }

                # Add prefix to all other attributes
                for key in entity_node:
                    # Entity property values can be list or dict, skip
                    # And list and dict are unhashable types when calling `prov_doc.entity()`
                    if not isinstance(entity_node[key], (list, dict)):
                        prov_key = f'{HUBMAP_NAMESPACE}:{key}'
                        # Use datetime string instead of timestamp integer
                        if key in ['created_timestamp', 'last_modified_timestamp', 'published_timestamp']:
                            entity_attributes[prov_key] = activity_time
                        else:
                            entity_attributes[prov_key] = entity_node[key]
            
                # Register entity
                prov_doc.entity(entity_uri, entity_attributes)

        # Build activity uri and entity uri if not already built
        # For the Lab nodes
        if activity_uri is None:
            activity_uri = build_uri(HUBMAP_NAMESPACE, 'activities', activity_node['uuid'])

        if entity_uri is None:
            entity_uri = build_uri(HUBMAP_NAMESPACE, 'entities', entity_node['uuid'])

        # The following relationships apply to all node including Lab entity nodes
        # (Activity) - [ACTIVITY_OUTPUT] -> (Entity)
        if rel_dict['rel_data']['type'] == 'ACTIVITY_OUTPUT':
            # Relationship: the entity wasGeneratedBy the activity
            prov_doc.wasGeneratedBy(entity_uri, activity_uri)
        # (Entity) - [ACTIVITY_INPUT] -> (Activity)
        elif rel_dict['rel_data']['type'] == 'ACTIVITY_INPUT':
            # Relationship: the activity used the entity
            prov_doc.used(activity_uri, entity_uri)

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
Convert the timestamp int to formatted datetime string

Parameters
----------
timestamp : int
    The timestamp in integer form

Returns
-------
str
    The formatted datetime string, e.g., 2001-10-26T21:32:52
"""
def timestamp_to_datetime(timestamp):
    date = datetime.datetime.fromtimestamp(int(timestamp) / 1e3)
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
    created_by_user_displayname_prov_key = f'{HUBMAP_NAMESPACE}:userDisplayName'
    created_by_user_email_prov_key = f'{HUBMAP_NAMESPACE}:userEmail'
    created_by_user_sub_prov_key = f'{HUBMAP_NAMESPACE}:userUUID'

    # Shared attribute
    agent_dict = {
        'prov:type': PROV['Person']
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

auth_helper_instance: AuthHelper
    The auth helper instance passed in
    
Returns
-------
dict
    The prov dict for organization 
"""
def get_organization_record(node_dict, auth_helper_instance):
    group = {}

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = auth_helper_instance.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']
    groups_by_name_dict = globus_groups_info['by_name']
    
    group_uuid_prov_key = f'{HUBMAP_NAMESPACE}:groupUUID'
    # Return displayname (no dash, space separated) instead of name (dash-connected)
    group_name_prov_key = f'{HUBMAP_NAMESPACE}:groupName'

    # Shared attribute
    org_dict = {
        'prov:type': PROV['Organization']
    }

    if 'group_uuid' in node_dict:
        group_uuid = node_dict['group_uuid']
        if group_uuid not in groups_by_id_dict:
            raise LookupError(f'Cannot find group with uuid: {group_uuid}')
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

    # Add to org_dict if the attributes exist in group
    if 'uuid' in group:
        org_dict[group_uuid_prov_key] = group['uuid']

    if 'displayname' in group:
        org_dict[group_name_prov_key] = group['displayname']
    
    return org_dict

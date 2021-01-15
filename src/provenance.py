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
    relation_list = []

    # Pack the nodes into a dictionary using the uuid as key
    for node in normalized_provenance_dict['nodes']:
        nodes_dict[node['uuid']] = node
    
    # Loop through the relationships and build the provenance document
    for rel_dict in normalized_provenance_dict['relationships']:
        # Step 1: build the PROV core concepts: Entity, Acvivitiy
        from_uuid = rel_dict['fromNode']['uuid']
        to_uuid = rel_dict['toNode']['uuid']

        from_node = nodes_dict[from_uuid]
        to_node = nodes_dict[to_uuid]

        # Find out if the from node is Entity or Activity
        prov_type = None
        is_entity = True
        if from_node['label'] == 'Entity':
            prov_type = from_node['entity_type']
        elif from_node['label'] == 'Activity':
            prov_type = from_node['creation_action']
            is_entity = False

        # Use submission_id as the label for Entity if exists
        # Otherwise use uuid
        label_text = None                                
        if 'submission_id' in from_node:
            label_text = from_node['submission_id']
        else:
            label_text = from_node['uuid']
        
        # Need to add the agent and organization here, 
        # plus the appropriate relationships (between the entity and the agent plus orgainzation)
        agent_record = get_agent_record(to_node)

        # Use `created_by_user_sub` as agent id if exists, otherwise try created_by_user_email
        agent_id = None
        created_by_user_sub_prov_key = f'{HUBMAP_NAMESPACE}:created_by_user_sub'
        created_by_user_email_prov_key = f'{HUBMAP_NAMESPACE}:created_by_user_email'

        if created_by_user_sub_prov_key in agent_record:
            agent_id = agent_record[created_by_user_sub_prov_key]
        elif created_by_user_email_prov_key in agent_record:
            agent_id = str(agent_record[created_by_user_email_prov_key]).replace('@', '-')
            agent_id = str(agent_id).replace('.', '-')

        agent_uri = build_uri(HUBMAP_NAMESPACE, 'agent', agent_id)
        org_record = get_organization_record(to_node)


        logger.debug("ddddddddddddddd")
        logger.debug(organization_record)

        group_uuid_prov_key = f'{HUBMAP_NAMESPACE}:group_uuid'
        org_uri = build_uri(HUBMAP_NAMESPACE, 'organization', org_record[group_uuid_prov_key])
        doc_agent = None
        doc_org = None
        
        get_agent = prov_doc.get_record(agent_uri)
        # Only add agent once
        # Multiple entities can be associated to the same agent
        if len(get_agent) == 0:
            doc_agent = prov_doc.agent(agent_uri, agent_record)
        else:
            doc_agent = get_agent[0]

        get_org = prov_doc.get_record(org_uri)
        # Only add this once
        # Multiple entities can be associated to different agents who are from the same organization
        if len(get_org) == 0:
            doc_org = prov_doc.agent(org_uri, org_record)
        else:
            doc_org = get_org[0]
                  
        # Attributes to be exposed in the PROV document
        exposed_attributes = {}

        # Add node attributes to the exposed_attributes
        if from_node['label'] == 'Entity':
            # Skip Lab nodes
            if from_node['entity_type'] != 'Lab':
                # Normalize the result based on schema
                final_node = schema_manager.normalize_entity_result_for_response(from_node)
                for key in final_node:
                    prov_key = f'{HUBMAP_NAMESPACE}:{key}'
                    exposed_attributes[prov_key] = final_node[key]
        else:
            for key in from_node:
                prov_key = f'{HUBMAP_NAMESPACE}:{key}'
                exposed_attributes[prov_key] = from_node[key]

        if is_entity == True:
            prov_doc.entity(build_uri(HUBMAP_NAMESPACE, 'entities', from_node['uuid']), exposed_attributes)
        else:
            activity_timestamp_json = get_json_timestamp(int(to_node['created_timestamp']))
            activity_url = build_uri(HUBMAP_NAMESPACE, 'activities', from_node['uuid'])
            doc_activity = prov_doc.activity(activity_url, activity_timestamp_json, activity_timestamp_json, exposed_attributes)
            prov_doc.actedOnBehalfOf(doc_agent, doc_org, doc_activity)

        # Step 2: build the PROV relations: WasGeneratedBy, Used, ActedOnBehalfOf
        to_node_uri = None
        from_node_uri = None

        if to_node['label'] == 'Entity':
            to_node_uri = build_uri('hubmap', 'entities', to_node['uuid'])
        else:
            to_node_uri = build_uri('hubmap', 'activities', to_node['uuid'])
        
        if from_node['label'] == 'Entity':
            from_node_uri = build_uri('hubmap', 'entities', from_node['uuid'])
        else:
            from_node_uri = build_uri('hubmap', 'activities', from_node['uuid'])
        
        if rel_dict['rel_data']['type'] == 'ACTIVITY_OUTPUT':
            prov_doc.wasGeneratedBy(to_node_uri, from_node_uri)

        if rel_dict['rel_data']['type'] == 'ACTIVITY_INPUT':
            prov_doc.used(to_node_uri, from_node_uri)
        
        # For now, simply create a "relation" where the fromNode's uuid is connected to a toNode's uuid via a relationship:
        # ex: {'fromNodeUUID': '42e10053358328c9079f1c8181287b6d', 'relationship': 'ACTIVITY_OUTPUT', 'toNodeUUID': '398400024fda58e293cdb435db3c777e'}
        rel_data_record = {
            'fromNodeUUID': from_node['uuid'], 
            'relationship': rel_dict['rel_data']['type'], 
            'toNodeUUID': to_node['uuid']
        }

        relation_list.append(rel_data_record)

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
    agent_attributes = ['created_by_user_displayname', 'created_by_user_email', 'created_by_user_sub']
        
    # All agents share this same PROV_TYPE
    agent_dict = {
        PROV_TYPE: 'prov:Person'
    }

    for key in agent_attributes:
        if key in node_dict:
            prov_key = f'{HUBMAP_NAMESPACE}:{key}'
            # Add to the result
            agent_dict[prov_key] = node_dict[key]

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

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']
    groups_by_name_dict = globus_groups_info['by_name']

    logger.debug("=========groups_by_id_dict")
    logger.debug(groups_by_id_dict)

    logger.debug("=========groups_by_name_dict")
    logger.debug(groups_by_name_dict)
        
    # All organizations share this same PROV_TYPE
    org_dict = {
        PROV_TYPE: 'prov:Organization'
    }

    if ('group_uuid' not in node_dict) and ('group_name' not in node_dict):
        node_dict['group_uuid'] = 'Missing'
        node_dict['group_uuid'] = 'Missing'
    elif ('group_uuid' in node_dict) and ('group_name' not in node_dict):
        group_uuid = node_dict['group_uuid']
        if group_uuid in groups_by_id_dict:
            group_record = groups_by_id_dict[group_uuid]
            # Add group_name to node_dict
            node_dict['group_name'] = group_record['displayname']
        else:
            raise LookupError(f'Cannot find group with uuid: {group_uuid}')
    elif ('group_uuid' not in node_dict) and ('group_name' in node_dict):
        group_name = node_dict['group_name']
        if group_name in groups_by_name_dict:
            group_record = groups_by_name_dict[group_name]
            # Add group_uuid to node_dict
            node_dict['group_uuid'] = group_record['uuid']
        else:
            raise LookupError(f'Cannot find group with name: {group_name}')

    # By now both 'group_uuid' and 'group_name' exist
    group_uuid_prov_key = f'{HUBMAP_NAMESPACE}:group_uuid'
    group_name_prov_key = f'{HUBMAP_NAMESPACE}:group_name'

    # Add to the result
    org_dict[group_uuid_prov_key] = node_dict['group_uuid']
    org_dict[group_name_prov_key] = node_dict['group_name']

    return org_dict

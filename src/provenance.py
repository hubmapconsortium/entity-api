import prov
from prov.serializers.provjson import ProvJSONSerializer
from prov.model import ProvDocument, PROV_TYPE, Namespace, NamespaceManager
import logging
import datetime

# Local modules
from schema import schema_manager

# Set logging fromat and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


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

    prov_doc.add_namespace('hubmap', 'https://hubmapconsortium.org/')
    prov_doc.add_namespace('prov', 'https://hubmapconsortium.org/')
    
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
        agent_unique_id = str(agent_record['hubmap:created_by_user_email']).replace('@', '-')
        agent_unique_id = str(agent_unique_id).replace('.', '-')

        if 'created_by_user_sub' in agent_record:
            agent_unique_id = agent_record['created_by_user_sub']

        agent_uri = build_uri('hubmap', 'agent', agent_unique_id)
        organization_record = get_organization_record(to_node)
        organization_uri = build_uri('hubmap', 'organization', organization_record['hubmap:group_uuid'])
        doc_agent = None
        doc_org = None
        
        get_agent = prov_doc.get_record(agent_uri)
        # Only add agent once
        # Multiple entities can be associated to the same agent
        if len(get_agent) == 0:
            doc_agent = prov_doc.agent(agent_uri, agent_record)
        else:
            doc_agent = get_agent[0]

        get_org = prov_doc.get_record(organization_uri)
        # Only add this once
        # Multiple entities can be associated to different agents who are from the same organization
        if len(get_org) == 0:
            doc_org = prov_doc.agent(organization_uri, organization_record)
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
                    prov_key = f'hubmap:{key}'
                    exposed_attributes[prov_key] = final_node[key]
        else:
            for key in from_node:
                prov_key = f'hubmap:{key}'
                exposed_attributes[prov_key] = from_node[key]

        if is_entity == True:
            prov_doc.entity(build_uri('hubmap', 'entities', from_node['uuid']), exposed_attributes)
        else:
            activity_timestamp_json = get_json_timestamp(int(to_node['created_timestamp']))
            activity_url = build_uri('hubmap', 'activities', from_node['uuid'])
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
node_data : dict
    The entity dict

Returns
-------
dict
    The prov dict for person 
"""
def get_agent_record(node_data):
    agent_attributes = ['created_by_user_displayname', 'created_by_user_email', 'created_by_user_sub']
        
    # All agents share this same PROV_TYPE
    agent_dict = {
        PROV_TYPE: 'prov:Person'
    }

    for key in agent_attributes:
        if key in node_data:
            prov_key = f'hubmap:{key}'
            # Add to the result
            agent_dict[prov_key] = node_data[key]

    return agent_dict

"""
Build the agent - organization record

Parameters
----------
node_data : dict
    The entity dict

Returns
-------
dict
    The prov dict for organization 
"""
def get_organization_record(node_data):
    org_attributes = ['group_uuid', 'group_name']
        
    # All organizations share this same PROV_TYPE
    org_dict = {
        PROV_TYPE: 'prov:Organization'
    }

    for key in org_attributes:
        if key in node_data:
            prov_key = f'hubmap:{key}'
            # Add to the result
            org_dict[prov_key] = node_data[key]

    return org_dict

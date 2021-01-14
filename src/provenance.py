import prov
from prov.serializers.provjson import ProvJSONSerializer
from prov.model import ProvDocument, PROV_TYPE, Namespace, NamespaceManager
import logging
import datetime

# Local modules
import app_neo4j_queries

# HuBMAP commons
from hubmap_commons import globus_groups

# Set logging fromat and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


PROV_ENTITY_TYPE = 'prov:Entity'
PROV_ACTIVITY_TYPE = 'prov:Activity'
PROV_AGENT_TYPE = 'prov:Agent'
PROV_COLLECTION_TYPE = 'prov:Collection'
PROV_ORGANIZATION_TYPE = 'prov:Organization'
PROV_PERSON_TYPE = 'prov:Person'
PROV_LABEL_ATTRIBUTE = 'prov:label'
PROV_TYPE_ATTRIBUTE = 'prov:type'
PROV_GENERATED_TIME_ATTRIBUTE = 'prov:generatedAtTime'
HUBMAP_DOI_ATTRIBUTE = 'hubmap:doi' #the doi concept here might be a good alternative: https://sparontologies.github.io/datacite/current/datacite.html
HUBMAP_DISPLAY_DOI_ATTRIBUTE = 'hubmap:displayDOI' 
HUBMAP_SPECIMEN_TYPE_ATTRIBUTE = 'hubmap:specimenType' 
HUBMAP_DISPLAY_IDENTIFIER_ATTRIBUTE = 'hubmap:displayIdentifier' 
HUBMAP_UUID_ATTRIBUTE = 'hubmap:uuid' 
HUBMAP_MODIFIED_TIMESTAMP = 'hubmap:modifiedTimestamp'
HUBMAP_PROV_GROUP_NAME = 'hubmap:groupName'
HUBMAP_PROV_GROUP_UUID = 'hubmap:groupUUID'
HUBMAP_PROV_USER_DISPLAY_NAME = 'hubmap:userDisplayName'
HUBMAP_PROV_USER_EMAIL = 'hubmap:userEmail'
HUBMAP_PROV_USER_UUID = 'hubmap:userUUID'


def get_provenance_history(provenance_dict):
    ignore_attributes = [
        'entity_type', 
        'created_timestamp', 
        'uuid', 
        'label'
    ]
    
    known_attribute_map = {
        'group_name': HUBMAP_PROV_GROUP_NAME, 
        'group_uuid': HUBMAP_PROV_GROUP_UUID,
        'created_by_user_displayname': HUBMAP_PROV_USER_DISPLAY_NAME, 
        'created_by_user_email': HUBMAP_PROV_USER_EMAIL,
        'created_by_user_sub': HUBMAP_PROV_USER_UUID, 
        'last_modified_timestamp': HUBMAP_MODIFIED_TIMESTAMP
    }

    prov_doc = ProvDocument()

    #NOTE!! There is a bug with the JSON serializer.  I can't add the prov prefix using this mechanism
    
    prov_doc.add_namespace('ex', 'http://example.org/')
    prov_doc.add_namespace('hubmap', 'https://hubmapconsortium.org/')
    
    #prov_doc.add_namespace('dct', 'http://purl.org/dc/terms/')
    #prov_doc.add_namespace('foaf','http://xmlns.com/foaf/0.1/')
    
    relation_list = []
    
    if 'relationships' not in provenance_dict:
        raise LookupError(f"No relationships found for uuid: {uuid}")

    if 'nodes' not in provenance_dict:
        raise LookupError(f"No graph nodes found for uuid: {uuid}")
    
    nodes_dict = {}
    # Pack the nodes into a dictionary using the uuid as key
    for node in provenance_dict['nodes']:
        nodes_dict[node['uuid']] = node
        
    for rel_dict in provenance_dict['relationships']:
        # Step 1: build the PROV core concepts: Entity, Acvivitiy
        from_uuid = rel_dict['fromNode']['uuid']
        to_uuid = rel_dict['toNode']['uuid']

        from_node = nodes_dict[from_uuid]
        to_node = nodes_dict[to_uuid]

        # Find out if the from node is Entity or Activity
        prov_type = None
        isEntity = True
        if from_node['label'] == 'Entity':
            prov_type = from_node['entity_type']
        elif from_node['label'] == 'Activity':
            prov_type = from_node['creation_action']
            isEntity = False

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
        agent_unique_id = str(agent_record[HUBMAP_PROV_USER_EMAIL]).replace('@', '-')
        agent_unique_id = str(agent_unique_id).replace('.', '-')

        if HUBMAP_PROV_USER_UUID in agent_record:
            agent_unique_id = agent_record[HUBMAP_PROV_USER_UUID]

        agent_uri = build_uri('hubmap','agent', agent_unique_id)
        organization_record = get_organization_record(to_node)
        organization_uri = build_uri('hubmap','organization', organization_record[HUBMAP_PROV_GROUP_UUID])
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
                  
        other_attributes = {
            PROV_LABEL_ATTRIBUTE: label_text,
            PROV_TYPE_ATTRIBUTE: prov_type, 

            # doi renamed to doi_sufix_id, and doi_sufix_id is no longer needed
            #HUBMAP_DOI_ATTRIBUTE : from_node['doi'],

            HUBMAP_DISPLAY_IDENTIFIER_ATTRIBUTE: label_text, 
            HUBMAP_UUID_ATTRIBUTE: from_node['uuid']                                                    
        }

        # Skip Lab nodes
        if ((from_node['label'] == 'Entity') and (from_node['entity_type'] != 'Lab')) or (from_node['label'] == 'Activity'):
            # Add hubmap_id, display_doi renamed to hubmap_id
            other_attributes[HUBMAP_DISPLAY_DOI_ATTRIBUTE] = from_node['hubmap_id'] 

        if isEntity == True:
            prov_doc.entity(build_uri('hubmap','entities',from_node['uuid']), other_attributes)
        else:
            activity_timestamp_json = get_json_timestamp(int(to_node['created_timestamp']))
            activity_url = build_uri('hubmap','activities',from_node['uuid'])
            doc_activity = prov_doc.activity(activity_url, activity_timestamp_json, activity_timestamp_json, other_attributes)
            prov_doc.actedOnBehalfOf(doc_agent, doc_org, doc_activity)

        # Step 2: build the PROV relations: WasGeneratedBy, Used, ActedOnBehalfOf
        to_node_uri = None
        from_node_uri = None

        if 'entity_type' in to_node:
            to_node_uri = build_uri('hubmap', 'entities', to_node['uuid'])
        else:
            to_node_uri = build_uri('hubmap', 'activities', to_node['uuid'])
        
        if 'entity_type' in from_node:
            from_node_uri = build_uri('hubmap', 'entities', from_node['uuid'])
        else:
            from_node_uri = build_uri('hubmap', 'activities', from_node['uuid'])
        
        if rel_dict['rel_data']['type'] == 'ACTIVITY_OUTPUT':
            #prov_doc.wasGeneratedBy(entity, activity, time, identifier, other_attributes)
            prov_doc.wasGeneratedBy(to_node_uri, from_node_uri)

        if rel_dict['rel_data']['type'] == 'ACTIVITY_INPUT':
            #prov_doc.used(activity, entity, time, identifier, other_attributes)
            prov_doc.used(to_node_uri, from_node_uri)
        
        # for now, simply create a "relation" where the fromNode's uuid is connected to a toNode's uuid via a relationship:
        # ex: {'fromNodeUUID': '42e10053358328c9079f1c8181287b6d', 'relationship': 'ACTIVITY_OUTPUT', 'toNodeUUID': '398400024fda58e293cdb435db3c777e'}
        rel_data_record = {
            'fromNodeUUID': from_node['uuid'], 
            'relationship': rel_dict['rel_data']['type'], 
            'toNodeUUID': to_node['uuid']
        }

        relation_list.append(rel_data_record)

    # Why not being used? 
    return_data = {
        'nodes': nodes_dict, 
        'relations': relation_list
    }  

    logger.debug(return_data)

    # there is a bug in the JSON serializer.  So manually insert the prov prefix
    
    output_doc = prov_doc.serialize(indent=2) 
    output_doc = output_doc.replace('"prefix": {', '"prefix": {\n    "prov" : "http://www.w3.org/ns/prov#", ')
    
    #output_doc = prov_doc.serialize(format='rdf', rdf_format='trig')
    #output_doc = prov_doc.serialize(format='provn')
    return output_doc

def build_uri(prefix, uri_type, identifier):
    return prefix + ':' + str(uri_type) + '/' + str(identifier)

def get_json_timestamp(int_timestamp):
    date = datetime.datetime.fromtimestamp(int_timestamp / 1e3)
    jsondate = date.strftime("%Y-%m-%dT%H:%M:%S")
    return jsondate

def get_agent_record(node_data):
    agent_attribute_map = {
        'created_by_user_displayname': HUBMAP_PROV_USER_DISPLAY_NAME, 
        'created_by_user_email': HUBMAP_PROV_USER_EMAIL,
        'created_by_user_sub' : HUBMAP_PROV_USER_UUID
    }

    return_dict = {}
    for attribute_key in node_data:
        if attribute_key in agent_attribute_map:
            return_dict[agent_attribute_map[attribute_key]] = node_data[attribute_key]
    return_dict[PROV_TYPE] = 'prov:Person'
    return return_dict

def get_organization_record(node_data):
    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']
    groups_by_name_dict = globus_groups_info['by_name']

    organization_attribute_map = {
        'displayname' : HUBMAP_PROV_GROUP_NAME, 
        'uuid' : HUBMAP_PROV_GROUP_UUID
    }

    # lookup the node's provenance group using the group JSON file as a source
    # previously it relied on data found in the nodes, but that might be incomplete
    return_dict = {}
    group_record = {}
    if 'group_uuid' in node_data:
        group_uuid = node_data['group_uuid']
        if group_uuid in groups_by_id_dict:
            group_record = groups_by_id_dict[group_uuid]
        else:
            raise LookupError('Cannot find group for uuid: ' + group_uuid)
    elif 'group_name' in node_data:
        group_name = node_data['group_name']
        if group_name in groups_by_name_dict:
            group_record = groups_by_name_dict[group_name]
        #handle the case where the group UUID is incorrectly stored in the name field:
        elif group_name in groups_by_id_dict:
            group_record = groups_by_id_dict[group_name]
        else:
            raise LookupError('Cannot find group for name: ' + group_name)
    for attribute_key in group_record:
        if attribute_key in organization_attribute_map:
            return_dict[organization_attribute_map[attribute_key]] = group_record[attribute_key]
    return_dict[PROV_TYPE] = 'prov:Organization'
    return return_dict
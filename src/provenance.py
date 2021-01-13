import prov
from prov.serializers.provjson import ProvJSONSerializer
from prov.model import ProvDocument, PROV_TYPE, Namespace, NamespaceManager
import logging

# Local modules
import app_neo4j_queries

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
HUBMAP_METADATA_ATTRIBUTE = 'hubmap:metadata'
HUBMAP_MODIFIED_TIMESTAMP = 'hubmap:modifiedTimestamp'
HUBMAP_PROV_GROUP_NAME = 'hubmap:groupName'
HUBMAP_PROV_GROUP_UUID = 'hubmap:groupUUID'
HUBMAP_PROV_USER_DISPLAY_NAME = 'hubmap:userDisplayName'
HUBMAP_PROV_USER_EMAIL = 'hubmap:userEmail'
HUBMAP_PROV_USER_UUID = 'hubmap:userUUID'


def get_provenance_history(uuid, depth = None):
    metadata_ignore_attributes = [
        'entity_type', 
        'created_timestamp', 
        'reference_uuid',
        'uuid', 
        'source_uuid', 
        'source_display_id', # Gone
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
    
    # max_level_str is the string used to put a limit on the number of levels to traverse
    max_level_str = ''
    if depth is not None and len(str(depth)) > 0:
        max_level_str = f"maxLevel: {depth}, "

    # Convert neo4j json to dict
    result = app_neo4j_queries.get_provenance(neo4j_driver_instance, uuid, max_level_str)
    record = dict(result['json'])
    
    logger.info(record)

    if 'relationships' not in record:
        raise LookupError(f"Unable to find relationships for uuid: {uuid}")

    if 'nodes' not in record:
        raise LookupError(f"Unable to find nodes for uuid: {uuid}")
    
    node_dict = {}
    # pack the nodes into a dictionary using the uuid as a key
    for node_record in record['nodes']:
        node_dict[node_record['uuid']] = node_record
        
    # TODO: clean up nodes
    # remove nodes that lack metadata
    
    # need to devise a methodology for this
    # try preprocessing the record['relationships'] here:
    # make a copy of the node_dict called unreferenced_node_dict
    # loop through the relationships and find all the has_metadata relationships
    # for each node pair in the has_metadata relationship, delete it from the unreferenced_node_dict
    # once the loop is finished, continue as before
    # add some logic when generating the wasGenerated and used relationships.  If either node is in the 
    # unreferenced_node_dict, then ignore the relationship
        
    # now, connect the nodes
    for rel_record in record['relationships']:
        from_uuid = rel_record['fromNode']['uuid']
        to_uuid = rel_record['toNode']['uuid']
        from_node = node_dict[from_uuid]
        to_node = node_dict[to_uuid]
        if rel_record['rel_data']['type'] == 'HAS_METADATA':
            # assign the metadata node as the metadata attribute
            # just extract the provenance information from the metadata node
            
            entity_timestamp_json = get_json_timestamp(int(to_node['created_timestamp']))
            
            provenance_data = {
                PROV_GENERATED_TIME_ATTRIBUTE : entity_timestamp_json
            }

            type_code = None
            isEntity = True
            if 'entity_type' in from_node:
                type_code = from_node['entity_type']
            elif 'creation_action' in from_node:
                type_code = from_node['creation_action']
                isEntity = False
            label_text = None                                
            if 'submission_id' in from_node:
                label_text = from_node['submission_id']
            else:
                label_text = from_node['uuid']
                
            # build metadata attribute from the Metadata node
            metadata_attribute = {}
            for attribute_key in to_node:
                if attribute_key not in metadata_ignore_attributes:
                    if attribute_key in known_attribute_map:                                            
                        # special case: timestamps
                        if attribute_key == 'last_modified_timestamp':
                            provenance_data[known_attribute_map[attribute_key]] = get_json_timestamp(int(to_node[attribute_key]))
                    else: #add any extraneous data to the metadata attribute
                        metadata_attribute[attribute_key] = to_node[attribute_key]
                   
            # Need to add the agent and organization here, plus the appropriate relationships (between the entity and the agent plus orgainzation)
            agent_record = get_agent_record(to_node)
            agent_unique_id = str(agent_record[HUBMAP_PROV_USER_EMAIL]).replace('@', '-')
            agent_unique_id = str(agent_unique_id).replace('.', '-')
            if HUBMAP_PROV_USER_UUID in agent_record:
                agent_unique_id =agent_record[HUBMAP_PROV_USER_UUID]
            agent_uri = build_uri('hubmap','agent',agent_unique_id)
            organization_record = get_organization_record(to_node)
            organization_uri = build_uri('hubmap','organization',organization_record[HUBMAP_PROV_GROUP_UUID])
            doc_agent = None
            doc_org = None
            
            get_agent = prov_doc.get_record(agent_uri)
            # only add this once
            if len(get_agent) == 0:
                doc_agent = prov_doc.agent(agent_uri, agent_record)
            else:
                doc_agent = get_agent[0]

            get_org = prov_doc.get_record(organization_uri)
            # only add this once
            if len(get_org) == 0:
                doc_org = prov_doc.agent(organization_uri, organization_record)
            else:
                doc_org = get_org[0]
                      
            other_attributes = {
                PROV_LABEL_ATTRIBUTE : label_text,
                PROV_TYPE_ATTRIBUTE : type_code, 
                HUBMAP_DOI_ATTRIBUTE : from_node['doi'],
                HUBMAP_DISPLAY_DOI_ATTRIBUTE : from_node['hubmap_id'],
                HUBMAP_DISPLAY_IDENTIFIER_ATTRIBUTE : label_text, 
                HUBMAP_UUID_ATTRIBUTE : from_node['uuid']                                                    
            }

            # only add metadata if it contains data
            if len(metadata_attribute) > 0:
                other_attributes[HUBMAP_METADATA_ATTRIBUTE] = json.dumps(metadata_attribute)
            # add the provenance data to the other_attributes
            other_attributes.update(provenance_data)
            if isEntity == True:
                prov_doc.entity(build_uri('hubmap','entities',from_node['uuid']), other_attributes)
            else:
                activity_timestamp_json = get_json_timestamp(int(to_node['created_timestamp']))
                doc_activity = prov_doc.activity(build_uri('hubmap','activities',from_node['uuid']), activity_timestamp_json, activity_timestamp_json, other_attributes)
                prov_doc.actedOnBehalfOf(doc_agent, doc_org, doc_activity)
        elif rel_record['rel_data']['type'] in ['ACTIVITY_OUTPUT', 'ACTIVITY_INPUT']:
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
            
            if rel_record['rel_data']['type'] == 'ACTIVITY_OUTPUT':
                #prov_doc.wasGeneratedBy(entity, activity, time, identifier, other_attributes)
                prov_doc.wasGeneratedBy(to_node_uri, from_node_uri)

            if rel_record['rel_data']['type'] == 'ACTIVITY_INPUT':
                #prov_doc.used(activity, entity, time, identifier, other_attributes)
                prov_doc.used(to_node_uri, from_node_uri)
            
            # for now, simply create a "relation" where the fromNode's uuid is connected to a toNode's uuid via a relationship:
            # ex: {'fromNodeUUID': '42e10053358328c9079f1c8181287b6d', 'relationship': 'ACTIVITY_OUTPUT', 'toNodeUUID': '398400024fda58e293cdb435db3c777e'}
            rel_data_record = {'fromNodeUUID' : from_node['uuid'], 'relationship' : rel_record['rel_data']['type'], 'toNodeUUID' : to_node['uuid']}
            relation_list.append(rel_data_record)
    
    return_data = {'nodes' : node_dict, 'relations' : relation_list}  


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
        if group_uuid in self.groupsById:
            group_record = self.groupsById[group_uuid]
        else:
            raise LookupError('Cannot find group for uuid: ' + group_uuid)
    elif 'group_name' in node_data:
        group_name = node_data['group_name']
        if group_name in self.groupsByName:
            group_record = self.groupsByName[group_name]
        #handle the case where the group UUID is incorrectly stored in the name field:
        elif group_name in self.groupsById:
            group_record = self.groupsById[group_name]
        else:
            raise LookupError('Cannot find group for name: ' + group_name)
    for attribute_key in group_record:
        if attribute_key in organization_attribute_map:
            return_dict[organization_attribute_map[attribute_key]] = group_record[attribute_key]
    return_dict[PROV_TYPE] = 'prov:Organization'
    return return_dict
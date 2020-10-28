import neo4j
from neo4j import CypherError, TransactionError
import logging

logger = logging.getLogger(__name__)

# Use "entity" as the filed name of the single result record
record_field_name = "result"

####################################################################################################
## Activity creation
####################################################################################################

"""
Create a new activity node in neo4j

Parameters
----------
tx : neo4j transaction handler
    The neo4j transaction handler instance
json_list_str : string
    The string representation of a list containing only one entity to be created

Returns
-------
neo4j.node
    A neo4j node instance of the newly created entity node
"""
def create_activity_tx(tx, json_list_str):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = ("WITH apoc.convert.fromJsonList('{json_list_str}') AS activities_list " +
                           "UNWIND activities_list AS data " +
                           "CREATE (a:Activity) " +
                           "SET a = data " +
                           "RETURN a AS {record_field_name}")

    query = parameterized_query.format(json_list_str = json_list_str,
                                       record_field_name = record_field_name)

    logger.info("======create_activity_tx() query:======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    logger.info("======create_activity_tx() resulting node:======")
    logger.info(node)

    return node

####################################################################################################
## Entity retrival
####################################################################################################

"""
Get target entity dict

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
uuid : string
    The uuid of target entity 

Returns
-------
dict
    A dictionary of entity details returned from the Cypher query
"""
def get_entity(neo4j_driver, entity_class, uuid):
    nodes = []
    entity_dict = {}

    parameterized_query = ("MATCH (e:{entity_class}) " + 
                           "WHERE e.uuid = '{uuid}' " +
                           "RETURN e AS {record_field_name}")

    query = parameterized_query.format(entity_class = entity_class, 
                                       uuid = uuid, 
                                       record_field_name = record_field_name)
    
    with neo4j_driver.session() as session:
        try:
            results = session.run(query)

            # Add all records to the nodes list
            for record in results:
                nodes.append(record.get(record_field_name))
            
            logger.info("======get_entity() resulting nodes:======")
            logger.info(nodes)

            # Return an empty dict if no result
            if len(nodes) < 1:
                return entity_dict

            # Raise an exception if multiple nodes returned
            if len(nodes) > 1:
                message = "{num_nodes} entity nodes with same uuid {uuid} found in the Neo4j database."
                raise Exception(message.format(num_nodes = str(len(nodes)), uuid = uuid))
            
            # Convert the neo4j node into Python dict
            entity_dict = node_to_dict(nodes[0])

            logger.info("======get_entity() resulting entity_dict:======")
            logger.info(entity_dict)

            return entity_dict
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

####################################################################################################
## Called by trigger methogs
####################################################################################################

"""
Get the source uuid of a given derived entity's uuid

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
uuid : string
    The uuid of target entity 

Returns
-------
string
    The uuid of source entity
"""
def get_source_uuid(neo4j_driver, uuid):
    nodes = []
    entity_dict = {}

    parameterized_query = ("MATCH (s:Entity)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Entity) " + 
                           "WHERE t.uuid = '{uuid}' " +
                           "RETURN s.uuid AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    with neo4j_driver.session() as session:
        try:
            result = session.run(query)

            record = result.single()
            source_uuid = record[record_field_name]

            logger.info("======get_source_uuid() resulting source_uuid:======")
            logger.info(source_uuid)

            return source_uuid
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

"""
Get a list of associated dataset uuids for a given derived entity's uuid

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
uuid : string
    The uuid of target entity 

Returns
-------
list
    The list comtaining associated dataset uuids
"""
def get_dataset_uuids_by_collection(neo4j_driver, uuid):
    dataset_uuids_list = []

    parameterized_query = ("MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) " + 
                           "WHERE c.uuid = '{uuid}' " +
                           "RETURN DISTINCT e.uuid AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    with neo4j_driver.session() as session:
        try:
            results = session.run(query)

            for record in results:
                dataset_uuids_list.append(record.get(record_field_name))

            logger.info("======get_collection_dataset_uuids() resulting list:======")
            logger.info(dataset_uuids_list)

            return dataset_uuids_list
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e


####################################################################################################
## Entity creation
####################################################################################################

"""
Create a new entity node in neo4j

Parameters
----------
tx : neo4j transaction handler
    The neo4j transaction handler instance
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
json_list_str : string
    The string representation of a list containing only one entity to be created
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
neo4j.node
    A neo4j node instance of the newly created entity node
"""
def create_entity_tx(tx, entity_class, json_list_str):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = ("WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list " +
                           "UNWIND entities_list AS data " +
                           "CREATE (e:{entity_class}) " +
                           "SET e = data " +
                           "RETURN e AS {record_field_name}")

    query = parameterized_query.format(json_list_str = json_list_str, 
                                       entity_class = entity_class, 
                                       record_field_name = record_field_name)

    logger.info("======create_entity_tx() query:======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    logger.info("======create_entity_tx() resulting node:======")
    logger.info(node)

    return node

####################################################################################################
## Relationship creation
####################################################################################################

"""
Create a relationship from the source node to the target node in neo4j

Parameters
----------
tx : neo4j transaction handler
    The neo4j transaction handler instance
source_node_uuid : str
    The uuid of source node
target_node_uuid : str
    The uuid of target node
relationship : str
    The relationship type to be created
direction: str
    The relationship direction from source node to target node: outgoing `->` or incoming `<-`
    Neo4j CQL CREATE command supports only directional relationships


Returns
-------
string
    The relationship type name
"""
def create_relationship_tx(tx, source_node_uuid, target_node_uuid, relationship, direction):
    incoming = "-"
    outgoing = "-"
    
    if direction == "<-":
        incoming = direction

    if direction == "->":
        outgoing = direction

    parameterized_query = ("MATCH (s), (t) " +
                           "WHERE s.uuid = '{source_node_uuid}' AND t.uuid = '{target_node_uuid}' " +
                           "CREATE (s){incoming}[r:{relationship}]{outgoing}(t) " +
                           "RETURN type(r) AS {record_field_name}") 

    query = parameterized_query.format(source_node_uuid = source_node_uuid,
                                       target_node_uuid = target_node_uuid,
                                       incoming = incoming,
                                       relationship = relationship,
                                       outgoing = outgoing,
                                       record_field_name = record_field_name)

    logger.info("======create_relationship_tx() query:======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.info("======create_relationship_tx() resulting relationship:======")
    logger.info("(source node) " + incoming + " [:" + relationship + "] " + outgoing + " (target node)")

    return relationship

"""
Create relationships between the target Dataset node and Collection nodes in neo4j

Parameters
----------
tx : neo4j transaction handler
    The neo4j transaction handler instance
entity_dict : dict
    The dictionary of the target Dataset entity
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
string
    The relationship type name
"""
def create_dataset_collection_relationship_tx(tx, entity_dict, collection_uuids_list):
    collection_uuids_list_str = '[' + ', '.join(collection_uuids_list) + ']'
    parameterized_query = ("MATCH (e1:Dataset), (e2:Collection) " +
                           "WHERE e1.uuid = '{uuid}' AND e2.uuid IN {collection_uuids_list_str} " +
                           "CREATE (e1)-[r:IN_COLLECTION]->(e2) " +
                           "RETURN type(r) AS {record_field_name}") 

    query = parameterized_query.format(uuid = entity_dict1['uuid'],
                                       collection_uuids_list_str = collection_uuids_list_str,
                                       record_field_name = record_field_name)

    logger.info("======create_dataset_collection_relationship_tx() query:======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.info("======create_dataset_collection_relationship_tx() resulting relationship:======")
    logger.info(relationship)

    return relationship

"""
Create a new entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
entity_json_list_str : string
    The string representation of a list containing only one Entity node to be created
source_entity_uuid : str
    The uuid of the source entity
activity_json_list_str : string
    The string representation of a list containing only one Activity node to be created
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
dict
    A dictionary of newly created entity details returned from the Cypher query
"""
def create_entity(neo4j_driver, entity_class, entity_json_list_str, activity_json_list_str = None, source_entity_uuid = None, collection_uuids_list = None):
    entity_dict = {}

    with neo4j_driver.session(default_access_mode = neo4j.WRITE_ACCESS) as session:
        try:
            tx = session.begin_transaction()

            entity_node = create_entity_tx(tx, entity_class, entity_json_list_str)
            entity_dict = node_to_dict(entity_node)

            logger.info("======create_entity() resulting entity_dict:======")
            logger.info(entity_dict)
 
            # In the event of this entity is being derived from a source entity
            if activity_json_list_str and source_entity_uuid:
                # 1 - create the Acvitity node
                activity_dict = create_activity_tx(tx, activity_json_list_str)
                # 2 - create relationship from source Entity node to this Activity node
                create_relationship_tx(tx, source_entity_uuid, activity_dict['uuid'], 'ACTIVITY_INPUT', '->')
                # 3 - create relationship from this Activity node to the derived Enity node
                create_relationship_tx(tx, activity_dict['uuid'], entity_dict['uuid'], 'ACTIVITY_OUTPUT', '->')

            # For Dataset associated with Collections
            if collection_uuids_list:
                create_dataset_collection_relationship_tx(tx, entity_dict, collection_uuids_list)
            
            tx.commit()

            return entity_dict
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except TransactionError as te:
            logger.info("======create_entity() transaction error:======")
            logger.info(te.value)

            if tx.closed() == False:
                tx.rollback()

            raise TransactionError('Neo4j transaction error: create_entity()' + te.value)
        except Exception as e:
            raise e

####################################################################################################
## Entity update
####################################################################################################

"""
Update the properties of an existing entity node in neo4j

Parameters
----------
tx : neo4j transaction handler
    The neo4j transaction handler instance
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
json_list_str : string
    The string representation of a list containing only one entity to be created
uuid : string
    The uuid of target entity 

Returns
-------
neo4j.node
    A neo4j node instance of the updated entity node
"""
def update_entity_tx(tx, entity_class, json_list_str, uuid):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = ("WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list " +
                           "UNWIND entities_list AS data " +
                           "MATCH (e:{entity_class}) " +
                           "WHERE e.uuid='{uuid}' " +
                           "SET e = data RETURN e AS {record_field_name}")

    query = parameterized_query.format(json_list_str = json_list_str, 
                                       entity_class = entity_class, 
                                       uuid = uuid, 
                                       record_field_name = record_field_name)
    
    logger.info("======update_entity_tx() query:======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    logger.info("======update_entity_tx() resulting node:======")
    logger.info(node)

    return node

"""
Update the properties of an existing entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
json_list_str : string
    The string representation of a list containing only one entity to be created
uuid : string
    The uuid of target entity 

Returns
-------
dict
    A dictionary of updated entity details returned from the Cypher query
"""
def update_entity(neo4j_driver, entity_class, json_list_str, uuid):
    entity_dict = {}

    with neo4j_driver.session() as session:
        try:
            entity_node = session.write_transaction(update_entity_tx, entity_class, json_list_str, uuid)
            entity_dict = node_to_dict(entity_node)

            logger.info("======update_entity() resulting entity_dict:======")
            logger.info(entity_dict)

            return entity_dict
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

####################################################################################################
## Internal Functions
####################################################################################################

"""
Convert the neo4j node into Python dict

Parameters
----------
entity_node : neo4j.node
    The target neo4j node to be converted

Returns
-------
dict
    A dictionary of target entity containing all property key/value pairs
"""
def node_to_dict(entity_node):
    entity_dict = {}

    for key, value in entity_node._properties.items():
        entity_dict.setdefault(key, value)

    return entity_dict

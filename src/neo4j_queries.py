from neo4j import CypherError
import logging

logger = logging.getLogger(__name__)

# Use "entity" as the filed name of the single result record
record_field_name = "result"

####################################################################################################
## Entity retrival
####################################################################################################

"""
Get target entity dict

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
uuid : string
    The uuid of target entity 

Returns
-------
dict
    A dictionary of entity details returned from the Cypher query
"""
def get_entity(neo4j_driver, entity_type, uuid):
    nodes = []
    entity_dict = {}

    parameterized_query = ("MATCH (e:{entity_type}) " + 
                           "WHERE e.uuid='{uuid}' " +
                           "RETURN e AS {record_field_name}")

    query = parameterized_query.format(entity_type = entity_type, 
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
## Entity creation
####################################################################################################

"""
Create a new entity node in neo4j

Parameters
----------
tx : neo4j transaction handler
    The neo4j transaction handler instance
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
json_list_str : string
    The string representation of a list containing only one entity to be created
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
neo4j.node
    A neo4j node instance of the newly created entity node
"""
def create_entity_tx(tx, entity_type, json_list_str):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = ("WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list " +
                           "UNWIND entities_list AS data " +
                           "CREATE (e:{entity_type}) " +
                           "SET e = data " +
                           "RETURN e AS {record_field_name}")

    query = parameterized_query.format(json_list_str = json_list_str, 
                                       entity_type = entity_type, 
                                       record_field_name = record_field_name)

    logger.info("======create_entity_tx() query:======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    logger.info("======create_entity_tx() resulting node:======")
    logger.info(node)

    return node

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
    parameterized_query = ("MATCH (e1:Dataset), (e2:Collection)" +
                           "WHERE e1.uuid = {uuid} AND e2.uuid IN {collection_uuids_list}" +
                           "CREATE (e1)-[r:IN_COLLECTION]->(e2)" +
                           "RETURN type(r) AS {record_field_name}") 

    query = parameterized_query.format(uuid = entity_dict1['uuid'],
                                       collection_uuids_list = collection_uuids_list,
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
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
json_list_str : string
    The string representation of a list containing only one entity to be created

Returns
-------
dict
    A dictionary of newly created entity details returned from the Cypher query
"""
def create_entity(neo4j_driver, entity_type, json_list_str, collection_uuids_list):
    entity_dict = {}

    with neo4j_driver.session(default_access_mode = neo4j.WRITE_ACCESS) as session:
        try:
            tx = session.begin_transaction()

            entity_node = create_entity_tx(tx, entity_type, json_list_str)
            entity_dict = node_to_dict(entity_node)

            logger.info("======create_entity() resulting entity_dict:======")
            logger.info(entity_dict)
 
            # For Dataset associated with Collections
            if collection_uuids_list:
                create_dataset_collection_relationship_tx(tx, entity_dict, collection_uuids_list)
            
            tx.commit()
            tx.close()

            return entity_dict
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
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
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
json_list_str : string
    The string representation of a list containing only one entity to be created
uuid : string
    The uuid of target entity 

Returns
-------
neo4j.node
    A neo4j node instance of the updated entity node
"""
def update_entity_tx(tx, entity_type, json_list_str, uuid):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = ("WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list " +
                           "UNWIND entities_list AS data " +
                           "MATCH (e:{entity_type}) " +
                           "WHERE e.uuid='{uuid}' " +
                           "SET e = data RETURN e AS {record_field_name}")

    query = parameterized_query.format(json_list_str = json_list_str, 
                                       entity_type = entity_type, 
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
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
json_list_str : string
    The string representation of a list containing only one entity to be created
uuid : string
    The uuid of target entity 

Returns
-------
dict
    A dictionary of updated entity details returned from the Cypher query
"""
def update_entity(neo4j_driver, entity_type, json_list_str, uuid):
    entity_dict = {}

    with neo4j_driver.session() as session:
        try:
            entity_node = session.write_transaction(update_entity_tx, entity_type, json_list_str, uuid)
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

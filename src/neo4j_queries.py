from neo4j import CypherError
import logging

logger = logging.getLogger(__name__)

# Use "entity" as the filed name of the single result record
record_field_name = "entity"

####################################################################################################
## Entity retrival
####################################################################################################

# id is either a `uuid` or `hubmap_id` (like HBM123.ABCD.987)
def get_entity(neo4j_driver, entity_type, id):
    nodes = []
    entity_dict = {}

    parameterized_query = ("MATCH (e:{entity_type}) " + 
                           "WHERE e.uuid='{id}' OR e.hubmap_id='{id}' " +
                           "RETURN e AS {record_field_name}")

    query = parameterized_query.format(entity_type = entity_type, 
                                       id = id, 
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
                message = "{num_nodes} entity nodes with same id {id} found in the Neo4j database."
                raise Exception(message.format(num_nodes = str(len(nodes)), id = id))
            
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

def create_entity_tx(tx, entity_type, json_list_str):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = ("WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list " +
                           "UNWIND entities_list AS data " +
                           "CREATE (e:{entity_type}) " +
                           "SET e = data RETURN e AS {record_field_name}")

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

def create_entity(neo4j_driver, entity_type, json_list_str):
    entity_dict = {}

    with neo4j_driver.session() as session:
        try:
            entity_node = session.write_transaction(create_entity_tx, entity_type, json_list_str)
            entity_dict = node_to_dict(entity_node)

            logger.info("======create_entity() resulting entity_dict:======")
            logger.info(entity_dict)

            return entity_dict
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

####################################################################################################
## Entity update
####################################################################################################

def update_entity_tx(tx, entity_type, json_list_str, id):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = ("WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list " +
                           "UNWIND entities_list AS data " +
                           "MATCH (e:{entity_type}) " +
                           "WHERE e.uuid='{id}' OR e.hubmap_id='{id}' " +
                           "SET e = data RETURN e AS {record_field_name}")

    query = parameterized_query.format(json_list_str = json_list_str, 
                                       entity_type = entity_type, 
                                       id = id, 
                                       record_field_name = record_field_name)
    
    logger.info("======update_entity_tx() query:======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    logger.info("======update_entity_tx() resulting node:======")
    logger.info(node)

    return node

def update_entity(neo4j_driver, entity_type, json_list_str, id):
    entity_dict = {}

    with neo4j_driver.session() as session:
        try:
            entity_node = session.write_transaction(update_entity_tx, entity_type, json_list_str, id)
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

# Convert the neo4j node into Python dict
def node_to_dict(entity_node):
    entity_dict = {}

    for key, value in entity_node._properties.items():
        entity_dict.setdefault(key, value)

    return entity_dict

from neo4j import CypherError
import logging

logger = logging.getLogger(__name__)

# Use "entity" as the filed name of the result
result_field_name = "entity"

# id is either a `uuid` or `hubmap_id` (like HBM123.ABCD.987)
def get_entity(neo4j_driver, entity_type, id):
    parameterized_query = "MATCH (e:{entity_type}) WHERE e.uuid='{id}' OR e.hubmap_id='{id}' RETURN e AS {result_field_name}"
    query = parameterized_query.format(entity_type = entity_type, id = id, result_field_name = result_field_name)
    
    nodes = []
    entity = {}

    with neo4j_driver.session() as session:
        try:
            results = session.run(query)

            # Add all records to the nodes list
            for record in results:
                nodes.append(record.get(result_field_name))
            
            logger.info("======Resulting nodes:======")
            logger.info(nodes)

            # Return an empty dict if no result
            if len(nodes) < 1:
                return entity

            # Raise an exception if multiple nodes returned
            if len(nodes) > 1:
                raise Exception(str(len(nodes)) + " entity nodes with same id found in the Neo4j database.")
            
            # If all good
            # Get all properties of the target node
            for key, value in nodes[0]._properties.items():
                entity.setdefault(key, value)

            # Return the entity dict
            logger.info("======Resulting entity:======")
            logger.info(entity)

            return entity
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

def create_entity_tx(tx, entity_type, json_list_str):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = "WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list UNWIND entities_list AS data CREATE (e:{entity_type}) SET e = data RETURN e AS {result_field_name}"
    query = parameterized_query.format(json_list_str = json_list_str, entity_type = entity_type, result_field_name = result_field_name)

    result = tx.run(query)
    record = result.single()

    return record

def create_entity(neo4j_driver, entity_type, json_list_str):
    with neo4j_driver.session() as session:
        try:
            entity = session.write_transaction(create_entity_tx, entity_type, json_list_str)

            # Return the entity dict
            logger.info("======Resulting entity:======")
            logger.info(entity)

            return entity
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

def update_entity_tx(tx, entity_type, json_list_str, id):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = "WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list UNWIND entities_list AS data MATCH (e:{entity_type}) WHERE e.uuid='{id}' OR e.hubmap_id='{id}' SET e = data RETURN e AS {result_field_name}"
    query = parameterized_query.format(json_list_str = json_list_str, entity_type = entity_type, id = id, result_field_name = result_field_name)
    
    result = tx.run(query)
    record = result.single()

    return record

def update_entity(neo4j_driver, entity_type, json_list_str, id):
    with neo4j_driver.session() as session:
        try:
            entity = session.write_transaction(create_entity_tx, entity_type, json_list_str, id)

            # Return the entity dict
            logger.info("======Resulting entity:======")
            logger.info(entity)

            return entity
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e


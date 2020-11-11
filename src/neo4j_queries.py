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
json_list_str : str
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
uuid : str
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
    
    logger.info("======get_entity() query:======")
    logger.info(query)

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
Get the source entities uuids of a given derived entity (Dataset/Donor/Sample) by uuid

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
uuid : str
    The uuid of target entity 

Returns
-------
list
    A unique list of uuids of source entities
"""
def get_source_uuids(neo4j_driver, uuid):
    parameterized_query = ("MATCH (s:Entity)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Entity) " + 
                           "WHERE t.uuid = '{uuid}' " +
                           "RETURN apoc.coll.toSet(COLLECT(s.uuid)) AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    logger.info("======get_source_uuids() query:======")
    logger.info(query)

    with neo4j_driver.session() as session:
        try:
            result = session.run(query)

            record = result.single()
            source_uuids = record[record_field_name]

            logger.info("======get_source_uuids() resulting source_uuids list:======")
            logger.info(source_uuids)

            return source_uuids
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
uuid : str
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
    
    logger.info("======get_dataset_uuids_by_collection() query:======")
    logger.info(query)

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


"""
Get count of published Dataset in the provenance hierarchy for a given  Collection/Sample/Donor

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
entity_class : str
    One of the normalized entity classes: Collection, Sample, Donor
uuid : str
    The uuid of target entity 

Returns
-------
int
    The count of published Dataset in the provenance hierarchy 
    below the target entity (Donor, Sample and Collection)
"""
def count_attached_published_datasets(neo4j_driver, entity_class, uuid):
    with neo4j_driver.session() as session:
        parameterized_query = ("MATCH (e:{entity_class})-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(d:Dataset) " +
                               "WHERE e.uuid='{uuid}' AND d.status = 'Published' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN COUNT(d) AS {record_field_name}")

        query = parameterized_query.format(entity_class = entity_class,
        	                               uuid = uuid, 
                                           record_field_name = record_field_name)

        logger.info("======count_attached_published_datasets() query:======")
        logger.info(query)

        try:
            result = session.run(query)
            record = result.single()
            count = record[record_field_name]

            logger.info("======count_attached_published_datasets() resulting count:======")
            logger.info(count)
            
            return count               
        except CypherError as ce:
            logger.error("======count_attached_published_datasets() Cypher error:======")
            logger.error(ce)

            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e


    dataset_uuids_list = []

    parameterized_query = ("MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) " + 
                           "WHERE c.uuid = '{uuid}' " +
                           "RETURN DISTINCT e.uuid AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    logger.info("======get_dataset_uuids_by_collection() query:======")
    logger.info(query)

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
json_list_str : str
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
str
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
str
    The relationship type name
"""
def create_dataset_collection_relationships_tx(tx, entity_dict, collection_uuids_list):
    collection_uuids_list_str = '[' + ', '.join(collection_uuids_list) + ']'
    parameterized_query = ("MATCH (e1:Dataset), (e2:Collection) " +
                           "WHERE e1.uuid = '{uuid}' AND e2.uuid IN {collection_uuids_list_str} " +
                           "CREATE (e1)-[r:IN_COLLECTION]->(e2) " +
                           "RETURN type(r) AS {record_field_name}") 

    query = parameterized_query.format(uuid = entity_dict1['uuid'],
                                       collection_uuids_list_str = collection_uuids_list_str,
                                       record_field_name = record_field_name)

    logger.info("======create_dataset_collection_relationships_tx() query:======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.info("======create_dataset_collection_relationships_tx() resulting relationship:======")
    logger.info(relationship)

    return relationship


"""
Create a new entity node (ans also links to existing Collection nodes if provided) in neo4j

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
entity_json_list_str : str
    The string representation of a list containing only one Entity node to be created
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
dict
    A dictionary of newly created entity details returned from the Cypher query
"""
def create_entity(neo4j_driver, entity_class, entity_json_list_str, collection_uuids_list = None):
    entity_dict = {}

    with neo4j_driver.session(default_access_mode = neo4j.WRITE_ACCESS) as session:
        try:
            tx = session.begin_transaction()

            entity_node = create_entity_tx(tx, entity_class, entity_json_list_str)
            entity_dict = node_to_dict(entity_node)

            logger.info("======create_entity() resulting entity_dict:======")
            logger.info(entity_dict)
 
            # For Dataset associated with Collections
            if collection_uuids_list:
                create_dataset_collection_relationships_tx(tx, entity_dict, collection_uuids_list)
            
            tx.commit()

            return entity_dict
        except CypherError as ce:
            logger.error("======create_entity() Cypher error:======")
            logger.error(ce)

            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except TransactionError as te:
            logger.error("======create_entity() transaction error:======")
            logger.error(te.value)

            if tx.closed() == False:
                logger.info("create_entity() transaction failed, rollback")

                tx.rollback()

            raise TransactionError('Neo4j transaction error: create_entity()' + te.value)
        except Exception as e:
            raise e


"""
Create a derived entity node and link to source entity node via Activity node and links in neo4j

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
entity_json_list_str : str
    The string representation of a list containing only one Entity node to be created
source_entities_list : list (of dictionaries)
    The list of source entities if the format of:
    [
        {"class": "Sample", "uuid": "6dada44324234"},
        {"class": "Sample", "uuid": "34dad6adsd230"},
        ...
    ]
activity_json_list_str : str
    The string representation of a list containing only one Activity node to be created
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
dict
    A dictionary of newly created derived entity details returned from the Cypher query
"""
def create_derived_entity(neo4j_driver, entity_class, entity_json_list_str, activity_json_list_str, source_entities_list, collection_uuids_list = None):
    entity_dict = {}

    with neo4j_driver.session(default_access_mode = neo4j.WRITE_ACCESS) as session:
        try:
            tx = session.begin_transaction()

            entity_node = create_entity_tx(tx, entity_class, entity_json_list_str)
            entity_dict = node_to_dict(entity_node)

            logger.info("======create_derived_entity() resulting entity_dict:======")
            logger.info(entity_dict)

            # Create the Acvitity node
            activity_dict = create_activity_tx(tx, activity_json_list_str)

            # Link each source entity to the newly created Activity node
            for source_entity in source_entities_list:
                # Create relationship from source Entity node to this Activity node
                create_relationship_tx(tx, source_entity['uuid'], activity_dict['uuid'], 'ACTIVITY_INPUT', '->')
                
            # Create relationship from this Activity node to the derived Enity node
            create_relationship_tx(tx, activity_dict['uuid'], entity_dict['uuid'], 'ACTIVITY_OUTPUT', '->')

            # For Dataset associated with Collections
            if collection_uuids_list:
                create_dataset_collection_relationship_tx(tx, entity_dict, collection_uuids_list)
            
            tx.commit()

            return entity_dict
        except CypherError as ce:
            logger.error("======create_derived_entity() Cypher error:======")
            logger.error(ce)

            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except TransactionError as te:
            logger.error("======create_derived_entity() transaction error:======")
            logger.error(te.value)

            if tx.closed() == False:
                logger.info("create_derived_entity() transaction failed, rollback")

                tx.rollback()

            raise TransactionError('Neo4j transaction error: create_derived_entity()' + te.value)
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
json_list_str : str
    The string representation of a list containing only one entity to be created
uuid : str
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
                           "WHERE e.uuid = '{uuid}' " +
                           # https://neo4j.com/docs/cypher-manual/current/clauses/set/#set-setting-properties-using-map
                           # `+=` is the magic here:
                           # Any properties in the map that are not on the node will be added.
                           # Any properties not in the map that are on the node will be left as is.
                           # Any properties that are in both the map and the node will be replaced in the node. However, if any property in the map is null, it will be removed from the node.
                           "SET e += data RETURN e AS {record_field_name}")

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
json_list_str : str
    The string representation of a list containing only one entity to be created
uuid : str
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
            logger.error("======update_entity() Cypher error:======")
            logger.error(ce)

            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e


"""
Get all ancestors by uuid

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
uuid : str
    The uuid of target entity 

Returns
-------
list
    A list of unique ancestor dictionaries returned from the Cypher query
"""
def get_ancestors(neo4j_driver, uuid):
    with neo4j_driver.session() as session:
        ancestors = []

        parameterized_query = ("MATCH (e:Entity)<-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]-(ancestor:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(ancestor)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

        logger.info("======get_ancestors() query:======")
        logger.info(query)

        try:
            result = session.run(query)
            record = result.single()
            entity_nodes = record[record_field_name]

            for entity_node in entity_nodes:
                entity_dict = node_to_dict(entity_node)
                ancestors.append(entity_dict)

            return ancestors               
        except CypherError as ce:
            logger.error("======get_ancestors() Cypher error:======")
            logger.error(ce)

            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e


"""
Get all descendants by uuid

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
uuid : str
    The uuid of target entity 

Returns
-------
dict
    A list of unique desendant dictionaries returned from the Cypher query
"""
def get_descendants(neo4j_driver, uuid):
    with neo4j_driver.session() as session:
        descendants = []

        parameterized_query = ("MATCH (e:Entity)-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(descendant:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(descendant)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

        logger.info("======get_descendants() query:======")
        logger.info(query)

        try:
            result = session.run(query)
            record = result.single()
            entity_nodes = record[record_field_name]

            for entity_node in entity_nodes:
                entity_dict = node_to_dict(entity_node)
                descendants.append(entity_dict)

            return descendants               
        except CypherError as ce:
            logger.error("======get_descendants() Cypher error:======")
            logger.error(ce)

            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

"""
Get all parents by uuid

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
uuid : str
    The uuid of target entity 

Returns
-------
dict
    A list of unique parent dictionaries returned from the Cypher query
"""
def get_parents(neo4j_driver, uuid):
    with neo4j_driver.session() as session:
        parents = []

        parameterized_query = ("MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(parent)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

        logger.info("======get_parents() query:======")
        logger.info(query)

        try:
            result = session.run(query)
            record = result.single()
            entity_nodes = record[record_field_name]

            for entity_node in entity_nodes:
                entity_dict = node_to_dict(entity_node)
                parents.append(entity_dict)

            return parents               
        except CypherError as ce:
            logger.error("======get_parents() Cypher error:======")
            logger.error(ce)

            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

"""
Get all children by uuid

Parameters
----------
neo4j_driver : neo4j.driver
    The neo4j driver instance
uuid : str
    The uuid of target entity 

Returns
-------
dict
    A list of unique child dictionaries returned from the Cypher query
"""
def get_children(neo4j_driver, uuid):
    with neo4j_driver.session() as session:
        children = []

        parameterized_query = ("MATCH (e:Entity)-[:ACTIVITY_INPUT]->(:Activity)-[:ACTIVITY_OUTPUT]->(child:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(child)) AS {record_field_name}")
        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

        logger.info("======get_children() query:======")
        logger.info(query)

        try:
            result = session.run(query)
            record = result.single()
            entity_nodes = record[record_field_name]

            for entity_node in entity_nodes:
                entity_dict = node_to_dict(entity_node)
                children.append(entity_dict)

            return children               
        except CypherError as ce:
            logger.error("======get_children() Cypher error:======")
            logger.error(ce)
            
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

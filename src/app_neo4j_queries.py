from neo4j.exceptions import CypherSyntaxError, TransactionError
import logging

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'

"""
Check neo4j connectivity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool

Returns
-------
bool
    True if is connected, otherwise error
"""
def check_connection(neo4j_driver):
    parameterized_query = ("RETURN 1 AS {record_field_name}")
    query = parameterized_query.format(record_field_name = record_field_name)

    try:
        # Sessions will often be created and destroyed using a with block context
        with neo4j_driver.session() as session:
            result = session.run(query)
            record = result.single()
            int_value = record[record_field_name]
            
            if int_value == 1:
                logger.info("Neo4j is connected :)")
                return True

            logger.info("Neo4j is NOT connected :(")
            return False
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling check_connection(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e


####################################################################################################
## Activity creation
####################################################################################################

"""
Create a new activity node in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
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

    logger.debug("======create_activity_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    logger.debug("======create_activity_tx() resulting node======")
    logger.debug(node)

    return node

####################################################################################################
## Entity retrival
####################################################################################################

"""
Get target entity dict

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 

Returns
-------
dict
    A dictionary of entity details returned from the Cypher query
"""
def get_entity(neo4j_driver, uuid):
    parameterized_query = ("MATCH (e:Entity) " + 
                           "WHERE e.uuid = '{uuid}' " +
                           "RETURN e AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    logger.debug("======get_entity() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            nodes = []
            entity_dict = {}

            results = session.run(query)

            # Add all records to the nodes list
            for record in results:
                nodes.append(record.get(record_field_name))
            
            logger.debug("======get_entity() resulting nodes======")
            logger.debug(nodes)

            # Return an empty dict if no result
            if len(nodes) < 1:
                return entity_dict

            # Raise an exception if multiple nodes returned
            if len(nodes) > 1:
                message = "{num_nodes} entity nodes with same uuid {uuid} found in the Neo4j database."
                raise Exception(message.format(num_nodes = str(len(nodes)), uuid = uuid))
            
            # Convert the neo4j node into Python dict
            entity_dict = node_to_dict(nodes[0])

            logger.debug("======get_entity() resulting entity_dict======")
            logger.debug(entity_dict)

            return entity_dict
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_entity(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e

"""
Get all the entity nodes for the given entity class

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of entity dicts of the given class returned from the Cypher query
"""
def get_entities_by_class(neo4j_driver, entity_class, property_key = None):
    if property_key:
        parameterized_query = ("MATCH (e:{entity_class}) " + 
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")

        query = parameterized_query.format(entity_class = entity_class, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:{entity_class}) " + 
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

        query = parameterized_query.format(entity_class = entity_class, 
                                           record_field_name = record_field_name)
    
    logger.info("======get_entities_by_class() query======")
    logger.info(query)

    try:
        with neo4j_driver.session() as session:
            result = session.run(query)
            record = result.single()
            
            result_list = []

            if property_key:
                # Just return the list of property values from each entity node
                result_list = record[record_field_name]
            else:
                # Convert the entity nodes to dicts
                nodes = record[record_field_name]

                for node in nodes:
                    entity_dict = node_to_dict(node)
                    result_list.append(entity_dict)

            return result_list
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_entities_by_class(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e


####################################################################################################
## Entity creation
####################################################################################################

"""
Create a new entity node in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
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

    logger.debug("======create_entity_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    logger.debug("======create_entity_tx() resulting node======")
    logger.debug(node)

    return node

####################################################################################################
## Relationship creation
####################################################################################################

"""
Create a relationship from the source node to the target node in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
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

    logger.debug("======create_relationship_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.debug("======create_relationship_tx() resulting relationship======")
    logger.debug("(source node) " + incoming + " [:" + relationship + "] " + outgoing + " (target node)")

    return relationship

"""
Create relationships between the target Dataset node and Collection nodes in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
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

    logger.debug("======create_dataset_collection_relationships_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.debug("======create_dataset_collection_relationships_tx() resulting relationship======")
    logger.debug(relationship)

    return relationship


"""
Create a new entity node (ans also links to existing Collection nodes if provided) in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
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
    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            entity_node = create_entity_tx(tx, entity_class, entity_json_list_str)
            entity_dict = node_to_dict(entity_node)

            logger.debug("======create_entity() resulting entity_dict======")
            logger.debug(entity_dict)

            # For Dataset associated with Collections
            if collection_uuids_list:
                logger.info("Create relationships between target dataset and associated collections")

                create_dataset_collection_relationships_tx(tx, entity_dict, collection_uuids_list)
            
            tx.commit()

            return entity_dict
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling create_entity(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except TransactionError as te:
        msg = "TransactionError from calling create_entity(): " + te.value
        logger.error(msg)

        if tx.closed() == False:
            logger.info("Failed to commit create_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)
    except Exception as e:
        raise e


"""
Create a derived entity node and link to source entity node via Activity node and links in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
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
    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            entity_node = create_entity_tx(tx, entity_class, entity_json_list_str)
            entity_dict = node_to_dict(entity_node)

            logger.debug("======create_derived_entity() resulting entity_dict======")
            logger.debug(entity_dict)

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
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling create_derived_entity(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except TransactionError as te:
        msg = "TransactionError from calling create_derived_entity(): " + te.value
        logger.error(msg)

        if tx.closed() == False:
            logger.error("Failed to commit create_derived_entity() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)
    except Exception as e:
        raise e


####################################################################################################
## Entity update
####################################################################################################

"""
Update the properties of an existing entity node in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
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
    
    logger.debug("======update_entity_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    logger.debug("======update_entity_tx() resulting node======")
    logger.debug(node)

    return node

"""
Update the properties of an existing entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
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
    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            entity_node = session.write_transaction(update_entity_tx, entity_class, json_list_str, uuid)
            entity_dict = node_to_dict(entity_node)

            logger.debug("======update_entity() resulting entity_dict======")
            logger.debug(entity_dict)

            return entity_dict
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling update_entity()" + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e


"""
Get all ancestors by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of unique ancestor dictionaries returned from the Cypher query
"""
def get_ancestors(neo4j_driver, uuid, property_key = None):
    if property_key:
        parameterized_query = ("MATCH (e:Entity)<-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]-(ancestor:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(ancestor.{property_key})) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)<-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]-(ancestor:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(ancestor)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_ancestors() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            result = session.run(query)
            record = result.single()

            result_list = []

            if property_key:
                # Just return the list of property values from each entity node
                result_list = record[record_field_name]
            else:
                # Convert the entity nodes to dicts
                nodes = record[record_field_name]

                for node in nodes:
                    entity_dict = node_to_dict(node)
                    result_list.append(entity_dict)

            return result_list               
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_ancestors()" + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e


"""
Get all descendants by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique desendant dictionaries returned from the Cypher query
"""
def get_descendants(neo4j_driver, uuid, property_key = None):
    if property_key:
        parameterized_query = ("MATCH (e:Entity)-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(descendant:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(descendant.{property_key})) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(descendant:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(descendant)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)
    logger.debug("======get_descendants() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            result = session.run(query)
            record = result.single()

            result_list = []

            if property_key:
                # Just return the list of property values from each entity node
                result_list = record[record_field_name]
            else:
                # Convert the entity nodes to dicts
                nodes = record[record_field_name]

                for node in nodes:
                    entity_dict = node_to_dict(node)
                    result_list.append(entity_dict)

            return result_list               
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_descendants(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e

"""
Get all parents by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique parent dictionaries returned from the Cypher query
"""
def get_parents(neo4j_driver, uuid, property_key = None):
    if property_key:
        parameterized_query = ("MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(parent.{property_key})) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(parent)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_parents() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            result = session.run(query)
            record = result.single()

            result_list = []

            if property_key:
                # Just return the list of property values from each entity node
                result_list = record[record_field_name]
            else:
                # Convert the entity nodes to dicts
                nodes = record[record_field_name]

                for node in nodes:
                    entity_dict = node_to_dict(node)
                    result_list.append(entity_dict)

            return result_list               
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_parents(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e

"""
Get all children by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A list of unique child dictionaries returned from the Cypher query
"""
def get_children(neo4j_driver, uuid, property_key = None):
    if property_key:
        parameterized_query = ("MATCH (e:Entity)-[:ACTIVITY_INPUT]->(:Activity)-[:ACTIVITY_OUTPUT]->(child:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(child.{property_key})) AS {record_field_name}")
        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)-[:ACTIVITY_INPUT]->(:Activity)-[:ACTIVITY_OUTPUT]->(child:Entity) " +
                               "WHERE e.uuid='{uuid}' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(child)) AS {record_field_name}")
        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_children() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            result = session.run(query)
            record = result.single()
            result_list = []

            if property_key:
                # Just return the list of property values from each entity node
                result_list = record[record_field_name]
            else:
                # Convert the entity nodes to dicts
                nodes = record[record_field_name]

                for node in nodes:
                    entity_dict = node_to_dict(node)
                    result_list.append(entity_dict)

            return result_list             
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_children(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
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

from neo4j.exceptions import TransactionError
import logging

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'


####################################################################################################
## Directly called by app.py
####################################################################################################

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




"""
Create a new entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
entity_json_list_str : str
    The string representation of a list containing only one Entity node to be created

Returns
-------
dict
    A dictionary of newly created entity details returned from the Cypher query
"""
def create_entity(neo4j_driver, entity_class, entity_json_list_str):
    # UNWIND expects json.entities to be List<T>
    parameterized_query = ("WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list " +
                           "UNWIND entities_list AS data " +
                           "CREATE (e:{entity_class}) " +
                           "SET e = data " +
                           "RETURN e AS {record_field_name}")

    query = parameterized_query.format(json_list_str = json_list_str, 
                                       entity_class = entity_class, 
                                       record_field_name = record_field_name)

    logger.debug("======create_entity() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            result = tx.run(query)
            record = result.single()
            entity_node = record[record_field_name]

            entity_dict = node_to_dict(entity_node)

            logger.debug("======create_entity() resulting entity_dict======")
            logger.debug(entity_dict)

            tx.commit()

            return entity_dict
    except TransactionError as te:
        msg = "TransactionError from calling create_entity(): " + te.value
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit create_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


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
    
    logger.debug("======update_entity() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}
            
            tx = session.begin_transaction()
            
            result = tx.run(query)
            record = result.single()
            entity_node = record[record_field_name]

            tx.commit()

            entity_dict = node_to_dict(entity_node)

            logger.debug("======update_entity() resulting entity_dict======")
            logger.debug(entity_dict)

            return entity_dict
    except TransactionError as te:
        msg = "TransactionError from calling create_entity(): " + te.value
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit update_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)



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
                               # Filter out the Lab entities
                               "WHERE e.uuid='{uuid}' AND ancestor.entity_class <> 'Lab' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(ancestor.{property_key})) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)<-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]-(ancestor:Entity) " +
                               # Filter out the Lab entities
                               "WHERE e.uuid='{uuid}' AND ancestor.entity_class <> 'Lab' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(ancestor)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_ancestors() query======")
    logger.debug(query)

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
                               # Filter out the Lab entities
                               "WHERE e.uuid='{uuid}' AND desendant.entity_class <> 'Lab' " +
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
                               # Filter out the Lab entities
                               "WHERE e.uuid='{uuid}' AND parent.entity_class <> 'Lab' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(parent.{property_key})) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) " +
                               # Filter out the Lab entities
                               "WHERE e.uuid='{uuid}' AND parent.entity_class <> 'Lab' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(parent)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_parents() query======")
    logger.debug(query)

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
                               # Filter out the Lab entities
                               "WHERE e.uuid='{uuid}' AND child.entity_class <> 'Lab' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(child.{property_key})) AS {record_field_name}")
        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)-[:ACTIVITY_INPUT]->(:Activity)-[:ACTIVITY_OUTPUT]->(child:Entity) " +
                               # Filter out the Lab entities
                               "WHERE e.uuid='{uuid}' AND child.entity_class <> 'Lab' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN apoc.coll.toSet(COLLECT(child)) AS {record_field_name}")
        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_children() query======")
    logger.debug(query)

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


"""
Link the datasets to the target collection

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
collection_uuid : str
    The uuid of target collection 
dataset_uuids_list : list
    A list of dataset uuids to be linked to collection
"""
def add_datasets_to_collection(neo4j_driver, collection_uuid, dataset_uuids_list):
    # Join the list of uuids and wrap each string in single quote
    joined_str = ', '.join("'{0}'".format(dataset_uuid) for dataset_uuid in dataset_uuids_list)
    # Format a string to be used in Cypher query.
    # E.g., ['fb6757b606ac35be7fa85062fde9c2e1', 'ku0gd44535be7fa85062fde98gt5']
    dataset_uuids_list_str = '[' + joined_str + ']'

    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Create relationships between the target Collection and the given Datasets")

            parameterized_query = ("MATCH (c:Collection), (d:Dataset) " +
                                   "WHERE c.uuid = '{uuid}' AND d.uuid IN {dataset_uuids_list_str} " +
                                   # Use MERGE instead of CREATE to avoid creating the relationship multiple times
                                   # MERGE creates the relationship only if there is no existing relationship
                                   "MERGE (c)<-[r:IN_COLLECTION]-(d)") 

            query = parameterized_query.format(uuid = collection_uuid,
                                               dataset_uuids_list_str = dataset_uuids_list_str)

            logger.debug("======add_datasets_to_collection() query======")
            logger.debug(query)

            tx.run(query)
            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling add_datasets_to_collection(): " + te.value
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit add_datasets_to_collection() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


####################################################################################################
## Internal Functions
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

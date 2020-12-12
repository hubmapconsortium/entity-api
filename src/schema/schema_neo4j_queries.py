from neo4j.exceptions import TransactionError
import logging

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'

####################################################################################################
## Called by trigger methogs
####################################################################################################

"""
Unlink the linkages between the target entity and its direct ancestors

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 
"""
def unlink_entity_to_direct_ancestors(neo4j_driver, uuid):
    parameterized_query = ("MATCH (s:Entity)-[in:ACTIVITY_INPUT]->(a:Activity)-[out:ACTIVITY_OUTPUT]->(t:Entity) " + 
                           "WHERE t.uuid = '{uuid}' " +
                           # Delete the Activity node and in input/out relationships
                           "DELETE in, a, out")

    query = parameterized_query.format(uuid = uuid)

    logger.debug("======unlink_entity_to_direct_ancestors() query======")
    logger.debug(query)

    try:
        # Sessions will often be created and destroyed using a with block context
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            tx.run(query)
            tx.commit()
    except TransactionError:
        msg = "TransactionError from calling unlink_entity_to_direct_ancestors(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit unlink_entity_to_direct_ancestors() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Get the direct ancestors uuids of a given dataset by uuid

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
    A unique list of uuids of source entities
"""
def get_dataset_direct_ancestors(neo4j_driver, uuid, property_key = None):
    if property_key:
        parameterized_query = ("MATCH (s:Dataset)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Entity) " + 
                               "WHERE t.uuid = '{uuid}' " +
                               "RETURN apoc.coll.toSet(COLLECT(s.{property_key})) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (s:Dataset)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Entity) " + 
                               "WHERE t.uuid = '{uuid}' " +
                               "RETURN apoc.coll.toSet(COLLECT(s)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_dataset_direct_ancestors() query======")
    logger.debug(query)

    # Sessions will often be created and destroyed using a with block context
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
Create a linkage (via Activity node) between the target entity node and the ancestor entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target entity
ancestor_uuid : str
    The uuid of ancestor entity
activity_json_list_str : str
    The string representation of a list containing only one Activity node to be created
"""
def link_entity_to_direct_ancestor(neo4j_driver, entity_uuid, ancestor_uuid, activity_json_list_str):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # Create the Acvitity node
            activity_dict = create_activity_tx(tx, activity_json_list_str)

            # Create relationship from ancestor entity node to this Activity node
            create_relationship_tx(tx, ancestor_uuid, activity_dict['uuid'], 'ACTIVITY_INPUT', '->')
                
            # Create relationship from this Activity node to the target entity node
            create_relationship_tx(tx, activity_dict['uuid'], entity_uuid, 'ACTIVITY_OUTPUT', '->')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_entity_to_direct_ancestor(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit link_entity_to_direct_ancestor() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Get a list of associated collection uuids for a given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of dataset
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of collection uuids
"""
def get_dataset_collections(neo4j_driver, uuid, property_key = None):
    if property_key:
        parameterized_query = ("MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) " + 
                               "WHERE e.uuid = '{uuid}' " +
                               "RETURN apoc.coll.toSet(COLLECT(c.{property_key})) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) " + 
                               "WHERE e.uuid = '{uuid}' " +
                               "RETURN apoc.coll.toSet(COLLECT(c)) AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_dataset_collection_uuids() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        result = session.run(query)
        record = result.single()
        result_list = []

        # Convert the entity nodes to dicts
        nodes = record[record_field_name]

        for node in nodes:
            entity_dict = node_to_dict(node)
            result_list.append(entity_dict)

        return result_list

"""
Get a list of associated dataset dicts for a given collection

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of collection

Returns
-------
list
    The list comtaining associated dataset dicts
"""
def get_collection_datasets(neo4j_driver, uuid):
    parameterized_query = ("MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) " + 
                           "WHERE c.uuid = '{uuid}' " +
                           "RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    logger.debug("======get_collection_datasets() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        result = session.run(query)
        record = result.single()
        result_list = []

        # Convert the entity nodes to dicts
        nodes = record[record_field_name]

        for node in nodes:
            entity_dict = node_to_dict(node)
            result_list.append(entity_dict)

        return result_list


"""
Get count of published Dataset in the provenance hierarchy for a given  Collection/Sample/Donor

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
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
    parameterized_query = ("MATCH (e:{entity_class})-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(d:Dataset) " +
                           "WHERE e.uuid='{uuid}' AND d.status = 'Published' " +
                           # COLLECT() returns a list
                           # apoc.coll.toSet() reruns a set containing unique nodes
                           "RETURN COUNT(d) AS {record_field_name}")

    query = parameterized_query.format(entity_class = entity_class,
                                       uuid = uuid, 
                                       record_field_name = record_field_name)

    logger.debug("======count_attached_published_datasets() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        result = session.run(query)
        record = result.single()
        count = record[record_field_name]

        logger.debug("======count_attached_published_datasets() resulting count======")
        logger.debug(count)
        
        return count               


"""
Get the parent of a given Sample entity

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
    The parent dict, can either be a Sample or Donor
"""
def get_sample_direct_ancestor(neo4j_driver, uuid, property_key = None):
    if property_key:
        parameterized_query = ("MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) " +
                               # Filter out the Lab entity if it's the ancestor
                               "WHERE e.uuid='{uuid}' AND parent.entity_class <> 'Lab' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN parent.{property_key} AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           property_key = property_key,
                                           record_field_name = record_field_name)
    else:
        parameterized_query = ("MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) " +
                               # Filter out the Lab entity if it's the ancestor
                               "WHERE e.uuid='{uuid}' AND parent.entity_class <> 'Lab' " +
                               # COLLECT() returns a list
                               # apoc.coll.toSet() reruns a set containing unique nodes
                               "RETURN parent AS {record_field_name}")

        query = parameterized_query.format(uuid = uuid, 
                                           record_field_name = record_field_name)

    logger.debug("======get_sample_direct_ancestor() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        result = session.run(query)
        record = result.single()

        if property_key:
            return record[record_field_name]
        else:
            # Convert the entity node to dict
            node = record[record_field_name]
            entity_dict = node_to_dict(node)
            return entity_dict               


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
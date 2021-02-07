import json
from neo4j.exceptions import TransactionError
import logging

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'

####################################################################################################
## Directly called by schema_triggers.py
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
    query = (f"MATCH (s:Entity)-[in:ACTIVITY_INPUT]->(a:Activity)-[out:ACTIVITY_OUTPUT]->(t:Entity) "
             f"WHERE t.uuid = '{uuid}' "
             # Delete the Activity node and in input/out relationships
             f"DELETE in, a, out")

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
    results = []

    if property_key:
        query = (f"MATCH (s:Entity)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Dataset) " 
                 f"WHERE t.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(s.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (s:Entity)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Dataset) "
                 f"WHERE t.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(s)) AS {record_field_name}")

    logger.debug("======get_dataset_direct_ancestors() query======")
    logger.debug(query)

    # Sessions will often be created and destroyed using a with block context
    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if record:
        if property_key:
            # Just return the list of property values from each entity node
            results = record[record_field_name]
        else:
            # Convert the list of nodes to a list of dicts
            results = _nodes_to_dicts(record[record_field_name])

    return results

"""
Create a linkage (via Activity node) between the target entity node and the ancestor entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target entity
direct_ancestor_uuid : str
    The uuid of direct ancestor entity
activity_data_dict : str
    The activity properties of the activity node to be created
"""
def link_entity_to_direct_ancestor(neo4j_driver, entity_uuid, direct_ancestor_uuid, activity_data_dict):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # Create the Acvitity node
            _create_activity_tx(tx, activity_data_dict)

            # Create relationship from ancestor entity node to this Activity node
            _create_relationship_tx(tx, direct_ancestor_uuid, activity_data_dict['uuid'], 'ACTIVITY_INPUT', '->')
                
            # Create relationship from this Activity node to the target entity node
            _create_relationship_tx(tx, activity_data_dict['uuid'], entity_uuid, 'ACTIVITY_OUTPUT', '->')

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
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
                 f"WHERE e.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(c.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
                 f"WHERE e.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(c)) AS {record_field_name}")

    logger.debug("======get_dataset_collections() query======")
    logger.debug(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if record:
        if property_key:
            # Just return the list of property values from each entity node
            results = record[record_field_name]
        else:
            # Convert the list of nodes to a list of dicts
            results = _nodes_to_dicts(record[record_field_name])

    return results

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
    results = []

    query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
             f"WHERE c.uuid = '{uuid}' "
             f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    logger.debug("======get_collection_datasets() query======")
    logger.debug(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if record:
        # Convert the list of nodes to a list of dicts
        results = _nodes_to_dicts(record[record_field_name])

    return results

"""
Get count of published Dataset in the provenance hierarchy for a given Sample/Donor

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Sample, Donor
uuid : str
    The uuid of target entity 

Returns
-------
int
    The count of published Dataset in the provenance hierarchy 
    below the target entity (Donor, Sample and Collection)
"""
def count_attached_published_datasets(neo4j_driver, entity_type, uuid):
    query = (f"MATCH (e:{entity_type})-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(d:Dataset) "
             # Use the string function toLower() to avoid case-sensetivity issue
             f"WHERE e.uuid='{uuid}' AND toLower(d.status) = 'published' "
             # COLLECT() returns a list
             # apoc.coll.toSet() reruns a set containing unique nodes
             f"RETURN COUNT(d) AS {record_field_name}")

    logger.debug("======count_attached_published_datasets() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        count = record[record_field_name]

        logger.debug("======count_attached_published_datasets() resulting count======")
        logger.debug(count)
        
        return count               

"""
Update the dataset and its ancestors' data_access_level for a given dataset.
The dataset's ancestor can be another Dataset, Donor, or Sample. Won't be Collection.
In this case, we'll only need to update the dataset itself, and its Donor/Sample ancestors

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target dataset 
data_access_level : str
    The new data_access_level to be updated for the given dataset and its ancestors (Sample/Donor)
"""
def update_dataset_and_ancestors_data_access_level(neo4j_driver, uuid, data_access_level):
    query = (f"MATCH (e:Entity)-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(d:Dataset) "
             f"WHERE e.entity_type IN ['Donor', 'Sample'] AND d.uuid='{uuid}' "
             f"SET e.data_access_level = '{data_access_level}', d.data_access_level = '{data_access_level}' "
             # We don't really use the returned value
             f"RETURN COUNT(e) AS {record_field_name}")

    logger.debug("======update_dataset_and_ancestors_data_access_level() query======")
    logger.debug(query)
    
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            tx.run(query)
            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling update_dataset_and_ancestors_data_access_level(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.info("Failed to commit update_dataset_ancestors_data_access_level() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)

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
    result = {}

    if property_key:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # Filter out the Lab entity if it's the ancestor
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN parent.{property_key} AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # Filter out the Lab entity if it's the ancestor
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN parent AS {record_field_name}")

    logger.debug("======get_sample_direct_ancestor() query======")
    logger.debug(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if record:
        if property_key:
            result = record[record_field_name]
        else:
            # Convert the entity node to dict
            result = _node_to_dict(record[record_field_name])

    return result

    
####################################################################################################
## Internal Functions
####################################################################################################

"""
Execute a unit of work in a managed read transaction

Parameters
----------
tx : transaction_function
    a function that takes a transaction as an argument and does work with the transaction
query : str
    The target cypher query to run

Returns
-------
neo4j.Record or None
    A single record returned from the Cypher query
"""
def _execute_readonly_tx(tx, query):
    result = tx.run(query)
    record = result.single()
    return record

"""
Create a new activity node in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
activity_data_dict : dict
    The dict containing properties of the Activity node to be created

Returns
-------
neo4j.node
    A neo4j node instance of the newly created entity node
"""
def _create_activity_tx(tx, activity_data_dict):
    # `UNWIND` in Cypher expects List<T>
    activity_data_list = [activity_data_dict]

    # Convert the list (only contains one entity) to json list string
    activity_json_list_str = json.dumps(activity_data_list)

    query = (f"WITH apoc.convert.fromJsonList('{activity_json_list_str}') AS activities_list "
             f"UNWIND activities_list AS data "
             f"CREATE (a:Activity) "
             f"SET a = data "
             f"RETURN a AS {record_field_name}")

    logger.debug("======_create_activity_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    return node

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
def _create_relationship_tx(tx, source_node_uuid, target_node_uuid, relationship, direction):
    incoming = "-"
    outgoing = "-"
    
    if direction == "<-":
        incoming = direction

    if direction == "->":
        outgoing = direction

    query = (f"MATCH (s), (t) "
             f"WHERE s.uuid = '{source_node_uuid}' AND t.uuid = '{target_node_uuid}' "
             f"CREATE (s){incoming}[r:{relationship}]{outgoing}(t) "
             f"RETURN type(r) AS {record_field_name}") 

    logger.debug("======_create_relationship_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.debug("======_create_relationship_tx() resulting relationship======")
    logger.debug(f"(source node) {incoming} [:{relationship}] {outgoing} (target node)")

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
def _node_to_dict(entity_node):
    entity_dict = {}

    for key, value in entity_node._properties.items():
        entity_dict.setdefault(key, value)

    return entity_dict

"""
Convert the list of neo4j nodes into a list of Python dicts

Parameters
----------
nodes : list
    The list of neo4j node to be converted

Returns
-------
list
    A list of target entity dicts containing all property key/value pairs
"""
def _nodes_to_dicts(nodes):
    dicts = []

    for node in nodes:
        entity_dict = _node_to_dict(node)
        dicts.append(entity_dict)

    return dicts
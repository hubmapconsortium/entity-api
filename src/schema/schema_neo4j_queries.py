from neo4j.exceptions import CypherSyntaxError, TransactionError
import logging

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'

####################################################################################################
## Called by trigger methogs
####################################################################################################

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

    try:
        # Sessions will often be created and destroyed using a with block context
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
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_dataset_direct_ancestors(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e

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
relink : bool
    A flag to indicate if recreating the linkages
Returns
-------
boolean
    True if everything goes well
"""
def link_entity_to_direct_ancestor(neo4j_driver, entity_uuid, ancestor_uuid, activity_json_list_str, relink = False):
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

            return True
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling link_entity_to_direct_ancestor(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except TransactionError as te:
        msg = "TransactionError from calling link_entity_to_direct_ancestor(): " + te.value
        logger.error(msg)

        if tx.closed() == False:
            logger.error("Failed to commit link_entity_to_direct_ancestor() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)
    except Exception as e:
        raise e

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

    try:
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
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_dataset_collections(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e

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

    try:
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
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_collection_datasets(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e


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

    try:
        with neo4j_driver.session() as session:
            result = session.run(query)
            record = result.single()
            count = record[record_field_name]

            logger.debug("======count_attached_published_datasets() resulting count======")
            logger.debug(count)
            
            return count               
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling count_attached_published_datasets(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except Exception as e:
        raise e


"""
Delete relationships between the target Collection node and Dataset nodes in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
collection_uuid : str
    The uuid of collection

Returns
-------
integer
    The number of deleted relationships
"""
def unlink_collection_to_datasets_tx(tx, collection_uuid):
    parameterized_query = ("MATCH (e1:Collection)<-[r:IN_COLLECTION]-(e2:Dataset) " +
                           "WHERE e1.uuid = '{uuid}'" +
                           "DELETE r " +
                           "RETURN count(e2) AS {record_field_name}") 

    query = parameterized_query.format(uuid = collection_uuid,
                                       record_field_name = record_field_name)

    logger.debug("======unlink_collection_to_datasets_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    count = record[record_field_name]

    logger.debug("======unlink_collection_to_datasets_tx() resulting count======")
    logger.debug(count)

    return count


"""
Delete relationships between the target Dataset node and Collection nodes in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
dataset_uuid : str
    The uuid of dataset

Returns
-------
integer
    The number of deleted relationships
"""
def unlink_dataset_to_collections_tx(tx, dataset_uuid):
    parameterized_query = ("MATCH (e1:Dataset)-[r:IN_COLLECTION]->(e2:Collection) " +
                           "WHERE e1.uuid = '{uuid}' " +
                           "DELETE r " +
                           "RETURN count(e2) AS {record_field_name}") 

    query = parameterized_query.format(uuid = dataset_uuid,
                                       record_field_name = record_field_name)

    logger.debug("======unlink_dataset_to_collections_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.debug("======unlink_dataset_to_collections_tx() resulting relationship======")
    logger.debug(relationship)

    return relationship

"""
Create relationships between the target Collection node and Dataset nodes in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
collection_uuid : str
    The uuid of collection
dataset_uuids_list: list
    The list of dataset uuids to be linked

Returns
-------
str
    The relationship type name
"""
def link_collection_to_datasets_tx(tx, collection_uuid, dataset_uuids_list):
    dataset_uuids_list_str = '[' + ', '.join(dataset_uuids_list) + ']'
    parameterized_query = ("MATCH (e1:Collection), (e2:Dataset) " +
                           "WHERE e1.uuid = '{uuid}' AND e2.uuid IN {dataset_uuids_list_str} " +
                           "CREATE (e1)<-[r:IN_COLLECTION]-(e2) " +
                           "RETURN type(r) AS {record_field_name}") 

    query = parameterized_query.format(uuid = collection_uuid,
                                       dataset_uuids_list_str = dataset_uuids_list_str,
                                       record_field_name = record_field_name)

    logger.debug("======link_collection_to_datasets_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.debug("======link_collection_to_datasets_tx() resulting relationship======")
    logger.debug(relationship)

    return relationship

"""
Create relationships between the target Dataset node and Collection nodes in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
dataset_uuid : str
    The uuid of dataset
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
str
    The relationship type name
"""
def link_dataset_to_collections_tx(tx, dataset_uuid, collection_uuids_list):
    collection_uuids_list_str = '[' + ', '.join(collection_uuids_list) + ']'
    parameterized_query = ("MATCH (e1:Dataset), (e2:Collection) " +
                           "WHERE e1.uuid = '{uuid}' AND e2.uuid IN {collection_uuids_list_str} " +
                           "CREATE (e1)-[r:IN_COLLECTION]->(e2) " +
                           "RETURN type(r) AS {record_field_name}") 

    query = parameterized_query.format(uuid = dataset_uuid,
                                       collection_uuids_list_str = collection_uuids_list_str,
                                       record_field_name = record_field_name)

    logger.debug("======link_dataset_to_collections_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    relationship = record[record_field_name]

    logger.debug("======link_dataset_to_collections_tx() resulting relationship======")
    logger.debug(relationship)

    return relationship

"""
Create relationships between the target Collection node and containing Datasets nodes in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
collection_uuid : str
    The uuid of target collection
dataset_uuids_list: list
    The list of dataset uuids to be linked

Returns
-------
str
    The relationship type name
"""
def link_collection_to_datasets(neo4j_driver, collection_uuid, dataset_uuids_list):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Create relationships between the target Collection and associated Datasets")

            relationship = link_collection_to_datasets_tx(tx, collection_uuid, dataset_uuids_list)

            tx.commit()

            return relationship
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling link_collection_to_datasets(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except TransactionError as te:
        msg = "TransactionError from calling link_collection_to_datasets(): " + te.value
        logger.error(msg)

        if tx.closed() == False:
            logger.info("Failed to commit link_collection_to_datasets() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)
    except Exception as e:
        raise e

"""
Recreate relationships between the target Collection node and containing Datasets nodes in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
collection_uuid : str
    The uuid of target collection
dataset_uuids_list: list
    The list of dataset uuids to be linked

Returns
-------
str
    The relationship type name
"""
def relink_collection_to_datasets(neo4j_driver, collection_uuid, dataset_uuids_list):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Delete relationships between the target Collection and associated Datasets")

            count_deleted = unlink_collection_to_datasets_tx(tx, collection_uuid)

            logger.info("Create relationships between the target Collection and associated Datasets")

            relationship = link_collection_to_datasets_tx(tx, collection_uuid, dataset_uuids_list)

            tx.commit()

            return relationship
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling relink_collection_to_datasets(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except TransactionError as te:
        msg = "TransactionError from calling relink_collection_to_datasets(): " + te.value
        logger.error(msg)

        if tx.closed() == False:
            logger.info("Failed to commit relink_collection_to_datasets() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)
    except Exception as e:
        raise e


"""
Create relationships between the target dataset node and collection nodes in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
dataset_uuid : str
    The uuid of target dataset
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
str
    The relationship type name
"""
def link_dataset_to_collections(neo4j_driver, dataset_uuid, collection_uuids_list):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()
   
            logger.info("Create relationships between the target Dataset and associated Collections")

            relationship = create_relationships_tx(tx, dataset_uuid, collection_uuids_list)

            tx.commit()

            return relationship
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling link_dataset_to_collections(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except TransactionError as te:
        msg = "TransactionError from calling link_dataset_to_collections(): " + te.value
        logger.error(msg)

        if tx.closed() == False:
            logger.info("Failed to commit link_dataset_to_collections() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)
    except Exception as e:
        raise e

"""
Recreate relationships between the target dataset node and collection nodes in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
dataset_uuid : str
    The uuid of target dataset
collection_uuids_list: list
    The list of collection uuids to be linked

Returns
-------
str
    The relationship type name
"""
def relink_dataset_to_collections(neo4j_driver, dataset_uuid, collection_uuids_list):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()
            
            logger.info("Delete relationships between the target Dataset and associated Collections")

            count_deleted = unlink_collection_to_datasets_tx(tx, dataset_uuid)

            logger.info("Create relationships between the target Dataset and associated Collections")

            relationship = create_relationships_tx(tx, dataset_uuid, collection_uuids_list)

            tx.commit()

            return relationship
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling relink_dataset_to_collections(): " + ce.message
        logger.error(msg)
        raise CypherSyntaxError(msg)
    except TransactionError as te:
        msg = "TransactionError from calling relink_dataset_to_collections(): " + te.value
        logger.error(msg)

        if tx.closed() == False:
            logger.info("Failed to commit relink_dataset_to_collections() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)
    except Exception as e:
        raise e

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

    try:
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
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_sample_direct_ancestor(): " + ce.message
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
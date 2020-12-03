from neo4j.exceptions import CypherSyntaxError, TransactionError
import logging

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'

####################################################################################################
## Called by trigger methogs
####################################################################################################

"""
Get the source entities uuids of a given dataset by uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 

Returns
-------
list
    A unique list of uuids of source entities
"""
def get_dataset_source_uuids(neo4j_driver, uuid):
    parameterized_query = ("MATCH (s:Dataset)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Entity) " + 
                           "WHERE t.uuid = '{uuid}' " +
                           "RETURN apoc.coll.toSet(COLLECT(s.uuid)) AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    logger.debug("======get_dataset_source_uuids() query======")
    logger.debug(query)

    try:
        # Sessions will often be created and destroyed using a with block context
        with neo4j_driver.session() as session:
            result = session.run(query)

            record = result.single()
            source_uuids = record[record_field_name]

            logger.debug("======get_dataset_source_uuids() resulting source_uuids list======")
            logger.debug(source_uuids)

            return source_uuids
    except CypherSyntaxError as ce:
        msg = "CypherSyntaxError from calling get_dataset_source_uuids(): " + ce.message
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


def link_collection_to_datasets(neo4j_driver, collection_uuid, dataset_uuids_list):
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


def link_dataset_to_collections(neo4j_driver, dataset_uuid, collection_uuids_list):
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
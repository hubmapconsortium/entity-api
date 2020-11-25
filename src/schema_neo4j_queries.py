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
neo4j_db : neo4j.Session object
    The neo4j database session
uuid : str
    The uuid of target entity 

Returns
-------
list
    A unique list of uuids of source entities
"""
def get_dataset_source_uuids(neo4j_db, uuid):
    parameterized_query = ("MATCH (s:Dataset)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Entity) " + 
                           "WHERE t.uuid = '{uuid}' " +
                           "RETURN apoc.coll.toSet(COLLECT(s.uuid)) AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    logger.debug("======get_dataset_source_uuids() query======")
    logger.debug(query)

    try:
        result = neo4j_db.run(query)

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
neo4j_db : neo4j.Session object
    The neo4j database session
uuid : str
    The uuid of collection

Returns
-------
list
    The list comtaining associated dataset dicts
"""
def get_collection_datasets(neo4j_db, uuid):
    parameterized_query = ("MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) " + 
                           "WHERE c.uuid = '{uuid}' " +
                           "RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    query = parameterized_query.format(uuid = uuid, 
                                       record_field_name = record_field_name)
    
    logger.debug("======get_collection_datasets() query======")
    logger.debug(query)

    try:
        result = neo4j_db.run(query)
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
neo4j_db : neo4j.Session object
    The neo4j database session
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
def count_attached_published_datasets(neo4j_db, entity_class, uuid):
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
        result = neo4j_db.run(query)
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


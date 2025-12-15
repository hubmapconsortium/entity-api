import neo4j
from neo4j.exceptions import TransactionError
from neo4j import Session as Neo4jSession
from schema.schema_constants import SchemaConstants, Neo4jRelationshipEnum
import logging

logger = logging.getLogger(__name__)

# The filed name of the single result record
record_field_name = 'result'

####################################################################################################
## Functions can be called by app.py, schema_manager.py, and schema_triggers.py
####################################################################################################

"""
Create a new entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
entity_data_dict : dict
    The target Entity node to be created
superclass : str
    The normalized entity superclass type if defined, None by default

Returns
-------
dict
    A dictionary of newly created entity details returned from the Cypher query
"""
def create_entity(neo4j_driver, entity_type, entity_data_dict, superclass = None):
    # Always define the Entity label in addition to the target `entity_type` label
    labels = f':Entity:{entity_type}'

    if superclass is not None:
        labels = f':Entity:{entity_type}:{superclass}'

    node_properties_map = build_properties_map(entity_data_dict)
    
    query = (f"CREATE (e{labels}) "
             f"SET e = {node_properties_map} "
             f"RETURN e AS {record_field_name}")

    logger.info("======create_entity() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            result = tx.run(query)
            record = result.single()
            entity_node = record[record_field_name]

            entity_dict = node_to_dict(entity_node)

            tx.commit()

            return entity_dict
    except TransactionError as te:
        msg = f"TransactionError from calling create_entity(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.error("Failed to commit create_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


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
    result = {}

    query = (f"MATCH (e:Entity) "
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN e AS {record_field_name}")

    logger.info("======get_entity() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = node_to_dict(record[record_field_name])

    return result

"""
Given a list of UUIDs, return a dict mapping uuid -> entity_node
Only UUIDs present in Neo4j will be returned.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid_list : list of str
    The uuids of target entities to retrieve from Neo4j 

Returns
-------
dict
    A dictionary of entity details returned from the Cypher query, keyed by
    the uuid provided in uuid_list.
"""
def identify_existing_dataset_entities(neo4j_driver, dataset_uuid_list:list):

    if not dataset_uuid_list:
        return {}

    query = """
        MATCH (e:Entity)
        WHERE e.uuid IN $param_uuids
          AND e.entity_type='Dataset'
        RETURN  e.uuid AS uuid
    """

    with neo4j_driver.session() as session:
        results = session.run(query, param_uuids=dataset_uuid_list)
        return [record["uuid"] for record in results]

"""
Get the uuids for each entity in a list that doesn't belong to a certain entity type. Uuids are ordered by type

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
direct_ancestor_uuids : list
    List of the uuids to be filtered
entity_type : string
    The entity to be excluded

Returns
-------
dict
    A dictionary of entity uuids that don't pass the filter, grouped by entity_type
"""
def filter_ancestors_by_type(neo4j_driver, direct_ancestor_uuids, entity_type):
    query = (f"MATCH (e:Entity) "
             f"WHERE e.uuid in {direct_ancestor_uuids} AND toLower(e.entity_type) <> '{entity_type.lower()}' "
             f"RETURN e.entity_type AS entity_type, collect(e.uuid) AS uuids")
    logger.info("======filter_ancestors_by_type======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        records = session.run(query).data()
          
    return records if records else None


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
    results = []
    fields_to_omit = SchemaConstants.OMITTED_FIELDS
    if property_key:
        query = (f"MATCH (e:Entity)-[:ACTIVITY_INPUT]->(:Activity)-[:ACTIVITY_OUTPUT]->(child:Entity) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid='{uuid}' AND e.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(child.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:ACTIVITY_INPUT]->(:Activity)-[:ACTIVITY_OUTPUT]->(child:Entity) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid='{uuid}' AND e.entity_type <> 'Lab' "
                 f"WITH COLLECT(DISTINCT child) AS uniqueChildren "
                 f"RETURN [a IN uniqueChildren | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {fields_to_omit}))] AS {record_field_name}")

    logger.info("======get_children() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])
                
    return results


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
    results = []
    fields_to_omit = SchemaConstants.OMITTED_FIELDS
    if property_key:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(parent.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"WITH COLLECT(DISTINCT parent) AS uniqueParents "
                 f"RETURN [a IN uniqueParents | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {fields_to_omit}))] AS {record_field_name}")

    logger.info("======get_parents() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


"""
Get all siblings by uuid

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
    A list of unique sibling dictionaries returned from the Cypher query
"""
def get_siblings(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"MATCH (sibling:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent) "
                 f"WHERE sibling <> e "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() returns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(sibling.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"MATCH (sibling:Entity)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent) "
                 f"WHERE sibling <> e "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() returns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(sibling)) AS {record_field_name}")


    logger.info("======get_siblings() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])
    return results


"""
Get all tuplets by uuid

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
    A list of unique tuplet dictionaries returned from the Cypher query
"""
def get_tuplets(neo4j_driver, uuid, property_key=None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(a:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"MATCH (tuplet:Entity)<-[:ACTIVITY_OUTPUT]-(a) "
                 f"WHERE tuplet <> e "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() returns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(tuplet.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_OUTPUT]-(a:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"MATCH (tuplet:Entity)<-[:ACTIVITY_OUTPUT]-(a:Activity) "
                 f"WHERE tuplet <> e "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() returns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(tuplet)) AS {record_field_name}")


    logger.info("======get_tuplets() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])
    return results


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
    results = []
    fields_to_omit = SchemaConstants.OMITTED_FIELDS
    if property_key:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_INPUT|ACTIVITY_OUTPUT*]-(ancestor:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND ancestor.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(ancestor.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:ACTIVITY_INPUT|ACTIVITY_OUTPUT*]-(ancestor:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND ancestor.entity_type <> 'Lab' "
                 f"WITH COLLECT(DISTINCT ancestor) AS uniqueAncestors "
                 f"RETURN [a IN uniqueAncestors | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {fields_to_omit}))] AS {record_field_name}")

    logger.info("======get_ancestors() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


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
    results = []
    fields_to_omit = SchemaConstants.OMITTED_FIELDS
    if property_key:
        query = (f"MATCH (e:Entity)-[:ACTIVITY_INPUT|ACTIVITY_OUTPUT*]->(descendant:Entity) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid='{uuid}' AND e.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(descendant.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:ACTIVITY_INPUT|ACTIVITY_OUTPUT*]->(descendant:Entity) "
                 # The target entity can't be a Lab
                 f"WHERE e.uuid='{uuid}' AND e.entity_type <> 'Lab' "
                 f"WITH COLLECT(DISTINCT descendant) AS uniqueDescendants "
                 f"RETURN [a IN uniqueDescendants | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {fields_to_omit}))] AS {record_field_name}")                 

    logger.info("======get_descendants() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])
                
    return results


"""
Get all collections by uuid

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
    A list of unique collection dictionaries returned from the Cypher query
"""
def get_collections(neo4j_driver, uuid, property_key = None):
    results = []

    if property_key:
        query = (f"MATCH (c:Collection)<-[:IN_COLLECTION]-(ds:Dataset) "
                 f"WHERE ds.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(c.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (c:Collection)<-[:IN_COLLECTION]-(ds:Dataset) "
                 f"WHERE ds.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(c)) AS {record_field_name}")

    logger.info("======get_collections() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


"""
Get all uploads by uuid

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
    A list of unique upload dictionaries returned from the Cypher query
"""
def get_uploads(neo4j_driver, uuid, property_key = None):
    results = []
    if property_key:
        query = (f"MATCH (u:Upload)<-[:IN_UPLOAD]-(ds:Dataset) "
                 f"WHERE ds.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(u.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (u:Upload)<-[:IN_UPLOAD]-(ds:Dataset) "
                 f"WHERE ds.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(u)) AS {record_field_name}")

    logger.info("======get_uploads() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)
        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


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
properties_to_exclude : list
    A list of node properties to exclude from result

Returns
-------
list
    A unique list of uuids of source entities
"""
def get_dataset_direct_ancestors(neo4j_driver, uuid, property_key = None, properties_to_exclude = []):
    results = []

    if property_key:
        query = (f"MATCH (s:Entity)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Dataset) " 
                 f"WHERE t.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(s.{property_key})) AS {record_field_name}")
    else:
        if properties_to_exclude:
            query = (f"MATCH (s:Entity)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Dataset) "
                     f"WHERE t.uuid = '{uuid}' "
                     f"WITH apoc.coll.toSet(COLLECT(s)) AS uniqueDirectAncestors "
                     f"RETURN [a IN uniqueDirectAncestors | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {properties_to_exclude}))] AS {record_field_name}")
        else:
            query = (f"MATCH (s:Entity)-[:ACTIVITY_INPUT]->(a:Activity)-[:ACTIVITY_OUTPUT]->(t:Dataset) "
                     f"WHERE t.uuid = '{uuid}' "
                     f"RETURN apoc.coll.toSet(COLLECT(s)) AS {record_field_name}")

    logger.info("======get_dataset_direct_ancestors() query======")
    logger.debug(query)

    # Sessions will often be created and destroyed using a with block context
    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


"""
For every Sample organ associated with the given dataset_uuid, retrieve the
organ information and organ Donor information for use in composing a title for the Dataset.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
dataset_uuid : str
    The UUID of a Dataset

Returns
-------
list : List containing the source metadata (string representation of a Python dict) of each Donor of an
       organ Sample associated with the Dataset. Could also be an empty list [] if no match.
"""
def get_dataset_donor_organs_info(neo4j_driver, dataset_uuid):

    with neo4j_driver.session() as session:
        ds_donors_organs_query = (  f"MATCH (e:Dataset)<-[:ACTIVITY_INPUT|ACTIVITY_OUTPUT*]-(org:Sample)<-[:ACTIVITY_INPUT|ACTIVITY_OUTPUT*]-(d:Donor)"
                                    f" WHERE e.uuid='{dataset_uuid}'"
                                    f"   AND org.sample_category IS NOT NULL"
                                    f"   AND org.sample_category='organ'"
                                    f"   AND org.organ IS NOT NULL"
                                    f" RETURN apoc.coll.toSet(COLLECT({{donor_uuid: d.uuid"
                                    f"                                  , donor_metadata: d.metadata"
                                    f"                                  , organ_type: org.organ}})) AS donorOrganSet")

        logger.info("======get_dataset_donor_organs_info() ds_donors_organs_query======")
        logger.debug(ds_donors_organs_query)

        with neo4j_driver.session() as session:
            record = session.read_transaction(execute_readonly_tx
                                              , ds_donors_organs_query)

    return record['donorOrganSet'] if record and record['donorOrganSet'] else []


"""
Get entity type for a given uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target entity

Returns
-------
str
    The entity_type string
"""
def get_entity_type(neo4j_driver, entity_uuid: str) -> str:
    query: str = f"Match (ent {{uuid: '{entity_uuid}'}}) return ent.entity_type"

    logger.info("======get_entity_type() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)
        if record and len(record) == 1:
            return record[0]

    return None


"""
Get Activity.creation_action for a given collection

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of given entity

Returns
-------
str
    The creation action string
"""
def get_entity_creation_action_activity(neo4j_driver, entity_uuid: str) -> str:
    query: str = f"MATCH (ds:Dataset {{uuid:'{entity_uuid}'}})<-[:ACTIVITY_OUTPUT]-(a:Activity) RETURN a.creation_action"

    logger.info("======get_entity_creation_action() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)
        if record and len(record) == 1:
            return record[0]

    return None


"""
Create or recreate one or more linkages (via Activity nodes) 
between the target entity node and the direct ancestor nodes in neo4j

Note: the size of direct_ancestor_uuids equals to that of activity_data_dict_list

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target child entity
direct_ancestor_uuids : list
    A list of uuids of direct ancestors
activity_data_dict : dict
    A dict of activity properties to be created
"""
def link_entity_to_direct_ancestors(neo4j_driver, entity_uuid, direct_ancestor_uuids, activity_data_dict):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages and Activity node between this entity and its direct ancestors
            _delete_activity_node_and_linkages_tx(tx, entity_uuid)

            # Get the activity uuid
            activity_uuid = activity_data_dict['uuid']

            # Create the Acvitity node
            create_activity_tx(tx, activity_data_dict)

            # Create relationship from this Activity node to the target entity node
            create_relationship_tx(tx, activity_uuid, entity_uuid, 'ACTIVITY_OUTPUT', '->')

            # Create relationship from each ancestor entity node to this Activity node
            create_outgoing_activity_relationships_tx(tx=tx
                                                      , source_node_uuids=direct_ancestor_uuids
                                                      , activity_node_uuid=activity_uuid)
                    
            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_entity_to_direct_ancestors(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.error("Failed to commit link_entity_to_direct_ancestors() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)
    

"""
Create linkages from new direct ancestors to an EXISTING activity node in neo4j.


Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target child entity
new_ancestor_uuid : str
    The uuid of new direct ancestor to be linked
activity_uuid : str
    The uuid of the existing activity node to link to
"""
def add_new_ancestors_to_existing_activity(neo4j_driver, new_ancestor_uuids, activity_uuid, create_activity, activity_data_dict, dataset_uuid):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()
            if create_activity:
                create_activity_tx(tx, activity_data_dict)
                create_relationship_tx(tx, activity_uuid, dataset_uuid, 'ACTIVITY_OUTPUT', '->')
            create_outgoing_activity_relationships_tx(tx=tx
                                                      , source_node_uuids=new_ancestor_uuids
                                                      , activity_node_uuid=activity_uuid)
                    
            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling add_new_ancestors_to_existing_activity(): "
        logger.exception(msg)

        if tx.closed() == False:
            logger.error("Failed to commit add_new_ancestors_to_existing_activity() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)

"""
Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of the target entity nodeget_paren

Returns
-------
str
    The uuid of the direct ancestor Activity node
"""
def get_parent_activity_uuid_from_entity(neo4j_driver, entity_uuid):
    query = """
        MATCH (activity:Activity)-[:ACTIVITY_OUTPUT]->(entity:Entity {uuid: $entity_uuid})
        RETURN activity.uuid AS activity_uuid
    """
    
    with neo4j_driver.session() as session:
        result = session.run(query, entity_uuid=entity_uuid)
        
        record = result.single()
        if record:
            return record["activity_uuid"]
        else:
            return None


"""
Create or recreate linkage 
between the publication node and the associated collection node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of the publication
associated_collection_uuid : str
    the uuid of the associated collection
"""
def link_publication_to_associated_collection(neo4j_driver, entity_uuid, associated_collection_uuid):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete any old linkage between this publication and any associated_collection
            _delete_publication_associated_collection_linkages_tx(tx, entity_uuid)

            # Create relationship from this publication node to the associated collection node
            create_relationship_tx(tx, entity_uuid, associated_collection_uuid, 'USES_DATA', '->')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_publication_to_associated_collection(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.error("Failed to commit link_publication_to_associated_collection() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)

"""
Link a Collection to all the Datasets it should contain per the provided
argument.  First, all existing linkages are deleted, then a link between
each entry of the dataset_uuid_list and collection_uuid is created in the
correction direction with an IN_COLLECTION relationship.

No Activity nodes are created in the relationship between a Collection and
its Datasets.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
collection_uuid : str
    The uuid of a Collection entity which is the target of an IN_COLLECTION relationship.
dataset_uuid_list : list of str
    A list of uuids of Dataset entities which are the source of an IN_COLLECTION relationship.
"""
def link_collection_to_datasets(neo4j_driver, collection_uuid, dataset_uuid_list):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            # First delete all the old linkages between this Collection and its member Datasets
            _delete_collection_linkages_tx(tx=tx
                                           , uuid=collection_uuid)

            _create_relationships_unwind_tx(tx=tx
                                            , source_uuid_list=dataset_uuid_list
                                            , target_uuid=collection_uuid
                                            , relationship=Neo4jRelationshipEnum.IN_COLLECTION
                                            , direction='->')

            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_collection_to_datasets(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.error("Failed to commit link_collection_to_datasets() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)

"""
Create a revision linkage from the target entity node to the entity node 
of the previous revision in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of target entity
previous_revision_entity_uuid : str
    The uuid of previous revision entity
"""
def link_entity_to_previous_revision(neo4j_driver, entity_uuid, previous_revision_entity_uuids):
    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()
            for previous_uuid in previous_revision_entity_uuids:
                # Create relationship from ancestor entity node to this Activity node
                create_relationship_tx(tx, entity_uuid, previous_uuid, 'REVISION_OF', '->')
            tx.commit()
    except TransactionError as te:
        msg = "TransactionError from calling link_entity_to_previous_revision(): "
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            # Log the full stack trace, prepend a line with our message
            logger.error("Failed to commit link_entity_to_previous_revision() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)


"""
Get the uuid of previous revision entity for a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of previous revision entity 

Returns
-------
dict
    The parent dict, can either be a Sample or Donor
"""
def get_previous_revision_uuid(neo4j_driver, uuid):
    result = None

    # Don't use [r:REVISION_OF] because 
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH (e:Entity)-[:REVISION_OF]->(previous_revision:Entity) "
             f"WHERE e.uuid='{uuid}' "
             f"RETURN previous_revision.uuid AS {record_field_name}")

    logger.info("======get_previous_revision_uuid() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


"""
Get the uuids of previous revision entities for a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of the entity 

Returns
-------
list
    The previous revision uuids
"""
def get_previous_revision_uuids(neo4j_driver, uuid):
    result = []

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH (e:Entity)-[:REVISION_OF]->(previous_revision:Entity) "
             f"WHERE e.uuid='{uuid}' "
             f"RETURN COLLECT(previous_revision.uuid) AS {record_field_name}")

    logger.info("======get_previous_revision_uuids() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


"""
Get the uuid of next revision entity for a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of previous revision entity 

Returns
-------
dict
    The parent dict, can either be a Sample or Donor
"""
def get_next_revision_uuid(neo4j_driver, uuid):
    result = None

    # Don't use [r:REVISION_OF] because 
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH (e:Entity)<-[:REVISION_OF]-(next_revision:Entity) "
             f"WHERE e.uuid='{uuid}' "
             f"RETURN next_revision.uuid AS {record_field_name}")

    logger.info("======get_next_revision_uuid() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


"""
Get the uuids of next revision entities for a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of the entity 

Returns
-------
list
    The uuids of the next revision
"""
def get_next_revision_uuids(neo4j_driver, uuid):
    result = []

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH (e:Entity)<-[:REVISION_OF]-(next_revision:Entity) "
             f"WHERE e.uuid='{uuid}' "
             f"RETURN COLLECT(next_revision.uuid) AS {record_field_name}")

    logger.info("======get_next_revision_uuids() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            result = record[record_field_name]

    return result


"""
Get a list of associated Datasets and Publications (subclass of Dataset) uuids for a given collection

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of collection
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of datasets and publications
"""
def get_collection_associated_datasets(neo4j_driver, uuid, property_key = None):
    results = []
    if property_key:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION|:USES_DATA]->(c:Collection) "
                 f"WHERE c.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION|:USES_DATA]->(c:Collection) "
                 f"WHERE c.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    logger.info("======get_collection_associated_datasets() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


"""
Get a list of associated collection uuids for a given dataset or publication (subclass)

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of dataset or publication
property_key : str
    A target property key for result filtering
properties_to_exclude : list
    A list of node properties to exclude from result

Returns
-------
list
    A list of collection uuids
"""
def get_dataset_collections(neo4j_driver, uuid, property_key = None, properties_to_exclude = []):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
                 f"WHERE e.uuid = '{uuid}' "
                 f"RETURN apoc.coll.toSet(COLLECT(c.{property_key})) AS {record_field_name}")
    else:
        if properties_to_exclude:
            query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
                     f"WHERE e.uuid = '{uuid}' "
                     f"WITH apoc.coll.toSet(COLLECT(c)) AS uniqueCollections "
                     f"RETURN [c IN uniqueCollections | apoc.create.vNode(labels(c), apoc.map.removeKeys(properties(c), {properties_to_exclude}))] AS {record_field_name}")
        else:
            query = (f"MATCH (e:Entity)-[:IN_COLLECTION]->(c:Collection) "
                     f"WHERE e.uuid = '{uuid}' "
                     f"RETURN apoc.coll.toSet(COLLECT(c)) AS {record_field_name}")

    logger.info("======get_dataset_collections() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results

"""
Get the associated collection for a given publication

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of publication
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A dictionary representation of the collection
"""
def get_publication_associated_collection(neo4j_driver, uuid):
    result = {}
    query = (f"MATCH (p:Publication)-[:USES_DATA]->(c:Collection) "
             f"WHERE p.uuid = '{uuid}' "
             f"RETURN c as {record_field_name}")

    logger.info("=====get_publication_associated_collection() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = node_to_dict(record[record_field_name])

    return result


"""
Get the associated collection for a given publication

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of publication
property_key : str
    A target property key for result filtering

Returns
-------
dict
    A dictionary representation of the chosen values
"""
def get_collection_associated_publication(neo4j_driver, uuid):
    result = {}
    query = (f"MATCH (p:Publication)-[:USES_DATA]->(c:Collection) "
             f"WHERE c.uuid = '{uuid}' "
             f"RETURN {{uuid: p.uuid, hubmap_id: p.hubmap_id, title: p.title}} AS publication")

    logger.info("=====get_collection_associated_publication() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.run(query).single()
        if record:
            result = record["publication"]
    return result



"""
Get the associated Upload for a given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of dataset
properties_to_exclude : list
    A list of node properties to exclude from result

Returns
-------
dict
    A Upload dict
"""
def get_dataset_upload(neo4j_driver, uuid, properties_to_exclude = []):
    result = {}

    if properties_to_exclude:
        query = (f"MATCH (e:Entity)-[:IN_UPLOAD]->(s:Upload) "
                 f"WHERE e.uuid = '{uuid}' "
                 f"WITH s AS up "
                 f"RETURN apoc.create.vNode(labels(up), apoc.map.removeKeys(properties(up), {properties_to_exclude})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:IN_UPLOAD]->(s:Upload) "
                 f"WHERE e.uuid = '{uuid}' "
                 f"RETURN s AS {record_field_name}")

    logger.info("======get_dataset_upload() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the node to a dict
            result = node_to_dict(record[record_field_name])

    return result


"""
Get a list of associated dataset dicts for a given collection

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of collection
properties_to_exclude : list
    A list of node properties to exclude from result

Returns
-------
list
    The list containing associated dataset dicts
"""
def get_collection_datasets(neo4j_driver, uuid, properties_to_exclude = []):
    results = []

    fields_to_omit = SchemaConstants.OMITTED_FIELDS


    if properties_to_exclude:
        merged_list = properties_to_exclude + fields_to_omit
        
        query = (f"MATCH (e:Dataset)-[:IN_COLLECTION]->(c:Collection) "
                 f"WHERE c.uuid = '{uuid}' "
                 f"WITH COLLECT(DISTINCT e) AS uniqueDataset "
                 f"RETURN [a IN uniqueDataset | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {merged_list}))] AS {record_field_name}")
    else:
        query = (f"MATCH (e:Dataset)-[:IN_COLLECTION]->(c:Collection) "
                 f"WHERE c.uuid = '{uuid}' "
                 f"WITH COLLECT(DISTINCT e) AS uniqueDataset "
                 f"RETURN [a IN uniqueDataset | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {fields_to_omit}))] AS {record_field_name}")

    logger.info("======get_collection_datasets() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the list of nodes to a list of dicts
            results = nodes_to_dicts(record[record_field_name])

    return results


"""
Get a dictionary with an entry for each Dataset in a Collection. The dictionary is
keyed by Dataset uuid and contains the Dataset data_access_level.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of a Collection

Returns
-------
dict
     A dictionary with an entry for each Dataset in a Collection. The dictionary is
     keyed by Dataset uuid and contains the Dataset data_access_level.
"""
def get_collection_datasets_data_access_levels(neo4j_driver, uuid):
    results = []

    query = (f"MATCH (d:Dataset)-[:IN_COLLECTION]->(c:Collection) "
             f"WHERE c.uuid = '{uuid}' "
             f"RETURN COLLECT(DISTINCT d.data_access_level) AS {record_field_name}")

    logger.info("======get_collection_datasets_data_access_levels() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Just return the list of values
            results = record[record_field_name]

    return results

"""
Get a dictionary with an entry for each Dataset in a Collection. The dictionary is
keyed by Dataset uuid and contains the Dataset data_access_level.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of a Collection

Returns
-------
dict
     A dictionary with an entry for each Dataset in a Collection. The dictionary is
     keyed by Dataset uuid and contains the Dataset data_access_level.
"""
def get_collection_datasets_statuses(neo4j_driver, uuid):
    results = []

    query = (f"MATCH (d: Dataset)-[:IN_COLLECTION]->(c:Collection) "
             f"WHERE c.uuid = '{uuid}' "
             f"RETURN COLLECT(DISTINCT d.status) AS {record_field_name}")

    logger.info("======get_collection_datasets_statuses() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Just return the list of values
            results = record[record_field_name]
        else:
            results = []

    return results

"""
Link the dataset nodes to the target Upload node

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
upload_uuid : str
    The uuid of target Upload 
dataset_uuids_list : list
    A list of dataset uuids to be linked to Upload
"""
def link_datasets_to_upload(neo4j_driver, upload_uuid, dataset_uuids_list):
    # Join the list of uuids and wrap each string in single quote
    joined_str = ', '.join("'{0}'".format(dataset_uuid) for dataset_uuid in dataset_uuids_list)
    # Format a string to be used in Cypher query.
    # E.g., ['fb6757b606ac35be7fa85062fde9c2e1', 'ku0gd44535be7fa85062fde98gt5']
    dataset_uuids_list_str = '[' + joined_str + ']'

    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Create relationships between the target Upload and the given Datasets")

            query = (f"MATCH (s:Upload), (d:Dataset) "
                     f"WHERE s.uuid = '{upload_uuid}' AND d.uuid IN {dataset_uuids_list_str} "
                     # Use MERGE instead of CREATE to avoid creating the existing relationship multiple times
                     # MERGE creates the relationship only if there is no existing relationship
                     f"MERGE (s)<-[r:IN_UPLOAD]-(d)") 

            logger.info("======link_datasets_to_upload() query======")
            logger.debug(query)

            tx.run(query)
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling link_datasets_to_upload(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.error("Failed to commit link_datasets_to_upload() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Unlink the dataset nodes from the target Upload node

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
upload_uuid : str
    The uuid of target Upload 
dataset_uuids_list : list
    A list of dataset uuids to be unlinked from Upload
"""
def unlink_datasets_from_upload(neo4j_driver, upload_uuid, dataset_uuids_list):
    # Join the list of uuids and wrap each string in single quote
    joined_str = ', '.join("'{0}'".format(dataset_uuid) for dataset_uuid in dataset_uuids_list)
    # Format a string to be used in Cypher query.
    # E.g., ['fb6757b606ac35be7fa85062fde9c2e1', 'ku0gd44535be7fa85062fde98gt5']
    dataset_uuids_list_str = '[' + joined_str + ']'

    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            logger.info("Delete relationships between the target Upload and the given Datasets")

            query = (f"MATCH (s:Upload)<-[r:IN_UPLOAD]-(d:Dataset) "
                     f"WHERE s.uuid = '{upload_uuid}' AND d.uuid IN {dataset_uuids_list_str} "
                     f"DELETE r") 

            logger.info("======unlink_datasets_from_upload() query======")
            logger.debug(query)

            tx.run(query)
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling unlink_datasets_from_upload(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.error("Failed to commit unlink_datasets_from_upload() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Get a list of associated dataset dicts for a given Upload

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of Upload
property_key : str
    A target property key for result filtering
properties_to_exclude : list
    A list of node properties to exclude from result

Returns
-------
list
    The list containing associated dataset dicts
"""
def get_upload_datasets(neo4j_driver, uuid, property_key = None, properties_to_exclude = []):
    results = []
    fields_to_omit = SchemaConstants.OMITTED_FIELDS
    if property_key:
        query = (f"MATCH (e:Dataset)-[:IN_UPLOAD]->(s:Upload) "
                 f"WHERE s.uuid = '{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")
    else:
        if properties_to_exclude:
            merged_list = properties_to_exclude + fields_to_omit

            query = (f"MATCH (e:Dataset)-[:IN_UPLOAD]->(s:Upload) "
                     f"WHERE s.uuid = '{uuid}' "
                     f"WITH COLLECT(DISTINCT e) AS uniqueUploads "
                     f"RETURN [a IN uniqueUploads | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {merged_list}))] AS {record_field_name}")
        else:
            query = (f"MATCH (e:Dataset)-[:IN_UPLOAD]->(s:Upload) "
                     f"WHERE s.uuid = '{uuid}' "
                     f"WITH COLLECT(DISTINCT e) AS uniqueUploads "
                     f"RETURN [a IN uniqueUploads | apoc.create.vNode(labels(a), apoc.map.removeKeys(properties(a), {fields_to_omit}))] AS {record_field_name}")

    logger.info("======get_upload_datasets() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = nodes_to_dicts(record[record_field_name])

    return results


"""
Get the qualified uuids-found and Dataset-given a list of uuids for validation purposes

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuids : list
    The list of uuids from user input

Returns
-------
list
    A list of uuids that are found and Dataset type
    
"""
def get_found_dataset_uuids(neo4j_driver, uuids):
    query = (
        f"MATCH (e:Dataset) "
        f"WHERE e.uuid IN {uuids} "
        f"RETURN COLLECT(e.uuid) AS {record_field_name}")

    logger.info("======get_not_found_or_not_dataset_uuids() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        uuids_list = record[record_field_name]

        return uuids_list               


"""
Get the component dataset uuids for a given parent dataset uuid

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target parent dataset

Returns
-------
list
    A list of component dataset uuids
"""
def get_component_dataset_uuids(neo4j_driver, uuid):
    query = (
        f"MATCH (c:Dataset)<-[:ACTIVITY_OUTPUT]-(a:Activity)<-[:ACTIVITY_INPUT]-(p:Dataset) "
        f"WHERE p.uuid='{uuid}' AND a.creation_action='Multi-Assay Split' "
        f"RETURN COLLECT(c.uuid) AS {record_field_name}")

    logger.info("======get_component_dataset_uuids() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        uuids_list = record[record_field_name]

        return uuids_list  


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
    query = (f"MATCH (e:{entity_type})-[:ACTIVITY_INPUT|ACTIVITY_OUTPUT*]->(d:Dataset) "
             # Use the string function toLower() to avoid case-sensetivity issue
             f"WHERE e.uuid='{uuid}' AND toLower(d.status) = 'published' "
             # COLLECT() returns a list
             # apoc.coll.toSet() reruns a set containing unique nodes
             f"RETURN COUNT(d) AS {record_field_name}")

    logger.info("======count_attached_published_datasets() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        count = record[record_field_name]

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
properties_to_exclude : list
    A list of node properties to exclude from result

Returns
-------
dict
    The parent dict, can either be a Sample or Donor
"""
def get_sample_direct_ancestor(neo4j_driver, uuid, property_key = None, properties_to_exclude = []):
    result = {}

    if property_key:
        query = (f"MATCH (s:Sample)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # Filter out the Lab entity if it's the ancestor
                 f"WHERE s.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"RETURN parent.{property_key} AS {record_field_name}")
    else:
        if properties_to_exclude:
            query = (f"MATCH (s:Sample)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # Filter out the Lab entity if it's the ancestor
                 f"WHERE s.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"WITH parent AS p "
                 f"RETURN apoc.create.vNode(labels(p), apoc.map.removeKeys(properties(p), {properties_to_exclude})) AS {record_field_name}")
        else:
            query = (f"MATCH (s:Sample)<-[:ACTIVITY_OUTPUT]-(:Activity)<-[:ACTIVITY_INPUT]-(parent:Entity) "
                 # Filter out the Lab entity if it's the ancestor
                 f"WHERE s.uuid='{uuid}' AND parent.entity_type <> 'Lab' "
                 f"RETURN parent AS {record_field_name}")

    logger.info("======get_sample_direct_ancestor() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                result = record[record_field_name]
            else:
                # Convert the entity node to dict
                result = node_to_dict(record[record_field_name])

    return result


"""
Update the properties of an existing entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
entity_data_dict : dict
    The target entity with properties to be updated
uuid : str
    The uuid of target entity 

Returns
-------
dict
    A dictionary of updated entity details returned from the Cypher query
"""
def update_entity(neo4j_driver, entity_type, entity_data_dict, uuid):
    node_properties_map = build_properties_map(entity_data_dict)

    query = (f"MATCH (e:{entity_type}) "
             f"WHERE e.uuid = '{uuid}' "
             f"SET e += {node_properties_map} "
             f"RETURN e AS {record_field_name}")

    logger.info("======update_entity() query======")
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

            return entity_dict
    except TransactionError as te:
        msg = f"TransactionError from calling create_entity(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.error("Failed to commit update_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


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
def create_activity_tx(tx, activity_data_dict):
    node_properties_map = build_properties_map(activity_data_dict)

    query = (f"CREATE (e:Activity) "
             f"SET e = {node_properties_map} "
             f"RETURN e AS {record_field_name}")

    logger.info("======create_activity_tx() query======")
    logger.debug(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    return node


def validate_direct_ancestors(neo4j_driver, entity_uuids, allowed_types, disallowed_property_values=None):
    disallowed_rules_list = disallowed_property_values
    query = """
    MATCH (n)
    WHERE n.uuid IN $uuids
    WITH n,
        any(l IN labels(n) WHERE l IN $allowed_labels) AS label_ok,
        $disallowed AS rules
    WITH n, label_ok,
        any(rule IN rules WHERE n[rule.property] IS NOT NULL AND n[rule.property] = rule.value) AS has_forbidden_prop
    WHERE NOT label_ok OR has_forbidden_prop
    RETURN DISTINCT n.uuid AS invalid_uuid
    """
    with neo4j_driver.session() as session:
        result = session.run(query, 
                             uuids=entity_uuids, 
                             allowed_labels=allowed_types, 
                             disallowed=disallowed_rules_list)
        
        return [record["invalid_uuid"] for record in result]


"""
Build the property key-value pairs to be used in the Cypher clause for node creation/update

Parameters
----------
entity_data_dict : dict
    The target Entity node to be created

Returns
-------
str
    A string representation of the node properties map containing 
    key-value pairs to be used in Cypher clause
"""
def build_properties_map(entity_data_dict):
    separator = ', '
    node_properties_list = []

    for key, value in entity_data_dict.items():
        if isinstance(value, (int, bool)):
            # Treat integer and boolean as is
            key_value_pair = f"{key}: {value}"
        elif isinstance(value, str):
            # Special case is the value is 'TIMESTAMP()' string
            # Remove the quotes since neo4j only takes TIMESTAMP() as a function
            if value == 'TIMESTAMP()':
                key_value_pair = f"{key}: {value}"
            else:
                # Escape single quote
                escaped_str = value.replace("'", r"\'")
                # Quote the value
                key_value_pair = f"{key}: '{escaped_str}'"
        else:
            # Convert list and dict to string, retain the original data without removing any control characters
            # Will need to call schema_manager.convert_str_literal() to convert the list/dict literal back to object
            # Note that schema_manager.convert_str_literal() removes any control characters to avoid SyntaxError 
            # Must also escape single quotes in the string to build a valid Cypher query
            escaped_str = str(value).replace("'", r"\'")
            # Also need to quote the string value
            key_value_pair = f"{key}: '{escaped_str}'"

        # Add to the list
        node_properties_list.append(key_value_pair)

    # Example: {uuid: 'eab7fd6911029122d9bbd4d96116db9b', rui_location: 'Joe <info>', lab_tissue_sample_id: 'dadsadsd'}
    # Note: all the keys are not quoted, otherwise Cypher syntax error
    node_properties_map = f"{{ {separator.join(node_properties_list)} }}"

    return node_properties_map


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
def nodes_to_dicts(nodes):
    dicts = []

    for node in nodes:
        entity_dict = node_to_dict(node)
        dicts.append(entity_dict)

    return dicts


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
"""
def create_relationship_tx(tx, source_node_uuid, target_node_uuid, relationship, direction):
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

    logger.info("======create_relationship_tx() query======")
    logger.debug(query)

    result = tx.run(query)

"""
Create multiple relationships between a target node and each node in
a list of source nodes in neo4j

Parameters
----------
tx : neo4j.Session object
    The neo4j.Session object instance
source_uuid_list : list[str]
    A list of UUIDs for nodes which will have a relationship to the node with target_uuid
target_uuid : str
    The UUID of target node
relationship : Neo4jRelationshipEnum
    The string for the Neo4j relationship type between each source node and the target node.
direction: str
    The relationship direction of each source node to the target node: outgoing `->` or incoming `<-`
    Neo4j CQL CREATE command supports only directional relationships
"""
def _create_relationships_unwind_tx(tx:Neo4jSession, source_uuid_list:list, target_uuid:str
                                   , relationship:Neo4jRelationshipEnum, direction:str)->None:
    logger.info("====== enter _create_relationships_unwind_tx() ======")
    incoming = direction if direction == "<-" else "-"
    outgoing = direction if direction == "->" else "-"

    query = (
        f"MATCH (t {{uuid: $target_uuid}}) "
        f"UNWIND $source_uuid_list AS src_uuid "
        f"MATCH (s {{uuid: src_uuid}}) "
        f"CREATE (s){incoming}[r:{relationship.value}]{outgoing}(t) "
        f"RETURN src_uuid AS linked_uuid"
    )

    result = tx.run(  query=query
                    , target_uuid=target_uuid
                    , source_uuid_list=source_uuid_list)
    logger.info("====== returning from _create_relationships_unwind_tx() ======")

"""
Execute one query to create all outgoing relationships from each node whose
identifier is in the source node list to the target Activity node in neo4j

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
source_node_uuids : list
    A list of UUIDs used as node identifiers for source nodes related to target_node_uuid.
target_node_uuid : str
    The uuid of target Activity node
"""
def create_outgoing_activity_relationships_tx(tx, source_node_uuids:list, activity_node_uuid:str):
    # N.B. Neo4j CQL CREATE command supports only directional relationships
    query = (f"MATCH (e:Entity), (a:Activity)"
             f" WHERE e.uuid IN {source_node_uuids} AND a.uuid = '{activity_node_uuid}'"
             f" CREATE (e) - [r:ACTIVITY_INPUT]->(a)")

    logger.info("======create_outgoing_activity_relationships_tx() query======")
    logger.debug(query)

    result = tx.run(query)

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
def execute_readonly_tx(tx, query):
    result = tx.run(query)
    record = result.single()
    return record


####################################################################################################
## Internal Functions
####################################################################################################

"""
Delete the Activity node and linkages between an entity and its direct ancestors

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
uuid : str
    The uuid to target entity (child of those direct ancestors)
"""
def _delete_activity_node_and_linkages_tx(tx, uuid):
    query = (f"MATCH (s:Entity)-[in:ACTIVITY_INPUT]->(a:Activity)-[out:ACTIVITY_OUTPUT]->(t:Entity) "
             f"WHERE t.uuid = '{uuid}' "
             f"DELETE in, a, out")

    logger.info("======_delete_activity_node_and_linkages_tx() query======")
    logger.debug(query)

    result = tx.run(query)

"""
Delete only the ACTIVITY_INPUT linkages between a target entity and a specific set of its direct ancestors.
The Activity node and the entity nodes remain intact.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The uuid of the target child entity
ancestor_uuids : list
    A list of uuids of ancestors whose relationships should be deleted
"""
def delete_ancestor_linkages_tx(neo4j_driver, entity_uuid, ancestor_uuids):
    query = (
        "MATCH (a:Entity)-[r:ACTIVITY_INPUT]->(activity:Activity)-[:ACTIVITY_OUTPUT]->(t:Entity {uuid: $entity_uuid}) "
        "WHERE a.uuid IN $ancestor_uuids "
        "DELETE r"
    )

    logger.info("======delete_ancestor_linkages_tx() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            tx = session.begin_transaction()

            result = tx.run(
                query, 
                entity_uuid=entity_uuid,
                ancestor_uuids=ancestor_uuids
            )
            
            
            tx.commit()
            
    except TransactionError as te:
        msg = "TransactionError from calling delete_ancestor_linkages_tx(): "
        logger.exception(msg)

        if tx.closed() == False:
            logger.error("Failed to commit delete_ancestor_linkages_tx() transaction, rollback")
            tx.rollback()

        raise TransactionError(msg)

"""
Delete linkages between a publication and its associated collection

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
uuid : str
    The uuid to target publication
"""
def _delete_publication_associated_collection_linkages_tx(tx, uuid):
    query = (f"MATCH (p:Publication)-[r:USES_DATA]->(c:Collection) "
             f"WHERE p.uuid = '{uuid}' "
             f"DELETE r")

    logger.info("======_delete_publication_associated_collection_linkages_tx() query======")
    logger.debug(query)

    result = tx.run(query)

"""
Delete the linkages between a Collection and its member Datasets

Parameters
----------
tx : neo4j.Transaction object
    The neo4j.Transaction object instance
uuid : str
    The uuid of the Collection, related to Datasets by an IN_COLLECTION relationship
"""
def _delete_collection_linkages_tx(tx, uuid):
    query = (f"MATCH (d:Dataset)-[in:IN_COLLECTION]->(c:Collection)"
             f" WHERE c.uuid = '{uuid}' "
             f" DELETE in")

    logger.info("======_delete_collection_linkages_tx() query======")
    logger.debug(query)

    result = tx.run(query)


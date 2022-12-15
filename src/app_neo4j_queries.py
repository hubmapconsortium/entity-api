from neo4j.exceptions import TransactionError
import logging
import json

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
    query = (f"RETURN 1 AS {record_field_name}")

    # Sessions will often be created and destroyed using a with block context
    with neo4j_driver.session() as session:
        # Returned type is a Record object
        record = session.read_transaction(_execute_readonly_tx, query)

        # When record[record_field_name] is not None (namely the cypher result is not null)
        # and the value equals 1
        if record and record[record_field_name] and (record[record_field_name] == 1):
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
    result = {}

    query = (f"MATCH (e:Entity) "
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN e AS {record_field_name}")

    logger.info("======get_entity() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = _node_to_dict(record[record_field_name])

    return result

"""
Get all the entity nodes for the given entity type

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of entity dicts of the given type returned from the Cypher query
"""
def get_entities_by_type(neo4j_driver, entity_type, property_key = None):
    results = []

    if property_key:
        query = (f"MATCH (e:{entity_type}) "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:{entity_type}) "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    logger.info("======get_entities_by_type() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results

"""
Get all the public collection nodes

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
property_key : str
    A target property key for result filtering

Returns
-------
list
    A list of public collections returned from the Cypher query
"""
def get_public_collections(neo4j_driver, property_key = None):
    results = []

    if property_key:
        query = (f"MATCH (e:Collection) "
                 f"WHERE e.registered_doi IS NOT NULL AND e.doi_url IS NOT NULL "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Collection) "
                 f"WHERE e.registered_doi IS NOT NULL AND e.doi_url IS NOT NULL "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")

    logger.info("======get_public_collections() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Retrieve the ancestor organ(s) of a given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_uuid : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
list
    A list of organs that are ancestors of the given entity returned from the Cypher query
"""
def get_ancestor_organs(neo4j_driver, entity_uuid):
    results = []

    # specimen_type -> sample_category 12/15/2022
    query = (f"MATCH (e:Entity {{uuid:'{entity_uuid}'}})<-[*]-(organ:Sample {{sample_category:'organ'}}) "
             # COLLECT() returns a list
             # apoc.coll.toSet() reruns a set containing unique nodes
             f"RETURN apoc.coll.toSet(COLLECT(organ)) AS {record_field_name}")

    logger.info("======get_ancestor_organs() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            results = _nodes_to_dicts(record[record_field_name])

    return results


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

Returns
-------
dict
    A dictionary of newly created entity details returned from the Cypher query
"""
def create_entity(neo4j_driver, entity_type, entity_data_dict):
    node_properties_map = _build_properties_map(entity_data_dict)

    query = (# Always define the Entity label in addition to the target `entity_type` label
             f"CREATE (e:Entity:{entity_type}) "
             f"SET e = {node_properties_map} "
             f"RETURN e AS {record_field_name}")

    logger.info("======create_entity() query======")
    logger.info(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            result = tx.run(query)
            record = result.single()
            entity_node = record[record_field_name]

            entity_dict = _node_to_dict(entity_node)

            # logger.info("======create_entity() resulting entity_dict======")
            # logger.info(entity_dict)

            tx.commit()

            return entity_dict
    except TransactionError as te:
        msg = f"TransactionError from calling create_entity(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit create_entity() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Create multiple sample nodes in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
samples_dict_list : list
    A list of dicts containing the generated data of each sample to be created
activity_dict : dict
    The dict containing generated activity data
direct_ancestor_uuid : str
    The uuid of the direct ancestor to be linked to
"""
def create_multiple_samples(neo4j_driver, samples_dict_list, activity_data_dict, direct_ancestor_uuid):
    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            activity_uuid = activity_data_dict['uuid']

            # Step 1: create the Activity node
            _create_activity_tx(tx, activity_data_dict)

            # Step 2: create relationship from source entity node to this Activity node
            _create_relationship_tx(tx, direct_ancestor_uuid, activity_uuid, 'ACTIVITY_INPUT', '->')

            # Step 3: create each new sample node and link to the Activity node at the same time
            for sample_dict in samples_dict_list:
                node_properties_map = _build_properties_map(sample_dict)

                query = (f"MATCH (a:Activity) "
                         f"WHERE a.uuid = '{activity_uuid}' "
                         # Always define the Entity label in addition to the target `entity_type` label
                         f"CREATE (e:Entity:Sample {node_properties_map} ) "
                         f"CREATE (a)-[:ACTIVITY_OUTPUT]->(e)")

                logger.info("======create_multiple_samples() individual query======")
                logger.info(query)

                result = tx.run(query)

            # Then
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling create_multiple_samples(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit create_multiple_samples() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


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
    node_properties_map = _build_properties_map(entity_data_dict)

    query = (f"MATCH (e:{entity_type}) "
             f"WHERE e.uuid = '{uuid}' "
             f"SET e += {node_properties_map} "
             f"RETURN e AS {record_field_name}")

    logger.info("======update_entity() query======")
    logger.info(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            result = tx.run(query)
            record = result.single()
            entity_node = record[record_field_name]

            tx.commit()

            entity_dict = _node_to_dict(entity_node)

            # logger.info("======update_entity() resulting entity_dict======")
            # logger.info(entity_dict)

            return entity_dict
    except TransactionError as te:
        msg = f"TransactionError from calling create_entity(): {te.value}"
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
    results = []

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
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(ancestor)) AS {record_field_name}")

    logger.info("======get_ancestors() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

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
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(descendant)) AS {record_field_name}")

    logger.info("======get_descendants() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

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
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(parent)) AS {record_field_name}")

    logger.info("======get_parents() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results

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
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(child)) AS {record_field_name}")

    logger.info("======get_children() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results



"""
Get all revisions for a given dataset uuid and sort them in descending order based on their creation time

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity 

Returns
-------
dict
    A list of all the unique revision datasets in DESC order
"""
def get_sorted_revisions(neo4j_driver, uuid):
    results = []

    query = (f"MATCH (prev:Dataset)<-[:REVISION_OF *0..]-(e:Dataset)<-[:REVISION_OF *0..]-(next:Dataset) "
             f"WHERE e.uuid='{uuid}' "
             # COLLECT() returns a list
             # apoc.coll.toSet() reruns a set containing unique nodes
             f"WITH apoc.coll.toSet(COLLECT(next) + COLLECT(e) + COLLECT(prev)) AS collection "
             f"UNWIND collection as node "
             f"WITH node ORDER BY node.created_timestamp DESC "
             f"RETURN COLLECT(node) AS {record_field_name}")

    logger.info("======get_sorted_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # Convert the list of nodes to a list of dicts
            results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Get all previous revisions of the target entity by uuid

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
    A list of unique previous revisions dictionaries returned from the Cypher query
"""
def get_previous_revisions(neo4j_driver, uuid, property_key = None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)-[:REVISION_OF*]->(prev:Entity) "
                 f"WHERE e.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(prev.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:REVISION_OF*]->(prev:Entity) "
                 f"WHERE e.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(prev)) AS {record_field_name}")

    logger.info("======get_previous_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results


"""
Get all next revisions of the target entity by uuid

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
    A list of unique next revisions dictionaries returned from the Cypher query
"""
def get_next_revisions(neo4j_driver, uuid, property_key = None):
    results = []

    if property_key:
        query = (f"MATCH (e:Entity)<-[:REVISION_OF*]-(next:Entity) "
                 f"WHERE e.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(next.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[:REVISION_OF*]-(next:Entity) "
                 f"WHERE e.uuid='{uuid}' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(next)) AS {record_field_name}")

    logger.info("======get_next_revisions() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            if property_key:
                # Just return the list of property values from each entity node
                results = record[record_field_name]
            else:
                # Convert the list of nodes to a list of dicts
                results = _nodes_to_dicts(record[record_field_name])

    return results


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

            query = (f"MATCH (c:Collection), (d:Dataset) "
                     f"WHERE c.uuid = '{collection_uuid}' AND d.uuid IN {dataset_uuids_list_str} "
                     # Use MERGE instead of CREATE to avoid creating the relationship multiple times
                     # MERGE creates the relationship only if there is no existing relationship
                     f"MERGE (c)<-[r:IN_COLLECTION]-(d)")

            logger.info("======add_datasets_to_collection() query======")
            logger.info(query)

            tx.run(query)
            tx.commit()
    except TransactionError as te:
        msg = f"TransactionError from calling add_datasets_to_collection(): {te.value}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        if tx.closed() == False:
            logger.info("Failed to commit add_datasets_to_collection() transaction, rollback")

            tx.rollback()

        raise TransactionError(msg)


"""
Retrive the full tree above the given entity

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target entity: Donor/Sample/Dataset, not Collection 
depth : int
    The maximum number of hops in the traversal
"""
def get_provenance(neo4j_driver, uuid, depth):
    # max_level_str is the string used to put a limit on the number of levels to traverse
    max_level_str = ''
    if depth is not None and len(str(depth)) > 0:
        max_level_str = f"maxLevel: {depth}, "

    # More info on apoc.path.subgraphAll() procedure: https://neo4j.com/labs/apoc/4.0/graph-querying/expand-subgraph/
    query = (f"MATCH (n:Entity) "
             f"WHERE n.uuid = '{uuid}' "
             f"CALL apoc.path.subgraphAll(n, {{ {max_level_str} relationshipFilter:'<ACTIVITY_INPUT|<ACTIVITY_OUTPUT' }}) "
             f"YIELD nodes, relationships "
             f"WITH [node in nodes | node {{ .*, label:labels(node)[0] }} ] as nodes, "
             f"[rel in relationships | rel {{ .*, fromNode: {{ label:labels(startNode(rel))[0], uuid:startNode(rel).uuid }}, toNode: {{ label:labels(endNode(rel))[0], uuid:endNode(rel).uuid }}, rel_data: {{ type: type(rel) }} }} ] as rels "
             f"WITH {{ nodes:nodes, relationships:rels }} as json "
             f"RETURN json")

    logger.info("======get_provenance() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        return session.read_transaction(_execute_readonly_tx, query)


"""
Retrive the latest revision dataset of the given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target dataset
public : bool
    If get back the latest public revision dataset or the real one
"""
def get_dataset_latest_revision(neo4j_driver, uuid, public = False):
    # Defaut the latest revision to this entity itself
    result = get_entity(neo4j_driver, uuid)

    if public:
        # Don't use [r:REVISION_OF] because
        # Binding a variable length relationship pattern to a variable ('r') is deprecated
        query = (f"MATCH (e:Dataset)<-[:REVISION_OF*]-(next:Dataset) "
                 f"WHERE e.uuid='{uuid}' AND next.status='Published' "
                 f"WITH LAST(COLLECT(next)) as latest "
                 f"RETURN latest AS {record_field_name}")
    else:
        query = (f"MATCH (e:Dataset)<-[:REVISION_OF*]-(next:Dataset) "
                 f"WHERE e.uuid='{uuid}' "
                 f"WITH LAST(COLLECT(next)) as latest "
                 f"RETURN latest AS {record_field_name}")

    logger.info("======get_dataset_latest_revision() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        # Only convert when record[record_field_name] is not None (namely the cypher result is not null)
        if record and record[record_field_name]:
            # Convert the neo4j node into Python dict
            result = _node_to_dict(record[record_field_name])

    return result


"""
Retrive the calculated revision number of the given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of target dataset
"""
def get_dataset_revision_number(neo4j_driver, uuid):
    revision_number = 1

    # Don't use [r:REVISION_OF] because
    # Binding a variable length relationship pattern to a variable ('r') is deprecated
    query = (f"MATCH (e:Dataset)-[:REVISION_OF*]->(prev:Dataset) "
             f"WHERE e.uuid='{uuid}' "
             f"RETURN COUNT(prev) AS {record_field_name}")

    logger.info("======get_dataset_revision_number() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            # The revision number is the count of previous revisions plus 1
            revision_number = record[record_field_name] + 1

    return revision_number


"""
Retrieve the list of uuids for organs associated with a given dataset

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
uuid : str
    The uuid of the target entity: Dataset
"""
def get_associated_organs_from_dataset(neo4j_driver, dataset_uuid):
    results = []

    # specimen_type -> sample_category 12/15/2022
    query = (f"MATCH (ds:Dataset)<-[*]-(organ:Sample {{sample_category:'organ'}}) "
             f"WHERE ds.uuid='{dataset_uuid}'"
             f"RETURN apoc.coll.toSet(COLLECT(organ)) AS {record_field_name}")

    logger.info("======get_associated_organs_from_dataset() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

        if record and record[record_field_name]:
            results = _nodes_to_dicts(record[record_field_name])

    return results

"""
Retrieve all the provenance information about each dataset. Each dataset's prov-info is given by a dictionary. 
Certain fields such as first sample where there can be multiple nearest datasets in the provenance above a given
dataset, that field is a list inside of its given dictionary. Results can be filtered with certain parameters:
has_rui_info (true or false), organ (organ type), group_uuid, and dataset_status. These are passed in as a dictionary if
they are present.

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
param_dict : dictionary
    Dictionary containing any parameters desired to filter for certain results
published_only : boolean
    If a user does not have a token with HuBMAP-Read Group access, published_only is set to true. This will cause only 
    datasets with status = 'Published' to be included in the result.
"""
def get_prov_info(neo4j_driver, param_dict, published_only):
    group_uuid_query_string = ''
    organ_query_string = 'OPTIONAL MATCH'
    organ_where_clause = ""
    rui_info_query_string = 'OPTIONAL MATCH (ds)<-[*]-(ruiSample:Sample)'
    rui_info_where_clause = "WHERE NOT ruiSample.rui_location IS NULL AND NOT trim(ruiSample.rui_location) = '' "
    dataset_status_query_string = ''
    published_only_query_string = ''
    published_only_revisions_string = ''
    if 'group_uuid' in param_dict:
        group_uuid_query_string = f" AND toUpper(ds.group_uuid) = '{param_dict['group_uuid'].upper()}'"
    if 'organ' in param_dict:
        organ_query_string = 'MATCH'
        organ_where_clause = f" WHERE toUpper(organ.organ) = '{param_dict['organ'].upper()}'"
    if 'has_rui_info' in param_dict:
        rui_info_query_string = 'MATCH (ds)<-[*]-(ruiSample:Sample)'
        if param_dict['has_rui_info'].lower() == 'false':
            rui_info_query_string = 'MATCH (ds:Dataset)'
            rui_info_where_clause = "WHERE NOT EXISTS {MATCH (ds)<-[*]-(ruiSample:Sample) WHERE NOT ruiSample.rui_location IS NULL AND NOT TRIM(ruiSample.rui_location) = ''} MATCH (ds)<-[*]-(ruiSample:Sample)"
    if 'dataset_status' in param_dict:
        dataset_status_query_string = f" AND toUpper(ds.status) = '{param_dict['dataset_status'].upper()}'"
    if published_only:
        published_only_query_string = f" AND toUpper(ds.status) = 'PUBLISHED'"
        published_only_revisions_string = f" AND toUpper(rev.status) = 'PUBLISHED'"
    query = (f"MATCH (ds:Dataset)<-[:ACTIVITY_OUTPUT]-(a)<-[:ACTIVITY_INPUT]-(firstSample:Sample)<-[*]-(donor:Donor)"
             f"WHERE not (ds)-[:REVISION_OF]->(:Dataset)"
             f"{group_uuid_query_string}"
             f"{dataset_status_query_string}"
             f"{published_only_query_string}"
             f" WITH ds, COLLECT(distinct donor) AS DONOR, COLLECT(distinct firstSample) AS FIRSTSAMPLE"
             f" OPTIONAL MATCH (ds)<-[:REVISION_OF]-(rev:Dataset)"
             f"{published_only_revisions_string}"
             f" WITH ds, DONOR, FIRSTSAMPLE, COLLECT(rev.hubmap_id) as REVISIONS"
             f" OPTIONAL MATCH (ds)<-[*]-(metaSample:Sample)"
             f" WHERE NOT metaSample.metadata IS NULL AND NOT TRIM(metaSample.metadata) = ''"
             f" WITH ds, FIRSTSAMPLE, DONOR, REVISIONS, collect(distinct metaSample) as METASAMPLE"
             f" {rui_info_query_string}"
             f" {rui_info_where_clause}"
             f" WITH ds, FIRSTSAMPLE, DONOR, REVISIONS, METASAMPLE, collect(distinct ruiSample) as RUISAMPLE"
             # specimen_type -> sample_category 12/15/2022
             f" {organ_query_string} (donor)-[:ACTIVITY_INPUT]->(oa)-[:ACTIVITY_OUTPUT]->(organ:Sample {{sample_category:'organ'}})-[*]->(ds)"
             f" {organ_where_clause}"
             f" WITH ds, FIRSTSAMPLE, DONOR, REVISIONS, METASAMPLE, RUISAMPLE, COLLECT(DISTINCT organ) AS ORGAN "
             f" OPTIONAL MATCH (ds)-[:ACTIVITY_INPUT]->(a3)-[:ACTIVITY_OUTPUT]->(processed_dataset:Dataset)"
             f" WITH ds, FIRSTSAMPLE, DONOR, REVISIONS, METASAMPLE, RUISAMPLE, ORGAN, COLLECT(distinct processed_dataset) AS PROCESSED_DATASET"
             f" RETURN ds.uuid, FIRSTSAMPLE, DONOR, RUISAMPLE, ORGAN, ds.hubmap_id, ds.status, ds.group_name,"
             f" ds.group_uuid, ds.created_timestamp, ds.created_by_user_email, ds.last_modified_timestamp, "
             f" ds.last_modified_user_email, ds.lab_dataset_id, ds.data_types, METASAMPLE, PROCESSED_DATASET, REVISIONS")

    logger.info("======get_prov_info() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        # Because we're returning multiple things, we use session.run rather than session.read_transaction
        result = session.run(query)
        list_of_dictionaries = []
        for record in result:
            record_dict = {}
            record_contents = []
            # Individual items within a record are non subscriptable. By putting then in a small list, we can address
            # Each item in a record.
            for item in record:
                record_contents.append(item)
            record_dict['uuid'] = record_contents[0]
            content_one = []
            for entry in record_contents[1]:
                node_dict = _node_to_dict(entry)
                content_one.append(node_dict)
            record_dict['first_sample'] = content_one
            content_two = []
            for entry in record_contents[2]:
                node_dict = _node_to_dict(entry)
                content_two.append(node_dict)
            record_dict['distinct_donor'] = content_two
            content_three = []
            for entry in record_contents[3]:
                node_dict = _node_to_dict(entry)
                content_three.append(node_dict)
            record_dict['distinct_rui_sample'] = content_three
            content_four = []
            for entry in record_contents[4]:
                node_dict = _node_to_dict(entry)
                content_four.append(node_dict)
            record_dict['distinct_organ'] = content_four
            record_dict['hubmap_id'] = record_contents[5]
            record_dict['status'] = record_contents[6]
            record_dict['group_name'] = record_contents[7]
            record_dict['group_uuid'] = record_contents[8]
            record_dict['created_timestamp'] = record_contents[9]
            record_dict['created_by_user_email'] = record_contents[10]
            record_dict['last_modified_timestamp'] = record_contents[11]
            record_dict['last_modified_user_email'] = record_contents[12]
            record_dict['lab_dataset_id'] = record_contents[13]
            data_types = record_contents[14]
            data_types = data_types.replace("'", '"')
            data_types = json.loads(data_types)
            record_dict['data_types'] = data_types
            content_fifteen = []
            for entry in record_contents[15]:
                node_dict = _node_to_dict(entry)
                content_fifteen.append(node_dict)
            record_dict['distinct_metasample'] = content_fifteen
            content_sixteen = []
            for entry in record_contents[16]:
                node_dict = _node_to_dict(entry)
                content_sixteen.append(node_dict)
            record_dict['processed_dataset'] = content_sixteen
            content_seventeen = []
            for entry in record_contents[17]:
                node_dict = _node_to_dict(entry)
                content_seventeen.append(node_dict)
            record_dict['previous_version_hubmap_ids'] = content_seventeen
            list_of_dictionaries.append(record_dict)
    return list_of_dictionaries


"""
Returns all of the same information as get_prov_info however only for a single dataset at a time. Returns a dictionary
containing all of the provenance info for a given dataset. For fields such as first sample where there can be multiples,
they are presented in their own dictionary converted from their nodes in neo4j and placed into a list. 

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
dataset_uuid : string
    the uuid of the desired dataset
"""
def get_individual_prov_info(neo4j_driver, dataset_uuid):
    query = (f"MATCH (ds:Dataset {{uuid: '{dataset_uuid}'}})<-[*]-(firstSample:Sample)<-[*]-(donor:Donor)"
             f" WHERE (:Dataset)<-[]-()<-[]-(firstSample)"
             f" WITH ds, COLLECT(distinct donor) AS DONOR, COLLECT(distinct firstSample) AS FIRSTSAMPLE"
             f" OPTIONAL MATCH (ds)<-[*]-(metaSample:Sample)"
             f" WHERE NOT metaSample.metadata IS NULL AND NOT TRIM(metaSample.metadata) = ''"
             f" WITH ds, FIRSTSAMPLE, DONOR, COLLECT(distinct metaSample) AS METASAMPLE"
             f" OPTIONAL MATCH (ds)<-[*]-(ruiSample:Sample)"
             f" WHERE NOT ruiSample.rui_location IS NULL AND NOT TRIM(ruiSample.rui_location) = ''"
             f" WITH ds, FIRSTSAMPLE, DONOR, METASAMPLE, COLLECT(distinct ruiSample) AS RUISAMPLE"
             # specimen_type -> sample_category 12/15/2022
             f" OPTIONAL match (donor)-[:ACTIVITY_INPUT]->(oa)-[:ACTIVITY_OUTPUT]->(organ:Sample {{sample_category:'organ'}})-[*]->(ds)"
             f" WITH ds, FIRSTSAMPLE, DONOR, METASAMPLE, RUISAMPLE, COLLECT(distinct organ) AS ORGAN "
             f" OPTIONAL MATCH (ds)-[:ACTIVITY_INPUT]->(a3)-[:ACTIVITY_OUTPUT]->(processed_dataset:Dataset)"
             f" WITH ds, FIRSTSAMPLE, DONOR, METASAMPLE, RUISAMPLE, ORGAN, COLLECT(distinct processed_dataset) AS PROCESSED_DATASET"
             f" RETURN ds.uuid, FIRSTSAMPLE, DONOR, RUISAMPLE, ORGAN, ds.hubmap_id, ds.status, ds.group_name,"
             f" ds.group_uuid, ds.created_timestamp, ds.created_by_user_email, ds.last_modified_timestamp, "
             f" ds.last_modified_user_email, ds.lab_dataset_id, ds.data_types, METASAMPLE, PROCESSED_DATASET")
    logger.info("======get_prov_info() query======")
    logger.info(query)

    record_contents = []
    record_dict = {}
    with neo4j_driver.session() as session:
        result = session.run(query)
        if result.peek() is None:
            return
        for record in result:
            for item in record:
                record_contents.append(item)
            record_dict['uuid'] = record_contents[0]
            content_one = []
            for entry in record_contents[1]:
                node_dict = _node_to_dict(entry)
                content_one.append(node_dict)
            record_dict['first_sample'] = content_one
            content_two = []
            for entry in record_contents[2]:
                node_dict = _node_to_dict(entry)
                content_two.append(node_dict)
            record_dict['distinct_donor'] = content_two
            content_three = []
            for entry in record_contents[3]:
                node_dict = _node_to_dict(entry)
                content_three.append(node_dict)
            record_dict['distinct_rui_sample'] = content_three
            content_four = []
            for entry in record_contents[4]:
                node_dict = _node_to_dict(entry)
                content_four.append(node_dict)
            record_dict['distinct_organ'] = content_four
            record_dict['hubmap_id'] = record_contents[5]
            record_dict['status'] = record_contents[6]
            record_dict['group_name'] = record_contents[7]
            record_dict['group_uuid'] = record_contents[8]
            record_dict['created_timestamp'] = record_contents[9]
            record_dict['created_by_user_email'] = record_contents[10]
            record_dict['last_modified_timestamp'] = record_contents[11]
            record_dict['last_modified_user_email'] = record_contents[12]
            record_dict['lab_dataset_id'] = record_contents[13]
            data_types = record_contents[14]
            data_types = data_types.replace("'", '"')
            data_types = json.loads(data_types)
            record_dict['data_types'] = data_types
            content_fifteen = []
            for entry in record_contents[15]:
                node_dict = _node_to_dict(entry)
                content_fifteen.append(node_dict)
            record_dict['distinct_metasample'] = content_fifteen
            content_sixteen = []
            for entry in record_contents[16]:
                node_dict = _node_to_dict(entry)
                content_sixteen.append(node_dict)
            record_dict['processed_dataset'] = content_sixteen
    return record_dict


"""
Returns all of the Sample information associated with a Dataset, back to each Donor. Returns a dictionary
containing all of the provenance info for a given dataset. Each Sample is in its own dictionary, converted
from its neo4j node and placed into a list. 

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
dataset_uuid : string
    the uuid of the desired dataset
"""
def get_all_dataset_samples(neo4j_driver, dataset_uuid):
    query = f"MATCH p = (ds:Dataset {{uuid: '{dataset_uuid}'}})<-[*]-(dn:Donor) return p"
    logger.info("======get_all_dataset_samples() query======")
    logger.info(query)

    # Dictionary of Dictionaries, keyed by UUID, containing each Sample returned in the Neo4j Path
    dataset_sample_list = {}
    with neo4j_driver.session() as session:
        result = session.run(query)
        if result.peek() is None:
            return
        for record in result:
            for item in record:
                for node in item.nodes:
                    if node["entity_type"] == 'Sample':
                        if not node["uuid"] in dataset_sample_list:
                            # specimen_type -> sample_category 12/15/2022
                            dataset_sample_list[node["uuid"]] = {'sample_category': node["sample_category"]}
    return dataset_sample_list

"""
specimen_type -> sample_category 12/15/2022

Returns group_name, data_types, and status for every primary dataset. Also returns the organ type for the closest 
sample above the dataset in the provenance where {specimen_type: 'organ'}.  

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
"""
def get_sankey_info(neo4j_driver):
    query = (f"MATCH (ds:Dataset)<-[]-(a)<-[]-(:Sample)"
             # specimen_type -> sample_category 12/15/2022
             f"MATCH (donor)-[:ACTIVITY_INPUT]->(oa)-[:ACTIVITY_OUTPUT]->(organ:Sample {{sample_category:'organ'}})-[*]->(ds)"
             f"RETURN distinct ds.group_name, organ.organ, ds.data_types, ds.status, ds. uuid order by ds.group_name")
    logger.info("======get_sankey_info() query======")
    logger.info(query)
    with neo4j_driver.session() as session:
        # Because we're returning multiple things, we use session.run rather than session.read_transaction
        result = session.run(query)
        list_of_dictionaries = []
        for record in result:
            record_dict = {}
            record_contents = []
            # Individual items within a record are non subscriptable. By putting then in a small list, we can address
            # Each item in a record.
            for item in record:
                record_contents.append(item)
            record_dict['dataset_group_name'] = record_contents[0]
            record_dict['organ_type'] = record_contents[1]
            data_types_list = record_contents[2]
            data_types_list = data_types_list.replace("'", '"')
            data_types_list = json.loads(data_types_list)
            data_types = data_types_list[0]
            if (len(data_types_list)) > 1:
                if (data_types_list[0] == "scRNAseq-10xGenomics-v3" and data_types_list[1] == "snATACseq") or (data_types_list[1] == "scRNAseq-10xGenomics-v3" and data_types_list[0] == "snATACseq"):
                    data_types = "scRNA-seq (10x Genomics v3),snATAC-seq"
            record_dict['dataset_data_types'] = data_types
            record_dict['dataset_status'] = record_contents[3]
            list_of_dictionaries.append(record_dict)
        return list_of_dictionaries


"""
Returns sample uuid, sample rui location, sample metadata, sample group name, sample created_by_email, sample ancestor
uuid, sample ancestor entity type, organ uuid, organ type, lab tissue sample id, donor uuid, donor 
metadata, sample_hubmap_id, organ_hubmap_id, donor_hubmap_id, sample_submission_id, organ_submission_id,
 donor_submission_id, and sample_type all in a dictionary

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
param_dict : dictionary
    dictionary containing any filters to be applied in the samples-prov-info query
public_only : boolean
    This value indicates whether the query should return all samples, or only samples where data_access_level = 'Public'
"""
def get_sample_prov_info(neo4j_driver, param_dict, public_only):
    group_uuid_query_string = ''
    public_only_query_string = ''
    clause_modifier = "WHERE"
    if 'group_uuid' in param_dict:
        group_uuid_query_string = f" WHERE toUpper(s.group_uuid) = '{param_dict['group_uuid'].upper()}'"
        clause_modifier = "AND"
    if public_only:
        public_only_query_string = f" {clause_modifier} toUpper(s.data_access_level) = 'PUBLIC'"
    query = (
        f" MATCH (s:Sample)<-[*]-(d:Donor)"
        f" {group_uuid_query_string}"
        f" {public_only_query_string}"
        f" WITH s, d"
        # specimen_type -> sample_category 12/15/2022
        f" OPTIONAL MATCH (s)<-[*]-(organ:Sample{{sample_category: 'organ'}})"
        f" WITH s, organ, d"
        f" MATCH (s)<-[]-()<-[]-(da)"
        f" RETURN s.uuid, s.lab_tissue_sample_id, s.group_name, s.created_by_user_email, s.metadata, s.rui_location,"
        f" d.uuid, d.metadata, organ.uuid, organ.sample_category, organ.metadata, da.uuid, da.entity_type, "
        f"s.sample_category, organ.organ, s.organ, s.hubmap_id, s.submission_id, organ.hubmap_id, organ.submission_id, "
        f"d.hubmap_id, d.submission_id"
    )

    logger.info("======get_sample_prov_info() query======")
    logger.info(query)

    with neo4j_driver.session() as session:
        # Because we're returning multiple things, we use session.run rather than session.read_transaction
        result = session.run(query)
        list_of_dictionaries = []
        for record in result:
            record_dict = {}
            record_contents = []
            # Individual items within a record are not subscriptable. By putting them in a small list, we can address
            # each item in a record
            for item in record:
                record_contents.append(item)
            record_dict['sample_uuid'] = record_contents[0]
            record_dict['lab_sample_id'] = record_contents[1]
            record_dict['sample_group_name'] = record_contents[2]
            record_dict['sample_created_by_email'] = record_contents[3]
            record_dict['sample_metadata'] = record_contents[4]
            record_dict['sample_rui_info'] = record_contents[5]
            record_dict['donor_uuid'] = record_contents[6]
            record_dict['donor_metadata'] = record_contents[7]
            record_dict['organ_uuid'] = record_contents[8]
            record_dict['organ_type'] = record_contents[9]
            record_dict['organ_metadata'] = record_contents[10]
            record_dict['sample_ancestor_id'] = record_contents[11]
            record_dict['sample_ancestor_entity'] = record_contents[12]

            # sample_specimen_type -> sample_category 12/15/2022
            record_dict['sample_category'] = record_contents[13]

            record_dict['organ_organ_type'] = record_contents[14]
            record_dict['sample_organ'] = record_contents[15]
            record_dict['sample_hubmap_id'] = record_contents[16]
            record_dict['sample_submission_id'] = record_contents[17]
            record_dict['organ_hubmap_id'] = record_contents[18]
            record_dict['organ_submission_id'] = record_contents[19]
            record_dict['donor_hubmap_id'] = record_contents[20]
            record_dict['donor_submission_id'] = record_contents[21]

            list_of_dictionaries.append(record_dict)
    return list_of_dictionaries


"""
Returns "data_types", "donor_hubmap_id", "donor_submission_id", "hubmap_id", "organ", "organization", 
"provider_experiment_id", "uuid" in a dictionary

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
"""
def get_unpublished(neo4j_driver):
    query = (
        "MATCH (ds:Dataset)<-[*]-(d:Donor) "
        "WHERE ds.status <> 'Published' and ds.status <> 'Hold' "
        # specimen_type -> sample_category 12/15/2022
        "OPTIONAL MATCH (ds)<-[*]-(s:Sample {sample_category:'organ'}) "
        "RETURN distinct ds.data_types as data_types, ds.group_name as organization, ds.uuid as uuid, "
        "ds.hubmap_id as hubmap_id, s.organ as organ, d.hubmap_id as donor_hubmap_id, "
        "d.submission_id as donor_submission_id, ds.lab_dataset_id as provider_experiment_id"
    )

    with neo4j_driver.session() as session:
        rval = session.run(query).data()
        return rval

"""
Returns a list of dictionaries corresponding to matches to the neo4j query
containing uuid of matched datasets (if any) and their status

Paramters
---------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool

uuid
----
The id of the dataset who's paired datasets will be returned

data_type
---------
The datatype of the paired datasets being searched for

search_depth (optional)
----------------------
The max number of generations that will be searched beneath the 
sample ancestor of the given dataset uuid. The value given will be
doubled in the query so that it only counts dataset nodes, not 
activity nodes between datasets. For example, a value of "2" will search
(s:Sample)-[r1]->(a1:Activity)-[r2]->(d1:Dataset)-[r3]->(a2:Activity)-[r4]->(d2:Dataset)
or 2*2 nodes beyond the sample
"""
def get_paired_dataset(neo4j_driver, uuid, data_type, search_depth):
    # search depth is doubled because there is an activity node between each entity node
    number_of_jumps = f"*"
    if search_depth is not None:
        search_depth = 2 * search_depth
        number_of_jumps = f"*..{search_depth}"
    data_type = f"['{data_type}']"
    query = (
        f'MATCH (ds:Dataset)<-[*]-(s:Sample) WHERE ds.uuid = "{uuid}" AND (:Dataset)<-[]-()<-[]-(s)'
        f'MATCH (ods)<-[{number_of_jumps}]-(s) WHERE ods.data_types = "{data_type}"'
        f'return ods.uuid as uuid, ods.status as status'
    )
    paired_datasets = []
    with neo4j_driver.session() as session:
        rval = session.run(query).data()
        return rval

####################################################################################################
## Internal Functions
####################################################################################################

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
def _build_properties_map(entity_data_dict):
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
            # Convert list and dict to string
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
def _create_relationship_tx(tx, source_node_uuid, target_node_uuid, relationship, direction):
    incoming = "-"
    outgoing = "-"

    if direction == "<-":
        incoming = direction

    if direction == "->":
        outgoing = direction

    query = (f"MATCH (s), (t) " +
             f"WHERE s.uuid = '{source_node_uuid}' AND t.uuid = '{target_node_uuid}' "
             f"CREATE (s){incoming}[r:{relationship}]{outgoing}(t) "
             f"RETURN type(r) AS {record_field_name}")

    logger.info("======_create_relationship_tx() query======")
    logger.info(query)

    result = tx.run(query)


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
    node_properties_map = _build_properties_map(activity_data_dict)

    query = (f"CREATE (e:Activity) "
             f"SET e = {node_properties_map} "
             f"RETURN e AS {record_field_name}")

    logger.info("======_create_activity_tx() query======")
    logger.info(query)

    result = tx.run(query)
    record = result.single()
    node = record[record_field_name]

    return node

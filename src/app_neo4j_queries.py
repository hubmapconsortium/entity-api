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
    query = (f"RETURN 1 AS {record_field_name}")

    # Sessions will often be created and destroyed using a with block context
    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)
    
    if record[record_field_name] == 1:
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
    query = (f"MATCH (e:Entity) "
             f"WHERE e.uuid = '{uuid}' "
             f"RETURN e AS {record_field_name}")

    logger.debug("======get_entity() query======")
    logger.debug(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    # Convert the neo4j node into Python dict
    return _node_to_dict(record[record_field_name])

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

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)
       
    # Data handling should happen outside the neo4j session 
    if property_key:
        # Just return the list of property values from each entity node
        return record[record_field_name]
    else:
        # Convert the list of nodes to a list of dicts
        return _nodes_to_dicts(record[record_field_name])

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
    if property_key:
        query = (f"MATCH (e:Collection) "
                 f"WHERE e.has_doi = true "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Collection) "
                 f"WHERE e.has_doi = true "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(e)) AS {record_field_name}")
    
    logger.info("======get_public_collections() query======")
    logger.info(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if property_key:
        # Just return the list of property values from each entity node
        return record[record_field_name]
    else:
        # Convert the list of nodes to a list of dicts
        return _nodes_to_dicts(record[record_field_name])


"""
Create a new entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
entity_json_list_str : str
    The string representation of a list containing only one Entity node to be created

Returns
-------
dict
    A dictionary of newly created entity details returned from the Cypher query
"""
def create_entity(neo4j_driver, entity_type, entity_json_list_str):
    # UNWIND expects json.entities to be List<T>
    query = (f"WITH apoc.convert.fromJsonList('{entity_json_list_str}') AS entities_list "
             f"UNWIND entities_list AS data "
             # Always define the Entity label in addition to the target `entity_type` label
             f"CREATE (e:Entity:{entity_type}) "
             f"SET e = data "
             f"RETURN e AS {record_field_name}")

    logger.debug("======create_entity() query======")
    logger.debug(query)

    try:
        with neo4j_driver.session() as session:
            entity_dict = {}

            tx = session.begin_transaction()

            result = tx.run(query)
            record = result.single()
            entity_node = record[record_field_name]

            entity_dict = _node_to_dict(entity_node)

            logger.debug("======create_entity() resulting entity_dict======")
            logger.debug(entity_dict)

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
Update the properties of an existing entity node in neo4j

Parameters
----------
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
json_list_str : str
    The string representation of a list containing only one entity to be created
uuid : str
    The uuid of target entity 

Returns
-------
dict
    A dictionary of updated entity details returned from the Cypher query
"""
def update_entity(neo4j_driver, entity_type, json_list_str, uuid):
    # UNWIND expects json.entities to be List<T>
    query = (f"WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list "
             f"UNWIND entities_list AS data "
             f"MATCH (e:{entity_type}) "
             f"WHERE e.uuid = '{uuid}' "
             # https://neo4j.com/docs/cypher-manual/current/clauses/set/#set-setting-properties-using-map
             # `+=` is the magic here:
             # Any properties in the map that are not on the node will be added.
             # Any properties not in the map that are on the node will be left as is.
             # Any properties that are in both the map and the node will be replaced in the node. However, if any property in the map is null, it will be removed from the node.
             f"SET e += data RETURN e AS {record_field_name}")
    
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

            entity_dict = _node_to_dict(entity_node)

            logger.debug("======update_entity() resulting entity_dict======")
            logger.debug(entity_dict)

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
    if property_key:
        query = (f"MATCH (e:Entity)<-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]-(ancestor:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND ancestor.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(ancestor.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)<-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]-(ancestor:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND ancestor.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(ancestor)) AS {record_field_name}")

    logger.debug("======get_ancestors() query======")
    logger.debug(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if property_key:
        # Just return the list of property values from each entity node
        return record[record_field_name]
    else:
        # Convert the list of nodes to a list of dicts
        return _nodes_to_dicts(record[record_field_name])

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
        query = (f"MATCH (e:Entity)-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(descendant:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND descendant.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(descendant.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[r:ACTIVITY_INPUT|:ACTIVITY_OUTPUT*]->(descendant:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND descendant.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(descendant)) AS {record_field_name}")

    logger.debug("======get_descendants() query======")
    logger.debug(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if property_key:
        # Just return the list of property values from each entity node
        return record[record_field_name]
    else:
        # Convert the list of nodes to a list of dicts
        return _nodes_to_dicts(record[record_field_name])

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

    logger.debug("======get_parents() query======")
    logger.debug(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if property_key:
        # Just return the list of property values from each entity node
        return record[record_field_name]
    else:
        # Convert the list of nodes to a list of dicts
        return _nodes_to_dicts(record[record_field_name])


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
        query = (f"MATCH (e:Entity)-[:ACTIVITY_INPUT]->(:Activity)-[:ACTIVITY_OUTPUT]->(child:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND child.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(child.{property_key})) AS {record_field_name}")
    else:
        query = (f"MATCH (e:Entity)-[:ACTIVITY_INPUT]->(:Activity)-[:ACTIVITY_OUTPUT]->(child:Entity) "
                 # Filter out the Lab entities
                 f"WHERE e.uuid='{uuid}' AND child.entity_type <> 'Lab' "
                 # COLLECT() returns a list
                 # apoc.coll.toSet() reruns a set containing unique nodes
                 f"RETURN apoc.coll.toSet(COLLECT(child)) AS {record_field_name}")

    logger.debug("======get_children() query======")
    logger.debug(query)

    record = None
    with neo4j_driver.session() as session:
        record = session.read_transaction(_execute_readonly_tx, query)

    if property_key:
        # Just return the list of property values from each entity node
        return record[record_field_name]
    else:
        # Convert the list of nodes to a list of dicts
        return _nodes_to_dicts(record[record_field_name])


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
                     f"WHERE c.uuid = '{uuid}' AND d.uuid IN {dataset_uuids_list_str} "
                     # Use MERGE instead of CREATE to avoid creating the relationship multiple times
                     # MERGE creates the relationship only if there is no existing relationship
                     f"MERGE (c)<-[r:IN_COLLECTION]-(d)") 

            logger.debug("======add_datasets_to_collection() query======")
            logger.debug(query)

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

    logger.debug("======get_provenance() query======")
    logger.debug(query)

    with neo4j_driver.session() as session:
        return session.read_transaction(_execute_readonly_tx, query)


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

    query = (f"MATCH (s), (t) " +
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
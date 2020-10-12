from neo4j import CypherError

def get_entity_by_uuid(neo4j_driver, entity_type, uuid):
    nodes = []
    entity = {}

    with neo4j_driver.session() as session:
        try:
            # Python neo4j driver doesn't support variable for label
            if entity_type == "Dataset":
                query = "MATCH (e:Dataset) WHERE e.uuid=$uuid RETURN e"
            if entity_type == "Donor":
                query = "MATCH (e:Donor) WHERE e.uuid=$uuid RETURN e"
            if entity_type == "Sample":
                query = "MATCH (e:Sample) WHERE e.uuid=$uuid RETURN e"

            results = session.run(query, uuid = uuid)

            for record in results:
                nodes.append(record.get('e'))
            
            # Return an empty dict if no match
            if len(nodes) < 1:
                return entity

            # Raise an exception if multiple nodes found
            if len(nodes) > 1:
                raise Exception("Two or more entity nodes with same uuid found in the Neo4j database.")
            
            # Get all properties of the target node
            for key, value in nodes[0]._properties.items():
                entity.setdefault(key, value)

            # Return the dict
            return entity
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

def create_entity(neo4j_driver, entity_type, json_data):
    entity = {}

    with neo4j_driver.session() as session:
        try:
            # Python neo4j driver doesn't support variable for label
            if entity_type == "Dataset":
                query = "CREATE (e:Dataset {uuid: $uuid}) return e"

            result = session.run(query, uuid = json_data['uuid'])

            # Get all properties of the target node
            for key, value in result.single().get('e')._properties.items():
                entity.setdefault(key, value)

            # Return the dict
            return entity
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e
from neo4j import CypherError

# id is either a `uuid` or `hubmap_id` (like HBM123.ABCD.987)
def get_entity(neo4j_driver, entity_type, id):
    nodes = []
    entity = {}

    with neo4j_driver.session() as session:
        try:
            parameterized_query = "MATCH (e:{entity_type}) WHERE e.uuid='{id}' OR e.hubmap_id='{id}' RETURN e"
            query = parameterized_query.format(entity_type = entity_type, id = id)

            print(query)

            results = session.run(query)

            for record in results:
                nodes.append(record.get('e'))
            
            # Return an empty dict if no match
            if len(nodes) < 1:
                return entity

            # Raise an exception if multiple nodes found
            if len(nodes) > 1:
                raise Exception("Two or more entity nodes with same id found in the Neo4j database.")
            
            # Get all properties of the target node
            for key, value in nodes[0]._properties.items():
                entity.setdefault(key, value)

            # Return the entity dict
            return entity
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e

def create_entity(neo4j_driver, entity_type, json_list_str):
    entity = {}

    with neo4j_driver.session() as session:
        try:
            # UNWIND expects json.entities to be List<T>
            parameterized_query = "WITH apoc.convert.fromJsonList('{json_list_str}') AS entities_list UNWIND entities_list AS data CREATE (e:{entity_type}) SET e = data RETURN e"
            query = parameterized_query.format(json_list_str = json_list_str, entity_type = entity_type)

            print(query)

            result = session.run(query)

            # Get all properties of the target node
            for key, value in result.single().get('e')._properties.items():
                entity.setdefault(key, value)

            # Return the entity dict
            return entity
        except CypherError as ce:
            raise CypherError('A Cypher error was encountered: ' + ce.message)
        except Exception as e:
            raise e
from neo4j import GraphDatabase

# Python modules are first-class runtime objects, 
# they effectively become singletons, 
# initialized at the time of first import.
def initialize(uri, user, password)
    # Two leading underscores signals to Python that 
    # you want the variable to be "private" to the module
    global __NEO4J_DRIVER__

    if __NEO4J_DRIVER__ is not None:
        raise RuntimeError("You cannot create another neo4j_driver instance")

    __NEO4J_DRIVER__ = GraphDatabase.driver(uri, auth=(user, password))

    return __NEO4J_DRIVER__

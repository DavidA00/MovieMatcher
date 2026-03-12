import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")
DATABASE = os.getenv("NEO4J_DATABASE")


class Neo4jConnection:

    def __init__(self):
        self.driver = GraphDatabase.driver(
            URI,
            auth=(USER, PASSWORD)
        )

    def close(self):
        self.driver.close()

    def query(self, query, parameters=None):
        with self.driver.session(database=DATABASE) as session:
            result = session.run(query, parameters)
            return [r.data() for r in result]
import os
import sys
from dotenv import load_dotenv
import json
import re
from neo4j import GraphDatabase, basic_auth
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()


class Neo4jDriver:
    
    driver = GraphDatabase.driver(
        'bolt://' + os.getenv("HOST") + ':' + os.getenv("PORT"),
        auth=basic_auth(os.getenv("AUTH_USERNAME"), os.getenv("AUTH_PASSWORD")),
        max_connection_lifetime=15 * 60,
        max_connection_pool_size=500
    )

    def __init__(self):
        # Cấu hình logging
        logging.basicConfig(
            level=logging.INFO,  # Mức log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format='%(asctime)s - %(levelname)s - %(message)s',  # Định dạng log
            datefmt='%Y-%m-%d %H:%M:%S'  # Định dạng thời gian
        )
        # Log thông tin khởi tạo
        logging.info("Neo4jDriver initialized with HOST: %s", os.getenv("HOST"))

        self.db = self.get_db()
        self.count = 0

    def close(self):
        """Đóng kết nối với Neo4j."""
        self.driver.close()
        logging.info("Neo4j connection closed.")

    def get_db(self):
        """Trả về session của Neo4j."""
        return self.driver.session()

    @staticmethod
    def add_updated_timestamp(symbol):
        """Thêm timestamp cho node."""
        return " " + symbol + ".updated_at = localdatetime({timezone: 'Asia/Ho_Chi_Minh'}) "

    @staticmethod
    def add_updated_timestamp_relationship(symbol):
        """Thêm timestamp cho relationship."""
        return " " + symbol + ".updated_at = localdatetime({timezone: 'Asia/Ho_Chi_Minh'}) "

    def trans_dict_to_cypher(self, data_dict):
        """Chuyển đổi dictionary sang Cypher query."""
        cypher_query_data = []
        for k, v in data_dict.items():
            if isinstance(v, str):
                if "\\" in v:
                    v = v.replace("\\", "")
                if "'" in v:
                    v = v.replace("'", "\\'")
            cypher_query_data.append("n.{} = \"{}\"".format(k, v))
        return ", ".join(cypher_query_data) + ", " + self.add_updated_timestamp('n')

    def create_node(self, label, data):
        """creat node with label in Neo4j."""
        with self.driver.session() as session:
            _id = data["_id"]
            # del data["_id"]
            sub_query = self.trans_dict_to_cypher(data)
            query_create = """
                            MERGE (n: {}{{_id: "{}"}}) SET {} return n
                        """.format(label, _id, sub_query)
            session.run(query_create)

            # Ghi log sau khi tạo node
            logging.info("CREATE NODE %s WITH ID %s", label, _id)
            
            
            
    def create_realtionship_between_nodes(self, type_relationship, start_node, end_node):
        """creat node with label in Neo4j."""
        with self.driver.session() as session:
            type_relationship = type_relationship.upper()
            query = """
                        MATCH (n1: {} {{_id: '{}' }})
                        MATCH (n2: {} {{_id: '{}' }})
                        MERGE (n1)-[rel:{}]->(n2) SET rel.updated_at = localdatetime({{timezone: 'Asia/Ho_Chi_Minh'}}) RETURN n1, n2
                    """.format(start_node["label"], start_node["_id"], end_node["label"], end_node["_id"], type_relationship)
            session.run(query)
            logging.info("CREATE RELATIONSHIP %s between %s with _id %s and %s with _id %s", type_relationship, start_node["label"], start_node["_id"], end_node["label"], end_node["_id"])

    


if __name__ == "__main__":
    # Khởi tạo driver và test log
    neodriver = Neo4jDriver()
    neodriver.test_log()
    neodriver.close()


import os
import sys
from dotenv import load_dotenv
import json
import re
from neo4j import GraphDatabase, basic_auth
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from database_drivers.neo4j_driver import *
from database_drivers.mongodb_driver import *

class BuildingKG:
    def __init__(self, project_name, timestamp):
        self.project_name = project_name
        self.timestamp= timestamp
        self.mongo_driver = MongoDBDriver(self.project_name)

    def building_defi_kg(self):
        self.mongo_driver.get_all_log_events_and_related_datas(self.timestamp)
        


if __name__ == "__main__":
    model_building = BuildingKG("aave", 1733011200)
    model_building.building_defi_kg()
    
    
    
    
    
    
    pass
    
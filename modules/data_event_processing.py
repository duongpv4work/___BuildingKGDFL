import pymongo
import os
import sys
from dotenv import load_dotenv
import pymongo
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database_drivers.mongodb_driver import *
from database_drivers.neo4j_driver import *
from helpers import *

load_dotenv()


class EventDataProcessing:
    
    def __init__(self) -> None:
        self.neo4j_driver = Neo4jDriver()
    
    def log_event_processing(self, data_log_event):
        
        rs_cp = data_log_event.copy()
        clean_data_log_event = {
            "_id": rs_cp["_id"],
            "block_number": rs_cp["block_number"],
            "amount": rs_cp["amount"],
            "block_timestamp": rs_cp["block_timestamp"],
            "log_index": rs_cp["log_index"],
            # "reserve": rs_cp["reserve"],
            "label": "LOG_EVENT"
        }
        data_smc = {
            "_id": rs_cp["contract_address"],
                "label": "SMART_CONSTRACT",
                "address": rs_cp["contract_address"]
            }
        
        if rs_cp["event_type"] in ["BORROW", "DEPOSIT"]:
            # clean_data_log_event.update({"on_behalf_of": rs_cp["on_behalf_of"]})
            
            
            if rs_cp["wallet"] != rs_cp["user"]:
                data_wallet = {
                    "label": "WALLET",
                    "address": rs_cp["wallet"],
                    "_id": rs_cp["wallet"]
                }
                
                data_user = {
                    "label": "USER",
                    "address": rs_cp["user"],
                    "_id": rs_cp["user"]
                }
            else:
                data_user = {
                    "label": "USER",
                    "address": rs_cp["user"],
                    "_id": rs_cp["user"]
                }
                data_wallet = None
        
        
            return clean_data_log_event, data_smc, data_wallet, data_user
        
        
    def transaction_processing(self, data_transaction, log_index):
        
        rs_cp = data_transaction.copy()
        clean_data_transaction = {
            "_id": rs_cp["_id"],
            "block_number": rs_cp["block_number"],
            "block_hash": rs_cp["block_hash"],
            "block_timestamp": rs_cp["block_timestamp"],
            "gas": rs_cp["gas"],
            "gas_price": rs_cp["gas_price"],
            "transaction_index": rs_cp["transaction_index"],
            "label": "TRANSACTION"
        }
        data_from_address = {
            "_id": rs_cp["from_address"],
                "address": rs_cp["from_address"],
                "label": self.neo4j_driver.get_label_by_address(rs_cp["from_address"])[0]
            }
        data_to_address = {
                "_id": rs_cp["to_address"],
                "address": rs_cp["to_address"],
                "label": self.neo4j_driver.get_label_by_address(rs_cp["to_address"])[0]
        }
        data_log_event_transaction = rs_cp["decoded_input_event"][str(log_index)]
        
        ## process event action DEPOSIT, BORROW
        data_from_address_by_log_trans = {
                    "_id": data_log_event_transaction["from_address"],
                    "label": self.neo4j_driver.get_label_by_address(data_log_event_transaction["from_address"])[0]
                }
        data_relationship = {
            "label": data_log_event_transaction["event_type"],
            "amount": data_log_event_transaction["amount"]
        }
        
        
        data_event_action = {
            "start": data_from_address_by_log_trans,
            "asset_address": data_log_event_transaction["asset_address"],
            "relationship": data_relationship 
        }
        
        
        
        
        return clean_data_transaction, data_from_address, data_to_address, data_event_action
        
       
    
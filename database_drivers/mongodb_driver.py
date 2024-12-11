import pymongo
import os
import sys
from dotenv import load_dotenv
import pymongo
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database_drivers.neo4j_driver import *
from helpers import *
from modules.data_event_processing import *
load_dotenv()


class MongoDBDriver:
    def __init__(self, project_name) -> None:
        
        self.neo4j_driver = Neo4jDriver()
        
        self.project_name = project_name
        self.chainId = ""
        if self.project_name == "aave":
            self.chainId = "0x1"
        self.list_actions = ["DEPOSIT", "BORROW", "LIQUIDATE", "REPAY", "WITHDRAW"]
            
        self.raw_data_mongo = pymongo.MongoClient(os.getenv("RAW_DATA"))["ethereum_blockchain_etl"]
        self.lkg_pr = pymongo.MongoClient(os.getenv("RAW_DATA"))["LendingsPageRank"]
        self.full_lkg = pymongo.MongoClient(os.getenv("RAW_DATA"))["knowledge_graph"]
        
        if os.path.exists("raw_datas/{}/list_smc.txt".format(self.project_name)):
            f = open("raw_datas/{}/list_smc.txt".format(self.project_name), "r")
            self.smc_list = [line.strip() for line  in f.readlines()]
        else:
            self.smc_list = self.get_all_smc_in_pool(self.project_name)
            
        self.model_processing = EventDataProcessing()
    
    
        
    def get_related_block_with_transaction(self, block_hash):
        _id_block_hash = "block_{}".format(block_hash)
        for block in self.raw_data_mongo["blocks"].find({"_id": _id_block_hash}):
            block.update({"_id": block["_id"]})
            return (block)
        return False
    
    def get_realted_trans_to_lending_event(self, transaction_hash):
        _id_trans_hash = "transaction_{}".format(transaction_hash)
        for tran in self.raw_data_mongo["transactions"].find({"_id": _id_trans_hash}):
            tran.update({"_id": tran["_id"]})
            return tran
        return False

    def get_all_smc_in_pool(self, name_pool):
        lst_smc = []
        for smc in self.lkg_pr["smart_contract_label"].find(
                                        {"project": name_pool,
                                          "isSmartContract": True
                                          }):
            with open("raw_datas/{}/list_smc.txt".format(name_pool), "a") as f:
                f.write(smc["address"] + "\n")
                
            lst_smc.append(smc["address"])
        return lst_smc
    
    def get_data_token_by_address(self, address):
        for token in self.full_lkg["smart_contracts"].find({"_id": "{}_{}".format(self.chainId, address)}):
            token.update({"label": "TOKEN", "_id": token["_id"]})
            return token
        return False
    
    
    def get_all_log_events_and_related_datas(self, timestamp):
        query = {
            "contract_address": {"$in": self.smc_list},
            "block_timestamp":  {"$gt": timestamp},
            "event_type": {"$in": self.list_actions}
        }
        list_log_events = self.raw_data_mongo["lending_events"].find(query)
        for log in list_log_events:
            log.update({
                "_id": log["_id"],
                "label": "LOG_EVENT"
            })
            
            # tracking information in each log_event, related transaction and related blocks
            detail_transaction = self.get_realted_trans_to_lending_event(log["transaction_hash"])
            detail_block = self.get_related_block_with_transaction(detail_transaction["block_hash"])
            
            """
                PROCESS DATA OF LOG EVENTS
            """
            
            # create node log_event
            
            clean_data_log_event, data_smc, data_wallet, data_user = self.model_processing.log_event_processing(log)
            
            if log["event_type"] in ["BORROW", "DEPOSIT"]:
                self.neo4j_driver.create_node("LOG_EVENT", clean_data_log_event)
                self.neo4j_driver.create_node("SMART_CONSTRACT", data_smc)
                self.neo4j_driver.create_node("USER", data_user)
                if data_wallet != None:
                    self.neo4j_driver.create_node("WALLET", data_wallet)
                    self.neo4j_driver.create_realtionship_between_nodes("HAS", data_user, data_wallet)
                    self.neo4j_driver.create_realtionship_between_nodes("CALLS", clean_data_log_event, data_wallet)

                
                # create realtionship between nodes
                self.neo4j_driver.create_realtionship_between_nodes("CALLS", clean_data_log_event, data_smc)
                self.neo4j_driver.create_realtionship_between_nodes("CALLS", clean_data_log_event, data_user)
                
            

                # Process Transactions
                
                # update key lable to data
                detail_transaction.update({"label": "TRANSACTION"})
                
                
                clean_data_transaction, data_from_address, data_to_address, data_event_action = self.model_processing.transaction_processing(detail_transaction, log["log_index"])
                self.neo4j_driver.create_node("TRANSACTION", clean_data_transaction)
                self.neo4j_driver.create_realtionship_between_nodes("FROM", clean_data_transaction, data_from_address)
                self.neo4j_driver.create_realtionship_between_nodes("TO", clean_data_transaction, data_to_address)
                
                
                data_token_asset = self.get_data_token_by_address(data_event_action["asset_address"])
                
                self.neo4j_driver.create_event_relationship(data_event_action["relationship"], data_event_action["start"], data_token_asset)
                
                
                # processing data from blocks
                
                detail_block.update({"label": "BLOCK"})
                self.neo4j_driver.create_node("BLOCK", detail_block)
                self.neo4j_driver.create_realtionship_between_nodes("contains", detail_block, clean_data_transaction)
                
                
       
                            

            
            # break
        
        
            
            
            


if __name__ == "__main__":
    mongo_driver = MongoDBDriver("aave")
    print(mongo_driver.get_all_log_events(1733011200))
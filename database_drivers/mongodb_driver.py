import pymongo
import os
import sys
from dotenv import load_dotenv
import pymongo
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database_drivers.neo4j_driver import *
from helpers import *

load_dotenv()


class MongoDBDriver:
    def __init__(self, project_name) -> None:
        
        self.neo4j_driver = Neo4jDriver()
        
        self.project_name = project_name
        self.chainId = ""
        if self.project_name == "aave":
            self.chainId = "0x1"
            
        self.raw_data_mongo = pymongo.MongoClient(os.getenv("RAW_DATA"))["ethereum_blockchain_etl"]
        self.lkg_pr = pymongo.MongoClient(os.getenv("RAW_DATA"))["LendingsPageRank"]
        self.full_lkg = pymongo.MongoClient(os.getenv("RAW_DATA"))["knowledge_graph"]
        
        if os.path.exists("raw_datas/{}/list_smc.txt".format(self.project_name)):
            f = open("raw_datas/{}/list_smc.txt".format(self.project_name), "r")
            self.smc_list = [line.strip() for line  in f.readlines()]
        else:
            self.smc_list = self.get_all_smc_in_pool(self.project_name)
        
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
            "block_timestamp":  {"$gt": timestamp}
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
            
            # for log_event
            """
                clean_data_log_event = {
                    "_id": log["_id"],
                    "amount": log["amount"],
                    "event_type": log["event_type"],
                    "token_amount": log["token_amount"],
                    "type": log["type"]
                }
            """
            
            self.neo4j_driver.create_node("LOG_EVENT", log)
            self.neo4j_driver.create_node("TRANSACTION", detail_transaction)
            self.neo4j_driver.create_node("BLOCK", detail_block)
            
            # update key lable to data
            detail_transaction.update({"label": "TRANSACTION"})
            detail_block.update({"label": "BLOCK"})
            
            # create relationship between log_events and transaction
            self.neo4j_driver.create_realtionship_between_nodes("has_event", detail_transaction, log)
            
            # create relationship between Block and Transaction
            self.neo4j_driver.create_realtionship_between_nodes("contains", detail_block, detail_transaction)
            
            # process related data to transactions
            data_address_transaction_to = {
                "_id": detail_transaction["to_address"],
                "address": detail_transaction["to_address"],
                "label": "ADDRESS"
            }
            self.neo4j_driver.create_node("ADDRESS",data_address_transaction_to)
            self.neo4j_driver.create_realtionship_between_nodes("to", detail_transaction, data_address_transaction_to)
            
            data_address_transaction_from = {
                "_id": detail_transaction["from_address"],
                "address": detail_transaction["from_address"],
                "label": "ADDRESS"
            }
            self.neo4j_driver.create_node("ADDRESS",data_address_transaction_from)
            self.neo4j_driver.create_realtionship_between_nodes("from", detail_transaction, data_address_transaction_from)
            
            self.neo4j_driver.create_realtionship_between_nodes("calls", log, data_address_transaction_to)
            self.neo4j_driver.create_realtionship_between_nodes("calls", log, data_address_transaction_from)
            
            # check if address is TOKEN or SMART_CONTRACT then create node, respectively!
            data_realted_smc = {
                "_id": log["contract_address"],
                "address": log["contract_address"],
                "label": "SMART_CONTRACT"
            }
            self.neo4j_driver.create_node("SMART_CONTRACT", data_realted_smc)
            self.neo4j_driver.create_realtionship_between_nodes("calls", log, data_realted_smc)
            self.neo4j_driver.create_realtionship_between_nodes("to", detail_transaction, data_realted_smc)
            
            if "asset" in log.keys():
                isToken = self.get_data_token_by_address(log["asset"])
                if isToken != False:
                    data_realetd_token = isToken
                    self.neo4j_driver.create_node("TOKEN", data_realetd_token)
                    
                    # create realtionship if node exists
                    self.neo4j_driver.create_realtionship_between_nodes("to", detail_transaction, data_realetd_token)
                    self.neo4j_driver.create_realtionship_between_nodes("calls", log, data_realetd_token)
                            

            
            # break
        
        
            
            
            


if __name__ == "__main__":
    mongo_driver = MongoDBDriver("aave")
    print(mongo_driver.get_all_log_events(1733011200))
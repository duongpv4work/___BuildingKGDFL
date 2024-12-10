
from datetime import datetime

def date_string_to_timestamp(date_str, date_format="%d/%m/%Y %H:%M:%S"):
    date_obj = datetime.strptime(date_str, date_format)
    timestamp = date_obj.timestamp()
    return timestamp

def process_2_list(list_tokens, list_weight):
    rs = dict()
    for index in range(len(list_tokens)):
        rs.update({
            list_tokens[index]: list_weight[index]
        })
    return rs
        
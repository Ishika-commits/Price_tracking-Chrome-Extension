import json
import sys
from datetime import datetime
from mongoengine import connect
from elasticsearch import Elasticsearch, helpers
import logging
from models import AmazonProduct


logging.basicConfig(level=logging.INFO)

connect(db="price_tracker_db", host="mongodb://localhost:27017/price_tracker_db")
# class MongoEngine:

#     def __init__(self, mongo_uri, mongo_db):
#         self.mongo_uri = mongo_uri
#         self.mongo_db = mongo_db
#         self.connect_mongo()

# @classmethod
# def from_crawler(cls, crawler):
#     return cls(
#         mongo_uri=crawler.settings.get("MONGO_URI"),
#         mongo_db=crawler.settings.get("MONGO_DATABASE", "scrapy_db"),
#     ) 
# pass

es = Elasticsearch(['http://localhost:9200'])

def create_index():
    if not es.indices.exists(index='amazon_products'):
        es.indices.create(
            index='amazon_products',
            body={
                'mappings': {
                    'properties': {  
                        'asin': {'type': 'keyword'},
                        'title': {'type': 'text'},
                        'url': {'type': 'text'},
                        'mrp': {'type': 'float'},
                        'price': {'type': 'float'},
                        'current_price': {'type': 'float'},
                        'pincode': {'type': 'keyword'},
                        'seller_name': {'type': 'text'},
                        'timestamp': {'type': 'date'}
                    }
                }
            }
        )
        logging.info("Elasticsearch index 'amazon_products' created.")
    else:
        logging.info("Index already exists. Skipping creation.")

def transfer_amazon_products_data(pincode=False):
    index, doc_type = "amazon", "products"
    try:
        products = list(AmazonProduct.objects())
        
        for product in products:
            print(f"{product.title} - ₹{product.current_price}")


        body_string = ""
        for product in products:
            di = product.to_mongo().to_dict()
            di.pop("_id", None)
            
            st = json.dumps({"index": {"_index": index, "_type": doc_type}})
            
            di["asin"] = product.asin
            di["title"] = product.title
            di["url"] = product.url
            di["mrp"] = product.mrp
            di["price"] = product.price
            di["current_price"] = product.current_price
            di["pincode"] = product.pincode
            di["seller_name"] = product.seller_name
            
            st_value = json.dumps(di)
            single_string = st + "\n" + st_value + "\n"
            body_string += single_string

        es.bulk(body_string)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        err_message = str(exc_obj) + " At line no : " + str(exc_tb.tb_lineno)
        error_report = "Error while pushing Amazon product data: " + err_message
        logging.error(error_report)
        print(err_message)
        raise Exception(err_message)

es = Elasticsearch()

def sense_delete_selected(pincode, index, date_field="timestamp"):
    now = datetime.now()
    today_end = now.replace(hour=0, minute=0, second=0, microsecond=0)

    payload = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"pincode": pincode}},
                    {
                        "range": {
                            date_field: {
                                "gte": now.isoformat(),
                                "lte": today_end.isoformat()
                            }
                        }
                    }
                ]
            }
        }
    }

    result = es.search(index=index, body=payload, size=10000)
    docs = result["hits"]["hits"]

    if not docs:
        logging.info("No matching documents found.")
        return

    actions = [
        {
            "_index": index,
            "_source": doc["_source"]
        }
        for doc in docs
    ]
    helpers.bulk(es, actions)
    print(f"Indexed {len(actions)} documents from {now} to {today_end} with pincode {pincode}")

if __name__ == "__main__":
    create_index()
    transfer_amazon_products_data()

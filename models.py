from mongoengine import Document,  DateTimeField, StringField, FloatField, IntField, connect, BooleanField, ListField, ListField, ListField, EnumField, DictField
from mongoengine import Document,  DateTimeField, StringField, FloatField, IntField, connect, disconnect BooleanField, ListField, ListField, ListField, EnumField, DictField
import getpass
from datetime import datetime
import uuid
import os
from pymongo import ReadPreference


class MongoLog(Document):
    meta = {'db_alias': 'ecommerce_india'}
    timestamp = DateTimeField(default=datetime.now)
    client = StringField()
    insertion_time = IntField()
    source = StringField()  


#  Connect to initial MongoDB database 
MONGO_URI = "mongodb://localhost:27017/scrapy_amazon_db"

connect(host=MONGO_URI)

# Disconnect and reconnect to use a specific alias and target database
disconnect(alias='default')
connect(db='price_tracker_db', alias='default')

#  MongoEngine document to represent an Amazon product
class AmazonProduct(Document):
    asin = StringField(required=True)
    title= StringField(required=True)
    pincode = StringField(required=True)
    url = StringField(required=True)
    price = FloatField()
    mrp = FloatField()
    current_price = FloatField()
    seller = StringField()
    pincode = StringField(required=True)
    scraped_at = DateTimeField(default=datetime.now)
    meta = {'collection': 'sellers', 'alias': 'default'}

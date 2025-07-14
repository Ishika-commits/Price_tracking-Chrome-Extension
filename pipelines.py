from mongoengine import connect, disconnect, Document, StringField, DateTimeField, IntField, ListField, NotUniqueError
from datetime import datetime
from pymongo import ReadPreference
from itemadapter import ItemAdapter
from models import Seller, AmazonProduct
import logging

class MongoEngine:

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.connect_mongo()

@classmethod
def from_crawler(cls, crawler):
    return cls(
        mongo_uri=crawler.settings.get("MONGO_URI"),
        mongo_db=crawler.settings.get("MONGO_DATABASE", "scrapy_db"),
    ) 
pass

# connect(
#     db="price_tracker_db",
#     host="mongodb://localhost:27017/price_tracker_db",
#     alias="price-tracker-alias"
# )

def disconnect_mongo(alias='price-tracker-alias'):
    try:
        disconnect(alias=alias)
        logging.info(f"Disconnected MongoEngine from alias: {alias}")
    except Exception as e:
        logging.error(f"Failed to disconnect MongoEngine: {e}")

class Page(Document):
    meta = {'db_alias': 'user-db-alias'}
    title = StringField(max_length=200, required=True)
    date_modified = DateTimeField(default=datetime.datetime.utcnow)

    meta = {'db_alias': 'price-tracker-alias'}

def save_price_page(title):
    page = Page(title=title)
    page.save()
    print(f"Page saved with title: {page.title} at {page.date_modified}")

class PriceTrackDefaultEmpty(Document):
    price_history = ListField(IntField(), default=list)

class PriceTrackWithPresetValues(Document):
    price_history = ListField(IntField(), default=lambda: [999, 899, 799])

class Page(Document):
    tags = ListField(StringField(max_length=50))

class SaveItemPipeline:
    # def __init__(self, mongo_uri, mongo_db):
    #     self.mongo_uri = mongo_uri
    #     self.mongo_db = mongo_db
    #     self.mongo_engine = None

    def process_item(self, item, spider):
        # adapter = ItemAdapter(item)

        asin = item.get('asin', '').strip()
        title = item.get('title', '').strip()
        url = item.get('url', '').strip()
        mrp = float(item.get('mrp', 0)) if item.get('mrp') else None
        price = float(item.get('price', 0)) if item.get('price') else None
        current_price = float(item.get('current_price', 0)) if item.get('current_price') else None
        pincode = item.get('pincode', '').strip()
        seller_name = item.get('seller', '').strip()

        if not asin or not url:
            spider.logger.warning(f"Dropping item due to missing asin/url: {item}")
            return item

        # try:
        AmazonProduct(**{
            "asin": asin,
            'title': title,
            'url': url,
            'mrp': mrp,
            'price': price,
            'current_price': current_price,
            'pincode': pincode,
            'seller_name': seller_name
        }).save()

            # if not created:
            #     product.update(
            #         set__title=title,
            #         set__url=url,
            #         set__mrp=mrp,
            #         set__price=price,
            #         set__current_price=current_price,
            #         set__pincode=pincode,
            #         set__seller=seller_obj
            #     )
            # logging.info(f"{'Inserted' if created else 'Updated'} product {asin}")

        # except NotUniqueError:
        #     spider.logger.warning(f"Duplicate ASIN: {asin}")

        return item

if __name__ == "__main__":

    save_price_page("Price Track for ASIN B123XYZ")

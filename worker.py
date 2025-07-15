from datetime import datetime
from tracker.tracker.tracker.models import AmazonProduct
from celery import Celery
from helpers.sense_scheduler import dump_data
from tracker.tracker.spiders.price_tracker import AmazonSpider
from  helpers.sense_scheduler import start_spider   
import logging
import sys

celery_app = Celery("tasks", broker="redis://localhost:6379/0")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@celery_app.task()
def run_hourly_worker(pincode,search_query):
    logger.info(f"Spider initialized with pincode: {pincode}, search_query: {search_query}")

    try:
        extra_args = {
            "pincode": pincode,
            "search_query": search_query
        }
        start_spider(
            spider_name="amazon",
            headers={},
            server_ip="",
            extra_args=extra_args
        )

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.error(f"Error in run_hourly_worker(): {exc_obj} at line {exc_tb.tb_lineno}")

@celery_app.task(bind=True)
def run_daily_dump_worker(self):
    logger.info(f"Running Daily Dump Task: {self.name}")
    try:
        products = list(AmazonProduct.objects())
        if not products:
            logger.warning("No products found in MongoDB to dump.")
            return

        data_to_dump = []
        for product in products:
            product = product.to_mongo().to_dict() 
            data_to_dump.append({
                "asin": product.asin,
                "title": product.title,
                "url": product.url,
                "price": product.price,
                "mrp": product.mrp,
                "current_price": product.current_price,
                "pincode": product.pincode,
                "seller_name": product.seller_name,
                "scraped_at": getattr(product, "updated_at", datetime.now())
            })
            

        logger.info(f"Dumping {len(data_to_dump)} records to Elasticsearch.")
        dump_data(data_to_dump)

    except Exception as e:
        exc_obj, exc_tb = sys.exc_info()
        logger.error(f"Error in run_daily_dump_worker(): {exc_obj} at line {exc_tb.tb_lineno}")


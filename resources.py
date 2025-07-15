import falcon
from datetime import datetime, timedelta
from es_client import es, INDEX_NAME


class ProductTrendResource:
    def on_get(self, req, resp):
        pincode = req.get_param("pincode")
        asin = req.get_param("asin")

        if not pincode or not asin:
            resp.status = falcon.HTTP_400
            resp.media = {"error": "Missing required 'pincode' or 'asin' parameter"}
            return

        now = datetime.now()
        start_time = (now - timedelta(days=1))
        end_time = now
        interval_unit = "hour"
        format_str = "%Y-%m-%d %H:%M:%S"

        start_date = start_time.strftime(format_str)
        end_date = end_time.strftime(format_str)

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"pincode": pincode}},
                        {"match": {"asin": asin}},
                        {
                            "range": {
                                "timestamp": {
                                    "gte": start_date,
                                    "lte": end_date
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "time_buckets": {
                    "date_histogram": {
                        "field": "scraped_at",
                        "interval": interval_unit,
                        "min_doc_count": 1
                    },
                    "aggs": {
                        "latest_product": {
                            "top_hits": {
                                "size": 1,
                                "sort": [{"scraped_at": {"order": "desc"}}],
                                "_source": {
                                    "includes": [
                                        "asin", "title", "price", "mrp",
                                        "current_price", "seller", "url", "scraped_at"
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }

        result = es.search(index=INDEX_NAME, doc_type="product", body=query)
        buckets = result.get("aggregations", {}).get("time_buckets", {}).get("buckets", [])

        data = []
        for bucket in buckets:
            hits = bucket["latest_product"]["hits"]["hits"]
            if hits:
                hit = hits[0]["_source"]
                data.append({
                    "timestamp": bucket["key_as_string"],
                    **hit
                })

        data_scraped_range = {
            "start": start_date,
            "end": end_date
        }

        resp.status = falcon.HTTP_200
        resp.media = {
            "success": True,
            "interval": "hourly",
            "data_scraped_range": data_scraped_range,
            "data": data
        }

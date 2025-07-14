import falcon
from resources import ProductTrendResource
from waitress import serve


app = falcon.API()

app.add_route("/product-trend", ProductTrendResource())

from werkzeug.serving import run_simple

if __name__ == '__main__':
    run_simple('127.0.0.1', 8080, app,threaded= True, use_reloader=True)

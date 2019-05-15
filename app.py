# -*- coding: utf-8 -*-

from api import app
from api.sync_mongo_data import *
from api.test_api import *

if __name__ == '__main__':
    app.run(debug=True, port=5000)

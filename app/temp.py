from datetime import datetime, tzinfo, timezone
from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/')
result = client['meterDataArchival']['datewiseConfig'].aggregate([
    {
        '$match': {
            'date': datetime(2023, 1, 25, 0, 0, 0, tzinfo=timezone.utc)
        }
    }, {
        '$lookup': {
            'from': 'masterData', 
            'localField': 'masterDataId', 
            'foreignField': '_id', 
            'as': 'masterData'
        }
    }
])
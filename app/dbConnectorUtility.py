import pymongo

class DBConnectorUtil :
    
    myclient = None
    mydb = None
    mycol = None
    
    def __init__(self, collection = None):
        self.myclient = pymongo.MongoClient("mongodb://localhost:27017/") # Connection.
        self.mydb = self.myclient["meterDataArchival"] # DB Name.
        if(collection is not None) :
            self.mycol = self.mydb[collection]
    
    def getDbObject(self) :
        return self.mydb
    def getCollectionObject(self) :
        return self.mycol

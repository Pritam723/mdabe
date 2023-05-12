import pymongo

class DBConnectorUtil :
    
    myclient = None
    mydb = None
    mycol = None
    
    def __init__(self, collection = None):
        self.myclient = pymongo.MongoClient("mongodb://127.0.0.1:1434/") # Connection.
        self.mydb = self.myclient["meterDataArchival"] # DB Name.
        if(collection is not None) :
            self.mycol = self.mydb[collection]
    def getClient(self) :
        return self.myclient
    def closeClient(self) :
        self.myclient.close()
    def getDbObject(self) :
        return self.mydb
    def getCollectionObject(self) :
        return self.mycol

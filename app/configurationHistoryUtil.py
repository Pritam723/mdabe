from supportingFunctions import getDifferenceDicts, getDifferenceListOfDict
from datetime import datetime, tzinfo, timezone, timedelta
from pymongo import MongoClient
from dbConnectorUtility import DBConnectorUtil
from bson.objectid import ObjectId
import json
from bson import json_util


configInfo = {"masterData" : {"id" : "masterDataId", "data" : "realMeters"}, "fictdatData" : {"id" : "fictdatDataId", "data" : "fictMeters"}, "fictcfgData" : {"id" : "fictcfgDataID", "data" : "fictCFGs"} }


class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime):
            return (str(z))
        else:
            return super().default(z)


def getConfigDataByDate(dateObj, configType) :
    
    ''' Returns the details of the configuration (masterData/ fictdatData/ fictcfgData) for the configType for the dateObj.
    For masterData and fictdatData we get list of Dicts. For fictcfgData we get dict only.'''    
    
    datewiseConfig_collectionObj = DBConnectorUtil(collection = 'datewiseConfig').getCollectionObject()

    resultCursor = datewiseConfig_collectionObj.aggregate([
        {
            '$match': {
                'date': dateObj
            }
        }, {
            '$lookup': {
                'from': configType, 
                'localField': configInfo[configType]["id"], 
                'foreignField': '_id', 
                'as': configType
            }
        }
    ])
    
    result = list(resultCursor)
    
    if(len(result) == 0) : return [] # Because we need to calculate difference of List of Dicts.
    
    else : return result[0][configType][0][configInfo[configType]["data"]]  # result[0][masterData][0]['realMeters']

    
def getConfigDataByID(_id, configType) :
    
    ''' Returns the details of the configuration (masterData/ fictdatData/ fictcfgData) for the configType and _id.
    For masterData and fictdatData we get list of Dicts. For fictcfgData we get dict only.'''    

    if(_id is None) : return []
        
    configData_collectionObj = DBConnectorUtil(collection = configType).getCollectionObject()

    resultCursor = configData_collectionObj.find({'_id' : ObjectId(_id)})
    
    result = list(resultCursor)
    
    if(len(result) == 0) : return [] # Because we need to calculate difference of List of Dicts.
    
    else : return result[0][configInfo[configType]["data"]]


def compareConfigurationData(prevId, currId, configType) :
    
    # print(getConfigDataByID(prevId, "masterData"))
    # print(getConfigDataByID(currId, "masterData"))

    if(configType == "fictcfgData") :
        changeInfoInDictionaryForm = getDifferenceDicts(getConfigDataByID(prevId, configType), getConfigDataByID(currId, configType))
        changes = {'isDifferent': changeInfoInDictionaryForm['isDifferent'],
            'Deleted': [],
            'Added': []
        }

        for key, val in changeInfoInDictionaryForm['Deleted'].items() :
            changes['Deleted'].append({key : val})
        for key, val in changeInfoInDictionaryForm['Added'].items() :
            changes['Added'].append({key : val})


    else :
        changes = getDifferenceListOfDict(getConfigDataByID(prevId, configType), getConfigDataByID(currId, configType))
    
    return json.dumps(changes)


def getConfigDataChangeHistory(configType, selectedMeter, startDateTime, endDateTime) :

    # configInfo = {"masterData" : {"id" : "masterDataId", "data" : "realMeters"}, "fictdatData" : {"id" : "fictdatDataId", "data" : "fictMeters"}, "fictcfgData" : {"id" : "fictcfgDataID", "data" : "fictCFGs"} }

    startDateObj = datetime.strptime(startDateTime, "%d-%m-%Y")
    endtDateObj = datetime.strptime(endDateTime, "%d-%m-%Y")

    # print(f"working for {configType}") working for masterData
    id_to_be_compared = configInfo[configType]["id"]
    data_to_be_compared = configInfo[configType]["data"]
    # print(id_to_be_compared, data_to_be_compared) masterDataId realMeters




    datewiseConfig_collectionObj = DBConnectorUtil(collection = 'datewiseConfig').getCollectionObject()

    nDays = (endtDateObj - startDateObj).days + 1

    changeInfo = []

    i = 0

    previousDateConfig = {'date' : startDateObj, 'masterDataId': None, 'fictdatDataId': None, 'fictcfgDataID': None}

    while(i < nDays) :
    #     print(i)
        currDateObj = startDateObj + timedelta(days = i)

        # print(f"Running for {currDateObj}")

        
        
        datewiseConfigCursor = datewiseConfig_collectionObj.find({'date' : currDateObj})
        datewiseConfig = list(datewiseConfigCursor)

        currentDateConfig = {'date' : currDateObj, 'masterDataId': None, 'fictdatDataId': None, 'fictcfgDataID': None}


        if(len(list(datewiseConfig)) > 0) :
            currentDateConfig = list(datewiseConfig)[0]

        if(currentDateConfig[id_to_be_compared] != previousDateConfig[id_to_be_compared]) :



            if(currDateObj != startDateObj) :
                # print('masterDataId changes on date')
                # print(currDateObj)
                # print(f"So it was same from {previousDateConfig['date']} to {currentDateConfig['date'] - timedelta(days = 1)}")

                # print(f"Previous Configuration was : {previousDateConfig['masterDataId']}")

    #             difference = getDifferenceListOfDict( getMasterDataByID(previousDateConfig['masterDataId']), getMasterDataByID(currentDateConfig['masterDataId']) )

                changeInfo.append({'startDate' : previousDateConfig['date'], 'endDate' : currentDateConfig['date'] - timedelta(days = 1) , 'configDataId' : str(previousDateConfig[id_to_be_compared])} )

            previousDateConfig[id_to_be_compared] = currentDateConfig[id_to_be_compared]
            previousDateConfig['date'] = currentDateConfig['date']


        i = i + 1

    # if(previousDateConfig['date'] != currentDateConfig['date'])

    # print(f"Configuration is same from {previousDateConfig['date']} to {currentDateConfig['date']}")

    # print(f"Configuration is : {previousDateConfig['masterDataId']}")

    changeInfo.append({'startDate' : previousDateConfig['date'], 'endDate' : currentDateConfig['date'], 'configDataId' : str(previousDateConfig[id_to_be_compared])} )

    
    for index, item in enumerate(changeInfo) :
        item['index'] = index
        if(index == 0) :
            item['status'] = f"Configuration for the date {item['startDate'].strftime('%d-%m-%Y')}"
        else :
            item['status'] = f"Configuration was modified on {item['startDate'].strftime('%d-%m-%Y')}"

        if(item['configDataId'] is None) :
            item['dateInfo'] = f"There is no configuration from date {item['startDate'].strftime('%d-%m-%Y')} to {item['endDate'].strftime('%d-%m-%Y')}"
        else :
            item['dateInfo'] = f"Configuration is same from date {item['startDate'].strftime('%d-%m-%Y')} to {item['endDate'].strftime('%d-%m-%Y')}"

    
    # return changeInfo
    return json.dumps(changeInfo, cls=DateTimeEncoder)


# def getConfigurationHistory(configType, selectedMeter, startDateTime, endDateTime):

#     # Parameters look like {'configType': 'masterData', 'selectedMeter': 'Any', 'startDateTime': '03-04-2023', 'endDateTime': '03-04-2023'}

#     startDateObj = datetime.strptime(startDateTime, "%d-%m-%Y")
#     endtDateObj = datetime.strptime(endDateTime, "%d-%m-%Y")
    
#     changeInfo = getConfigDataChangeHistory(configType, selectedMeter, startDateTime, endDateTime)
#     return json.dumps(changeInfo, cls=DateTimeEncoder)
#     # return json.loads(json_util.dumps(changeInfo))


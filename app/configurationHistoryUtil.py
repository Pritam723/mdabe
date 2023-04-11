from supportingFunctions import getDifferenceDicts, getDifferenceListOfDict
from datetime import datetime, tzinfo, timezone, timedelta
from pymongo import MongoClient
from dbConnectorUtility import DBConnectorUtil
from bson.objectid import ObjectId
import json
from bson import json_util


configInfo = {"masterData" : {"name" : "MASTER.DAT", "id" : "masterDataId", "data" : "realMeters"}, "fictdatData" : {"name" : "FICTMTRS.DAT", "id" : "fictdatDataId", "data" : "fictMeters"}, "fictcfgData" : {"name" : "FICTMTRS.CFG", "id" : "fictcfgDataID", "data" : "fictCFGs"} }


class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime):
            return (str(z))
        else:
            return super().default(z)


def isParticularMeterDifferentInConfigurations(prevId, currId, configType, selectedMeter) :
    
    '''Given 2 configIDs of some configType, it says whether there is any occurance of selectedMeter in the Changes.
    This function helps while we try to see chnages of a Particular Selected Meter. Even if there is change detected in 2
    configurations, it may happen that the selectedMeter is not changed in those configurations.'''

    if(configType == "fictcfgData") :
        print("Checking fictcfgData for " + selectedMeter)
        changeInfoInDictionaryForm = getDifferenceDicts(getConfigDataByID(prevId, configType), getConfigDataByID(currId, configType))
        
        # print("Printing for Prev ID " + str(prevId))
        # print(getConfigDataByID(prevId, configType))

        # print("Printing for Curr ID " + str(currId))
        # print(getConfigDataByID(currId, configType))
        
        # print(changeInfoInDictionaryForm)

        if(selectedMeter in changeInfoInDictionaryForm['Deleted'] or f"({selectedMeter})" in changeInfoInDictionaryForm['Deleted']) :
            return True
        if(selectedMeter in changeInfoInDictionaryForm['Added'] or f"({selectedMeter})" in changeInfoInDictionaryForm['Added']) :
            return True
        return False
        
    else :
        changes = getDifferenceListOfDict(getConfigDataByID(prevId, configType), getConfigDataByID(currId, configType))

        #print(changes)        
        
        for item in changes['Deleted'] :
            if(item['Loc_Id'] == selectedMeter) :
                print("There is mention of " + selectedMeter + " in getDifferenceListOfDict deleted for " + str(prevId) + " and " + str(currId))
                return True
        

        for item in changes['Added'] :
            if(item['Loc_Id'] == selectedMeter) :
                print("There is mention of " + selectedMeter + " in getDifferenceListOfDict added for " + str(prevId) + " and " + str(currId))
                return True
        
        print("There is no mention of " + selectedMeter + " in getDifferenceListOfDict for " + str(prevId) + " and " + str(currId))

        return False



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

    emptyConfig = None
    if(configType == "fictcfgData") :
        emptyConfig = {} # Because we need to calculate difference of Dicts.
    else :
        emptyConfig = [] # Because we need to calculate difference of List of Dicts.

    if(_id is None or _id == "None") : return emptyConfig # Because from the JS Frontend we may receive None as string.
        
    configData_collectionObj = DBConnectorUtil(collection = configType).getCollectionObject()

    resultCursor = configData_collectionObj.find({'_id' : ObjectId(_id)})
    
    result = list(resultCursor)
    
    if(len(result) == 0) : return emptyConfig
    
    else : return result[0][configInfo[configType]["data"]]


def compareConfigurationDataAllMeter(prevId, currId, configType) :
    
    print(f"comparing {prevId} vs {currId} for {configType}")
    print("Called compareConfigurationDataAllMeter")

    # print(getConfigDataByID(prevId, "masterData"))
    # print(getConfigDataByID(currId, "masterData"))

    if(configType == "fictcfgData") :
        changeInfoInDictionaryForm = getDifferenceDicts(getConfigDataByID(prevId, configType), getConfigDataByID(currId, configType))
        # print(changeInfoInDictionaryForm)        
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
        print(changes)
    return json.dumps(changes)


def compareConfigurationDataSelectedMeter(prevId, currId, configType, selectedMeter) :
    
    print(f"comparing {prevId} vs {currId} for {configType}")
    print("Called compareConfigurationDataSelectedMeter for " + selectedMeter)

    # print(getConfigDataByID(prevId, "masterData"))
    # print(getConfigDataByID(currId, "masterData"))

    if(configType == "fictcfgData") :
        changeInfoInDictionaryForm = getDifferenceDicts(getConfigDataByID(prevId, configType), getConfigDataByID(currId, configType))
        # print(changeInfoInDictionaryForm)

        changesSelectedMeter = {'isDifferent' : changeInfoInDictionaryForm['isDifferent'], 'Deleted' : [], 'Added' : []}

        if(changeInfoInDictionaryForm['Deleted'].get(selectedMeter) is not None) :
            changesSelectedMeter['Deleted'].append({selectedMeter : changeInfoInDictionaryForm['Deleted'][selectedMeter]})
            
        if(changeInfoInDictionaryForm['Deleted'].get('(' + selectedMeter + ')') is not None) :
            changesSelectedMeter['Deleted'].append({'(' + selectedMeter + ')' : changeInfoInDictionaryForm['Deleted']['(' + selectedMeter + ')']})
            
        if(changeInfoInDictionaryForm['Added'].get(selectedMeter) is not None) :
            changesSelectedMeter['Added'].append({selectedMeter : changeInfoInDictionaryForm['Added'][selectedMeter]})
            
            
        if(changeInfoInDictionaryForm['Added'].get('(' + selectedMeter + ')') is not None) :
            changesSelectedMeter['Added'].append({'(' + selectedMeter + ')' : changeInfoInDictionaryForm['Added']['(' + selectedMeter + ')']})

    else :
        changes = getDifferenceListOfDict(getConfigDataByID(prevId, configType), getConfigDataByID(currId, configType))
        # print(changes)

        changesSelectedMeter = {'isDifferent' : changes['isDifferent'], 'Deleted' : [], 'Added' : []}

        changesSelectedMeter['Deleted'] = [item for item in changes['Deleted'] if item['Loc_Id'] == selectedMeter]

        changesSelectedMeter['Added'] = [item for item in changes['Added'] if item['Loc_Id'] == selectedMeter]
    
    return json.dumps(changesSelectedMeter)


def getConfigDataChangeHistoryForAllMeters(configType, startDateTime, endDateTime) :

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
            item['status'] = f"{configInfo[configType]['name']} for the date {item['startDate'].strftime('%d-%m-%Y')}"
        else :
            item['status'] = f"{configInfo[configType]['name']} was modified on {item['startDate'].strftime('%d-%m-%Y')}"

        if(item['configDataId'] is None) :
            item['dateInfo'] = f"There is no {configInfo[configType]['name']} from date {item['startDate'].strftime('%d-%m-%Y')} to {item['endDate'].strftime('%d-%m-%Y')}"
        else :
            item['dateInfo'] = f"{configInfo[configType]['name']} is same from date {item['startDate'].strftime('%d-%m-%Y')} to {item['endDate'].strftime('%d-%m-%Y')}"

    
    # return changeInfo
    return json.dumps(changeInfo, cls=DateTimeEncoder)



def getConfigDataChangeHistoryForSelectedMeter(configType, selectedMeter ,startDateTime, endDateTime) :

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
    minimizeCheckingConfig = {'date' : startDateObj, 'masterDataId': None, 'fictdatDataId': None, 'fictcfgDataID': None}
    # 641af7adf855eecba645c6fd, 641af7d8f855eecba6463615

    while(i < nDays) :
    #     print(i)
        currDateObj = startDateObj + timedelta(days = i)

        # print(f"Running for {currDateObj}")

        
        
        datewiseConfigCursor = datewiseConfig_collectionObj.find({'date' : currDateObj})
        datewiseConfig = list(datewiseConfigCursor)

        currentDateConfig = {'date' : currDateObj, 'masterDataId': None, 'fictdatDataId': None, 'fictcfgDataID': None}


        if(len(list(datewiseConfig)) > 0) :
            currentDateConfig = list(datewiseConfig)[0]

        if(currentDateConfig[id_to_be_compared] != previousDateConfig[id_to_be_compared] and currentDateConfig[id_to_be_compared] != minimizeCheckingConfig[id_to_be_compared]) :
            # But there is still possibility that selectedMeter does not change here actually.
            # For a particular meter we need to check whether 'isParticularMeterDifferentInConfigurations' too.
            print("I got a different config")
            print("previous config ID is " + str(previousDateConfig[id_to_be_compared]))
            print("current config ID is " + str(currentDateConfig[id_to_be_compared]))

            print("Checking whether " + selectedMeter + " was changed in these 2 configs")
            # print(isParticularMeterDifferentInConfigurations(previousDateConfig[id_to_be_compared], currentDateConfig[id_to_be_compared],configType, selectedMeter))

            if(not isParticularMeterDifferentInConfigurations(previousDateConfig[id_to_be_compared], currentDateConfig[id_to_be_compared],configType, selectedMeter)) :
                # print("So although previous and current date Config is different, in the current config the selectedMeter is not changed.")
                # print("So One thing I can do is, keep track of this currentConfig too and chreck with next currentConfig. This will minimize checking isParticularMeterDifferentInConfigurations.")

                minimizeCheckingConfig[id_to_be_compared] = currentDateConfig[id_to_be_compared]
                minimizeCheckingConfig['date'] = currentDateConfig['date']

                i = i + 1
                continue
            
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

    # For the Last Date Entry
    changeInfo.append({'startDate' : previousDateConfig['date'], 'endDate' : currentDateConfig['date'], 'configDataId' : str(previousDateConfig[id_to_be_compared])} )

    
    for index, item in enumerate(changeInfo) :
        item['index'] = index
        if(index == 0) :
            item['status'] = f"Configuration for {selectedMeter} for the date {item['startDate'].strftime('%d-%m-%Y')}"
        else :
            item['status'] = f"Configuration for {selectedMeter} was modified on {item['startDate'].strftime('%d-%m-%Y')}"

        if(item['configDataId'] is None) :
            item['dateInfo'] = f"There is no Configuration for {selectedMeter} from date {item['startDate'].strftime('%d-%m-%Y')} to {item['endDate'].strftime('%d-%m-%Y')}"
        else :
            item['dateInfo'] = f"Configuration for {selectedMeter} is same from date {item['startDate'].strftime('%d-%m-%Y')} to {item['endDate'].strftime('%d-%m-%Y')}"

    
    # return changeInfo
    return json.dumps(changeInfo, cls=DateTimeEncoder)



def getConfigDataChangeHistory(configType, selectedMeter, startDateTime, endDateTime) :
    if(selectedMeter == "Any") :
        print("Called for ALL Meters")
        return getConfigDataChangeHistoryForAllMeters(configType,startDateTime,endDateTime)
    else :
        print("Called for particular meter " + selectedMeter)
        return getConfigDataChangeHistoryForSelectedMeter(configType,selectedMeter,startDateTime,endDateTime)

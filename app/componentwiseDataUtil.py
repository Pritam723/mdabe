import pandas as pd
from flask import send_file
from meterComponents import separateComponents, separateComponentsTillLastMeter
from configurationHistoryUtil import getConfigDataChangeHistory
from fetchMeterDataByParam import fetchMeterDataByParam
from dbConnectorUtility import DBConnectorUtil

configInfo = {"masterData" : {"name" : "MASTER.DAT", "id" : "masterDataId", "data" : "realMeters"}, "fictdatData" : {"name" : "FICTMTRS.DAT", "id" : "fictdatDataId", "data" : "fictMeters"}, "fictcfgData" : {"name" : "FICTMTRS.CFG", "id" : "fictcfgDataID", "data" : "fictCFGs"} }
componenetSeparatorFunction = {
    'Level-1' : separateComponents,
    'Recursive' : separateComponentsTillLastMeter
}
def componentWiseData(configType, selectedMeter, multiplierData, startDateTime, endDateTime, componentType, fetchBy, excelOnly = False):


    ###################### Initializing the DB Connectors. Make sure to close them before returning. ############################

    allTimeRealMeterIDs_DBConnectorObj = DBConnectorUtil(collection = 'allTimeRealMeterIDs')
    allTimeFictMeterIDs_DBConnectorObj = DBConnectorUtil(collection = 'allTimeFictMeterIDs')

    allTimeRealMeterIDs_collectionObj = allTimeRealMeterIDs_DBConnectorObj.getCollectionObject()
    allTimeFictMeterIDs_collectionObj = allTimeFictMeterIDs_DBConnectorObj.getCollectionObject()

    configData_DBConnectorObj = DBConnectorUtil(collection = "fictcfgData")
    configData_collectionObj = configData_DBConnectorObj.getCollectionObject()

    ##############################################################################################################################



    configDataChangeHistory = getConfigDataChangeHistory(configType, selectedMeter, startDateTime, endDateTime)

    # unionAllMeters = set({})

    yAxisData = {}
    xAxisData = []
    dataAddedForDaysTillNow = 0 # Indicates how many days of data is added in yAxisData Till now.

    for item in configDataChangeHistory :

        startDateTimeObj = item['startDate']
        endDateTimeObj = item['endDate']
        no_of_days = (endDateTimeObj - startDateTimeObj).days + 1
        endDateTimeObj = endDateTimeObj.replace(hour=23, minute=45, second = 0, microsecond = 0)

        # print(startDateTimeObj.strftime("%d-%m-%Y %H:%M:%S") + " to " + endDateTimeObj.strftime("%d-%m-%Y %H:%M:%S") + " configID is " + str(item['configDataId']))
        # print(f"No of days : {no_of_days}")
        # print("###################################################################################")
        # print("Printing the separate components and relevant data for that range")
        componentsForThisInterval = componenetSeparatorFunction[componentType](selectedMeter, item['configDataId'], allTimeRealMeterIDs_collectionObj, allTimeFictMeterIDs_collectionObj, configData_collectionObj)['components']
        componentsForThisIntervalToPass = [{'name' : item, 'code' : item} for item in componentsForThisInterval]

        # print(componentsForThisInterval)

        for meterID in componentsForThisInterval :
            if meterID not in yAxisData :
                yAxisData[meterID] = [None]*dataAddedForDaysTillNow*96
        
        # Now our yAxisData is ready with all updated/new meterIDs (If any). Compare it with received data now.

        receivedData = fetchMeterDataByParam(startDateTimeObj.strftime("%d-%m-%Y %H:%M:%S"), endDateTimeObj.strftime("%d-%m-%Y %H:%M:%S"), componentsForThisIntervalToPass, multiplierData, fetchBy = fetchBy, excelOnly = False)
        # The return object looks like {statrDateTime : datetimeObj, endDateTime : datetimeObj, xAxisData : [288 items separated by 15 minutes],
        # yAxisDataForAllMeters : {'FK-01' : [288 Data Points], 'FK-02' : [288 Data Points]}}
        # print(receivedData)
        received_xAxisData = receivedData['xAxisData']
        received_yAxisData = receivedData['yAxisDataForAllMeters']

        for key in yAxisData :
            if key in received_yAxisData :
                yAxisData[key].extend(received_yAxisData[key])
            else : # So, this key must have been omitted for this configuration interval.
                yAxisData[key].extend([None]*no_of_days*96)

        # For xAxisData, we do not need to worry.
        xAxisData.extend(received_xAxisData)
        
        dataAddedForDaysTillNow = dataAddedForDaysTillNow + no_of_days


        # unionAllMeters = unionAllMeters.union(componentsForThisInterval)
        # print("###################################################################################")

    # print(list(unionAllMeters))

    ############################################## Closing the DB Connectors. ####################################################


    allTimeRealMeterIDs_DBConnectorObj.closeClient()
    allTimeFictMeterIDs_DBConnectorObj.closeClient()
    configData_DBConnectorObj.closeClient()



    ##############################################################################################################################


    if(excelOnly == True) :
        df = pd.DataFrame(yAxisData, index = xAxisData)
        df.to_excel('ReportGeneration/ComponentData.xlsx', sheet_name = "Componentwise Data")

        return send_file('ReportGeneration/ComponentData.xlsx', as_attachment=True, download_name="ComponentData.xlsx")

    dataToReturn = { 'startDateTime' : startDateTime, 'endDateTime' : endDateTime, 'xAxisData' : xAxisData, 'yAxisDataForAllMeters' : yAxisData}


    return dataToReturn

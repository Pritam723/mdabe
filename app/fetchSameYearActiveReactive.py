import re
import os
import pandas as pd
import pymongo
from datetime import datetime, timedelta
from supportingFunctions import *
from dbConnectorUtility import DBConnectorUtil
import pandas as pd

def fetchSameYearActiveReactive(year, startDateTime, endDateTime, selectedMeters, fetchBy, xAxisData, yAxisDataForAllMeters, energyType = "activeHigh"):
    
    # myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    # mydb = myclient["meterDataArchival"]
    # mycol = mydb["meterData2023"]
    

    DBConnector = DBConnectorUtil(collection = f'meterData{year}') # Depends on the year. As the Collections are separated wrt Year.

    collectionObj = DBConnector.getCollectionObject()

    meterList = selectedMeters

    # print(startDateTime, endDateTime)
    # In format '13-02-2023 04:30:00' for 4:30AM February 13th 2023.

    startDateTime_DateComponent = startDateTime.split()[0]
    # We do not require rest 2 infos.
    # startDateTime_TimeComponent = startDateTime.split()[1]
    # startDateTimeBlock = getBlockFromTimeStamp(startDateTime_TimeComponent)

    endDate_DateComponent = endDateTime.split()[0]
    # We do not require rest 2 infos.
    # endDate_TimeComponent = endDateTime.split()[1]
    # endDateTimeBlock = getBlockFromTimeStamp(endDate_TimeComponent)
    
    # dateObj = datetime.strptime('13-02-2023 04-30-00','%d-%m-%Y %H:%M:%S')

    startDateObj = datetime.strptime(startDateTime_DateComponent,'%d-%m-%Y')
    endDateObj = datetime.strptime(endDate_DateComponent,'%d-%m-%Y')

    startDateTimeObj = datetime.strptime(startDateTime,'%d-%m-%Y %H:%M:%S')
    endDateTimeObj = datetime.strptime(endDateTime,'%d-%m-%Y %H:%M:%S')

    # print(startDateObj, endDateObj)
    no_of_days = (endDateObj - startDateObj).days + 1

    # xAxisData = [dt for dt in datetime_range(startDateObj, endDateObj, 15)]

    # startDateObj = datetime(2023, 2, 18, hour, minute, 0)
    # endDateObj = datetime(2023, 2, 20, hour-1, minute-15, 0)

    xAxisData =  xAxisData.extend([dt for dt in date_range(startDateObj, endDateObj, offSetDays = 1)])

    
    # yAxisDataForAllMeters = {} Do not Initialize, use the Reference Value.

    for meterInfo in meterList :
        meter = meterInfo['name']
        
        meterData = list(collectionObj.find( {fetchBy: meter, 'date': {'$lte': endDateObj, '$gte': startDateObj} }, {'_id' : 0, 'data' : 0})) # We do not need MWH data.
        # Remerber that meterData is sorted by meterID/ meterNO and then by Date. Because we have used compound index in our DB.
        # del meterData[3] # For testing Purpose Only.
        # del meterData[4] # For testing Purpose Only.
        
        meterDataLength = len(meterData)
        
        if(meterDataLength == 0) : # So there is no data for this meter. Complete Data should be Null 
            yAxisDataForAllMeters[meter].extend([None]*no_of_days)
            continue

        yAxisData = []
        countDays = 0
        dataIndex = 0

        while(countDays < no_of_days) :
            currDate = startDateObj + timedelta(days = countDays)

            # Handle startDateTime & endDateTime differently.
            # Not required here.
            # startBlock = 0
            # endBlock = 96
            
            # if(currDate == startDateObj):
            #     startBlock = startDateTimeBlock
            # if(currDate == endDateObj):
            #     endBlock = endDateTimeBlock

            if(dataIndex >= meterDataLength):
                #It means that we are at a date, data for which is not available. And the date is beyond the last available data date.
                yAxisData.append(None)
                countDays = countDays + 1
                continue

            if(currDate == meterData[dataIndex]['date']) :
                yAxisData.append(meterData[dataIndex][energyType])
                dataIndex = dataIndex + 1
            else :
                yAxisData.append(None)

            countDays = countDays + 1
        # print("Before Merge")   
        # print(yAxisData)
        yAxisDataForAllMeters[meter].extend(yAxisData)

    # dataToReturn = {'xAxisData' : xAxisData, 'yAxisDataForAllMeters' : yAxisDataForAllMeters}

    # return dataToReturn

    # No point of returning anything, as we have passed xAxisData and yAxisDataForAllMeters by Reference here. And we are modifying them here.
    # Same will be reflected in calling function too.



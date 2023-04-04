import re
import os
import pandas as pd
import pymongo
from pymongo import UpdateOne
from datetime import datetime, timedelta
from supportingFunctions import *
from dbConnectorUtility import DBConnectorUtil
import json
from flask import send_file
import pandas as pd

class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime):
            return (str(z))
        else:
            return super().default(z)

def fetchMeterDataByParam(startDateTime, endDateTime, selectedMeters, fetchBy, excelOnly = False):
    
    # myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    # mydb = myclient["meterDataArchival"]
    # mycol = mydb["meterData2023"]
    
    DBConnector = DBConnectorUtil(collection = 'meterData2018')

    collectionObj = DBConnector.getCollectionObject()

    meterList = selectedMeters

    print(startDateTime, endDateTime)
    # In format '13-02-2023 04:30:00' for 4:30AM February 13th 2023.

    startDateTime_DateComponent = startDateTime.split()[0]
    startDateTime_TimeComponent = startDateTime.split()[1]
    startDateTimeBlock = getBlockFromTimeStamp(startDateTime_TimeComponent)

    endDate_DateComponent = endDateTime.split()[0]
    endDate_TimeComponent = endDateTime.split()[1]
    endDateTimeBlock = getBlockFromTimeStamp(endDate_TimeComponent)
    
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

    xAxisData = [dt for dt in datetime_range(startDateTimeObj, endDateTimeObj, 15)]

    
    yAxisDataForAllMeters = {}

    for meterInfo in meterList :
        meter = meterInfo['name']
        
        meterData = list(collectionObj.find( {fetchBy: meter, 'date': {'$lte': endDateObj, '$gte': startDateObj} }, {'_id' : 0}))
        # Remerber that meterData is sorted by meterID/ meterNO and then by Date. Because we have used compound index in our DB.
        # del meterData[3] # For testing Purpose Only.
        # del meterData[4] # For testing Purpose Only.
        
        meterDataLength = len(meterData)
        
        if(meterDataLength == 0) : # So there is no data for this meter. Complete Data should be Null 
            yAxisDataForAllMeters[meter] = [None]*96*no_of_days
            continue

        yAxisData = []
        countDays = 0
        dataIndex = 0

        while(countDays < no_of_days) :
            currDate = startDateObj + timedelta(days = countDays)

            # Handle startDateTime & endDateTime differently.
            startBlock = 0
            endBlock = 96
            
            if(currDate == startDateObj):
                startBlock = startDateTimeBlock
            if(currDate == endDateObj):
                endBlock = endDateTimeBlock

            if(dataIndex >= meterDataLength):
                #It means that we are at a date, data for which is not available. And the date is beyond the last available data date.
                yAxisData = yAxisData + ([None]*96)[startBlock : endBlock + 1]
                countDays = countDays + 1
                continue

            if(currDate == meterData[dataIndex]['date']) :
                yAxisData = yAxisData + (meterData[dataIndex]['data'])[startBlock : endBlock + 1]
                dataIndex = dataIndex + 1
            else :
                yAxisData = yAxisData + ([None]*96)[startBlock : endBlock + 1]

            countDays = countDays + 1
        
        yAxisDataForAllMeters[meter] = yAxisData
    
    if(excelOnly == True) :
        df = pd.DataFrame(yAxisDataForAllMeters, index = xAxisData)
        df.to_excel('ReportGeneration/MeterData.xlsx')
        
        return send_file('ReportGeneration/MeterData.xlsx', as_attachment=True, download_name="MeterData.xlsx")

    dataToReturn = {'startDateTime' : startDateTimeObj, 'endDateTime' : endDateTimeObj, 'xAxisData' : xAxisData, 'yAxisDataForAllMeters' : yAxisDataForAllMeters}

    return json.dumps(dataToReturn, cls=DateTimeEncoder)



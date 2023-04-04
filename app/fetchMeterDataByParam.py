from datetime import datetime, timedelta
from supportingFunctions import *
import json
from flask import send_file
import pandas as pd
from fetchSameYearMeterData import fetchSameYearMeterData

class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime):
            return (str(z))
        else:
            return super().default(z)

def fetchMeterDataByParam(startDateTime, endDateTime, selectedMeters, fetchBy, excelOnly = False):
    
    startDateTimeObj = datetime.strptime(startDateTime,'%d-%m-%Y %H:%M:%S') # Used during Return Only.
    endDateTimeObj = datetime.strptime(endDateTime,'%d-%m-%Y %H:%M:%S')
    # See, whatever the startDateTime and endDateTime is, out fetchSameYearMeterData() can only work on one meterDataCollection at a time.
    # So, first we must generateYearWiseDateIntervals(). We get data in format '13-02-2023 04:30:00' for 4:30AM February 13th 2023. fetchSameYearMeterData() also
    # expects same format. generateYearWiseDateIntervals() handles all such i/p o/p format.

    dateIntervals = generateYearWiseDateIntervals(startDateTime, endDateTime)
    # [{'startDateTime': '18-06-2021 10:45:00',
    #   'endDateTime': '31-12-2021 23:45:00'},
    #  {'startDateTime': '01-01-2022 00:00:00',
    #   'endDateTime': '31-12-2022 23:45:00'},
    #  {'startDateTime': '01-01-2023 00:00:00',
    #   'endDateTime': '20-02-2023 07:15:00'}]

    meterList = selectedMeters
    xAxisData = []
    yAxisDataForAllMeters = {} # We will pass these 2 as reference in fetchSameYearMeterData() function.


    # Initialization
    for meterInfo in meterList :
        meter = meterInfo['name']
        yAxisDataForAllMeters[meter] = []

    for dateInterval in dateIntervals :
        # Call fetchSameYearMeterData for each Interval. The signature looks like,
        # fetchSameYearMeterData(year, startDateTime, endDateTime, selectedMeters, fetchBy, xAxisData, yAxisDataForAllMeters)
        # So, also pass the year.
        currentYear = datetime.strptime(dateInterval['startDateTime'], '%d-%m-%Y %H:%M:%S').year
        # dataForInterval =  fetchSameYearMeterData(currentYear, dateInterval['startDateTime'], dateInterval['endDateTime'], meterList, fetchBy, xAxisData, yAxisDataForAllMeters)

        fetchSameYearMeterData(currentYear, dateInterval['startDateTime'], dateInterval['endDateTime'], meterList, fetchBy, xAxisData, yAxisDataForAllMeters)
        # See, only calling it is enough as Lists and Dictionaries are mutable in Python.
        # print(xAxisData)

        # List and Dicts are Mutable, so not a problem.

        # We get back data in following format.
        # dataToReturn = {'xAxisData' : xAxisData, 'yAxisDataForAllMeters' : yAxisDataForAllMeters}
        # Append the data with Existing.

        # xAxisData = xAxisData + dataForInterval['xAxisData']

        # Now for all Meter Datas do the same.

        # for meterInfo in meterList :
        #     meter = meterInfo['name']
        #     yAxisDataForAllMeters[meter] = yAxisDataForAllMeters[meter] + dataForInterval['yAxisDataForAllMeters'][meter]     


    # So, now our xAxisData and yAxisDataForAllMeters for all dates are ready. Build the data to be returned here.

    if(excelOnly == True) :
        df = pd.DataFrame(yAxisDataForAllMeters, index = xAxisData)
        df.to_excel('ReportGeneration/MeterData.xlsx')
        
        return send_file('ReportGeneration/MeterData.xlsx', as_attachment=True, download_name="MeterData.xlsx")

    dataToReturn = {'startDateTime' : startDateTimeObj, 'endDateTime' : endDateTimeObj, 'xAxisData' : xAxisData, 'yAxisDataForAllMeters' : yAxisDataForAllMeters}

    return json.dumps(dataToReturn, cls=DateTimeEncoder)



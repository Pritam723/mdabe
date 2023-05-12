from datetime import datetime, timedelta
from supportingFunctions import *
import json
from flask import send_file
import pandas as pd
from fetchSameYearActiveReactive import fetchSameYearActiveReactive

class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime):
            return (str(z))
        else:
            return super().default(z)

def fetchActiveReactiveByParam(startDateTime, endDateTime, selectedMeters, multiplierData, fetchBy, energyType = "activeHigh", excelOnly = False):
    
    startDateTimeObj = datetime.strptime(startDateTime,'%d-%m-%Y %H:%M:%S') # Used during Return Only.
    endDateTimeObj = datetime.strptime(endDateTime,'%d-%m-%Y %H:%M:%S')

    # Note that ActiveReactive Data is same for one day. So, no need to log HH:MM:SS data. Keep it 00:00:00 for startDate and 23:45:00 for endDate

    # See, whatever the startDateTime and endDateTime is, out fetchSameYearActiveReactive() can only work on one meterDataCollection at a time.
    # So, first we must generateYearWiseDateIntervals(). We get data in format '13-02-2023 00:00:00' for 00:00AM February 13th 2023. fetchSameYearActiveReactive() also
    # expects same format. generateYearWiseDateIntervals() handles all such i/p o/p format.

    dateIntervals = generateYearWiseDateIntervals(startDateTime, endDateTime)
    # [{'startDateTime': '18-06-2021 00:00:00',
    #   'endDateTime': '31-12-2021 23:45:00'},
    #  {'startDateTime': '01-01-2022 00:00:00',
    #   'endDateTime': '31-12-2022 23:45:00'},
    #  {'startDateTime': '01-01-2023 00:00:00',
    #   'endDateTime': '20-02-2023 23:45:00'}]

    meterList = selectedMeters
    xAxisData = []
    yAxisDataForAllMeters = {} # We will pass these 2 as reference in fetchSameYearActiveReactive() function.


    # Initialization
    for meterInfo in meterList :
        meter = meterInfo['name']
        yAxisDataForAllMeters[meter] = []

    for dateInterval in dateIntervals :
        # Call fetchSameYearActiveReactive for each Interval. The signature looks like,
        # fetchSameYearActiveReactive(year, startDateTime, endDateTime, selectedMeters, fetchBy, xAxisData, yAxisDataForAllMeters)
        # So, also pass the year.
        currentYear = datetime.strptime(dateInterval['startDateTime'], '%d-%m-%Y %H:%M:%S').year
        # dataForInterval =  fetchSameYearActiveReactive(currentYear, dateInterval['startDateTime'], dateInterval['endDateTime'], meterList, fetchBy, xAxisData, yAxisDataForAllMeters)

        fetchSameYearActiveReactive(currentYear, dateInterval['startDateTime'], dateInterval['endDateTime'], meterList, multiplierData, fetchBy, xAxisData, yAxisDataForAllMeters, energyType)
        # See, only calling it is enough as Lists and Dictionaries are mutable in Python.
        # print(xAxisData)

        # Lists and Dicts are Mutable, so not a problem. 


    # So, now our xAxisData and yAxisDataForAllMeters for all dates are ready. Build the data to be returned here.

    if(excelOnly == True) :
        df = pd.DataFrame(yAxisDataForAllMeters, index = xAxisData)
        df.to_excel('ReportGeneration/EnergyData.xlsx', sheet_name = energyType)
        
        return send_file('ReportGeneration/EnergyData.xlsx', as_attachment=True, download_name="EnergyData.xlsx")

    dataToReturn = {'startDateTime' : startDateTimeObj, 'endDateTime' : endDateTimeObj, 'xAxisData' : xAxisData, 'yAxisDataForAllMeters' : yAxisDataForAllMeters}

    return dataToReturn



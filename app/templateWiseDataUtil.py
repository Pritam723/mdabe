
import pandas as pd
import re
from reportGeneration import getEquationValue
from fetchMeterDataByParam import fetchMeterDataByParam
from datetime import datetime
from supportingFunctions import *
from flask import send_file

def templateWiseData(selectedTemplate, startDateTime, endDateTime, excelOnly = False):
    # print(selectedTemplate)
    # print(startDateTime)
    # print(endDateTime)


    startDateTimeObj = datetime.strptime(startDateTime, "%d-%m-%Y")
    endDateTimeObj = datetime.strptime(endDateTime, "%d-%m-%Y")
    no_of_days = (endDateTimeObj - startDateTimeObj).days + 1

    endDateTimeObj = endDateTimeObj.replace(hour=23, minute=45, second = 0, microsecond = 0)


    meterIdPattern = re.compile(r'[A-Z]{2}-[0-9]{2}')

    data = pd.read_excel('ConfigurationFiles/TemplatesConfiguration.xlsx', sheet_name=selectedTemplate)
    yAxisTemplateData = {}
    xAxisData = []

    for index, row in data.iterrows():
        stat = row['Statistics']
        eq = row['Equation']

        if(isFloat(eq)) :
            yAxisTemplateData[f'{stat}'] =  [float(eq)] * 96 * no_of_days
            continue

        components = re.findall(meterIdPattern, eq)
        components = [{'name' : item, 'code' : item} for item in components]
        
        receivedData = fetchMeterDataByParam(startDateTimeObj.strftime("%d-%m-%Y %H:%M:%S"), endDateTimeObj.strftime("%d-%m-%Y %H:%M:%S"), components, multiplierData = {}, fetchBy = "meterID", excelOnly = False)
        # For 3 days, the return object looks like {statrDateTime : datetimeObj, endDateTime : datetimeObj, xAxisData : [288 items separated by 15 minutes],
        # yAxisDataForAllMeters : {'FK-01' : [288 Data Points], 'FK-02' : [288 Data Points]}}. We only require yAxisDataForAllMeters here.
        # print(receivedData)
        xAxisData = receivedData['xAxisData']
        received_yAxisData = receivedData['yAxisDataForAllMeters']
        # Now using this received_yAxisData, we can get the (FK-01) + (FK-02) + 1000 type of equation data. reportGeneration.py will help in that.
        
        try :
            yAxisDataForThisEquation = getEquationValue(eq, received_yAxisData, no_of_days)
            # yAxisTemplateData[f'{stat} : {eq}'] = yAxisDataForThisEquation
            yAxisTemplateData[f'{stat}'] = yAxisDataForThisEquation
        except IndexError as idxErr :
            yAxisTemplateData[f'{stat} : Wrong Equation'] =  [None] * 96 * no_of_days

    if(excelOnly == True) :
        df = pd.DataFrame(yAxisTemplateData, index = xAxisData)
        df.to_excel('ReportGeneration/TemplateData.xlsx', sheet_name = "Template Data")

        return send_file('ReportGeneration/TemplateData.xlsx', as_attachment=True, download_name="TemplateData.xlsx")

    dataToReturn = { 'startDateTime' : startDateTime, 'endDateTime' : endDateTime, 'xAxisData' : xAxisData, 'yAxisDataForAllMeters' : yAxisTemplateData}

    return dataToReturn
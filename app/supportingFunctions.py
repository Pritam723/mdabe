from datetime import datetime, timedelta
import re

def isFloat(value):
    if(value is None) :
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False

def changeToFloat(x) :
    if(isFloat(x)) :
        return float(x)
    else :
        return None
    
def getMultiplierValue(meter, multiplierData) :
    value = multiplierData.get(meter)
    if(isFloat(value)) :
        return changeToFloat(value)
    else :
        return 1 # +1 is default Multiplier


def datetime_range(startDateTimeObj, endDateTimeObj, offSetMinutes) :
    delta = timedelta(minutes=offSetMinutes)
    current = startDateTimeObj
    while current < (endDateTimeObj + timedelta(minutes = 15)):
        yield current
        current += delta


def date_range(startDateObj, endDateObj, offSetDays) :
    delta = timedelta(days=offSetDays)
    current = startDateObj
    while current < (endDateObj + timedelta(days = 1)):
        yield current
        current += delta

def isMeterNumberPattern(value) :
    meterNumberPattern = re.compile(r'^[A-Z]{2}-[0-9]{4}-[A-Z]$')
    result = re.match(meterNumberPattern, value)
    if result:
        return True
    else:
        return False
    
def isMeterIdPattern(value) :
    meterIdPattern = re.compile(r'^[A-Z]{2}-[0-9]{2}$')
    result = re.match(meterIdPattern, value)
    if result:
        return True
    else:
        return False
    
def getBlockFromTimeStamp(ts) :
    # We receive ts = 04-30-00 in this string format.
    # x = '22:30'
    hour = int(ts.split(':')[0])
    minute = int(ts.split(':')[1])
    block = int(hour*4 + minute/15)
    return block

def generateYearWiseDateIntervals(startDateTime, endDateTime) :
    
    startDateTimeObj = datetime.strptime(startDateTime, '%d-%m-%Y %H:%M:%S')
    endDateTimeObj = datetime.strptime(endDateTime, '%d-%m-%Y %H:%M:%S')
    
    dateIntervals = []
    
    startYear = startDateTimeObj.year
    endYear = endDateTimeObj.year

    # Must handle same year case. Other wise we get wrong output.
    if(startYear == endYear) :
        return [{'startDateTime' : startDateTime, 'endDateTime' : endDateTime}]

    currentYear = startYear

    while(currentYear <= endYear) :
        # print(currentYear)
        if(currentYear == startYear) :
            dateIntervals.append({ 'startDateTime' : startDateTimeObj.strftime('%d-%m-%Y %H:%M:%S'), 'endDateTime' : datetime(startYear, 12, 31, 23, 45, 0).strftime('%d-%m-%Y %H:%M:%S') })
        elif(currentYear == endYear) :
            dateIntervals.append({ 'startDateTime' : datetime(endYear, 1, 1, 0, 0, 0).strftime('%d-%m-%Y %H:%M:%S'), 'endDateTime' : endDateTimeObj.strftime('%d-%m-%Y %H:%M:%S')})
        else :
            dateIntervals.append({'startDateTime' : datetime(currentYear, 1, 1, 0, 0, 0).strftime('%d-%m-%Y %H:%M:%S'), 'endDateTime' : datetime(currentYear, 12, 31, 23, 45, 0).strftime('%d-%m-%Y %H:%M:%S')})

        currentYear = currentYear + 1

    return dateIntervals

def getDifferenceDicts(oldDict, newDict) :
    
    isDifferent = False
    
    deletedInNewDict = {key:oldDict.get(key) for key in oldDict if oldDict.get(key) != newDict.get(key)}

    addedInNewDict = {key:newDict.get(key) for key in newDict if newDict.get(key) != oldDict.get(key)}
    
    if(len(deletedInNewDict) + len(addedInNewDict) > 0) : isDifferent = True
        
    return {'isDifferent' : isDifferent, 'Deleted' : deletedInNewDict, 'Added' : addedInNewDict}

def getDifferenceListOfDict(oldList, newList) :
    
    isDifferent = False
    
    deletedInNewList = []
    for i in oldList:
        if i not in newList:
            deletedInNewList.append(i)

    addedInNewList = []
    for i in newList:
        if i not in oldList:
            addedInNewList.append(i)
    
    if(len(deletedInNewList) + len(addedInNewList) > 0) : isDifferent = True
        
    return {'isDifferent' : isDifferent, 'Deleted' : deletedInNewList, 'Added' : addedInNewList}
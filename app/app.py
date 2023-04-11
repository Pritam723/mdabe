from flask import Flask, request, send_file
from markupsafe import escape
from fetchMeterDataByParam import fetchMeterDataByParam
from fetchActiveReactiveByParam import fetchActiveReactiveByParam
from flask_cors import CORS, cross_origin
from supportingFunctions import *
from meterUtility import MeterUtil
from meterComponents import separateComponentsTillLastMeter
import json
from configurationHistoryUtil import getConfigDataChangeHistory, compareConfigurationDataAllMeter, compareConfigurationDataSelectedMeter
from dbConnectorUtility import DBConnectorUtil

app = Flask(__name__)
CORS(app)

################################################################ Testing Purpose #############################################################################
@app.route('/')
def index():
    return 'Index Page'


@app.route("/hi/<name>")
def hello(name):
    return json.dumps(separateComponentsTillLastMeter('RN-95'))

#############################################################################################################################################################


################################################################ Fetching Meter Meta Data ###################################################################

@app.route("/getMetersListed/<string:fetchBy>" , methods=['GET', 'POST'])
def getMetersListed(fetchBy):

    # meterUtilObj = MeterUtil()
    # realMeters = meterUtilObj.getRealMeters(fetchBy)
    # fictMeters = meterUtilObj.getFictitiousMeters(fetchBy)

    allTimeRealMeterIDs_collectionObj = DBConnectorUtil(collection = "allTimeRealMeterIDs").getCollectionObject()
    allTimeFictMeterIDs_collectionObj = DBConnectorUtil(collection = "allTimeFictMeterIDs").getCollectionObject()

    realMeters = list(allTimeRealMeterIDs_collectionObj.find({}, {'_id' : 0}))
    fictMeters = list(allTimeFictMeterIDs_collectionObj.find({}, {'_id' : 0}))

    metersToBeListed = {'Real Meters' : realMeters, 'Fictitious Meters' : fictMeters}
    return json.dumps(metersToBeListed)

#############################################################################################################################################################



################################################################ Fetching Meter Data MWH data ###############################################################

@app.route("/fetchMeterData/<string:fetchBy>", methods=['GET', 'POST'])
def fetchMeterData(fetchBy):
    data = request.get_json()
    # print(data)

    return fetchMeterDataByParam(startDateTime = data['startDateTime'], endDateTime = data['endDateTime'], selectedMeters = data['selectedMeters'], fetchBy = fetchBy)

@app.route("/fetchMeterDataInExcel", methods=["GET","POST"])
def fetchMeterDataInExcel():
    # data = dict(request)
    data = dict(request.form) 
    # print(type(request))
    # startDateTime = data['startDateTime'], endDateTime = data['endDateTime'], selectedMeters = data['selectedMeters'], fetchBy = fetchBy
    # print(data)
    startDateTime = data['startDateTime']
    endDateTime = data['endDateTime']
    selectedMeters = json.loads(data['selectedMeters'])
    fetchBy = data['fetchBy']

    return fetchMeterDataByParam(startDateTime, endDateTime, selectedMeters, fetchBy, excelOnly = True)

#############################################################################################################################################################

################################################################ Fetching Active Reactive data ###############################################################

@app.route("/fetchActiveReactive/<string:fetchBy>", methods=['GET', 'POST'])
def fetchActiveReactive(fetchBy):
    data = request.get_json()
    # print(data)
    
    return fetchActiveReactiveByParam(startDateTime = data['startDateTime'], endDateTime = data['endDateTime'], selectedMeters = data['selectedMeters'], fetchBy = fetchBy, energyType = data['energyType'])

@app.route("/fetchActiveReactiveInExcel", methods=['GET', 'POST'])
def fetchActiveReactiveInExcel():
    # data = dict(request)
    data = dict(request.form) 
    # print(type(request))
    # startDateTime = data['startDateTime'], endDateTime = data['endDateTime'], selectedMeters = data['selectedMeters'], fetchBy = fetchBy
    # print(data)
    startDateTime = data['startDateTime']
    endDateTime = data['endDateTime']
    selectedMeters = json.loads(data['selectedMeters'])
    fetchBy = data['fetchBy']
    energyType = data['energyType']

    return fetchActiveReactiveByParam(startDateTime, endDateTime, selectedMeters, fetchBy, energyType, excelOnly = True)

#############################################################################################################################################################

################################################################ Fetching Configuration Change History ######################################################

@app.route("/getConfigurationChangeHistory", methods=['GET', 'POST'])
def getConfigurationChangeHistory():
    # data = request.get_json()
    # print(data)

    data = dict(request.form) 

    # print(data) {'configType': 'masterData', 'selectedMeter': 'Any', 'startDateTime': '03-04-2023', 'endDateTime': '03-04-2023'}

    configType = data['configType']
    selectedMeter = data['selectedMeter']
    startDateTime = data['startDateTime']
    endDateTime = data['endDateTime'] # dd-mm-YYYY ex. '03-04-2023'.

    return getConfigDataChangeHistory(configType, selectedMeter, startDateTime, endDateTime)

@app.route("/compareConfigurations", methods=['GET', 'POST'])
def compareConfigurations():
   
    # print("I am here")
    data = dict(request.form) 

    print(data)

    if(data['selectedMeter'] == "Any") :
        return compareConfigurationDataAllMeter(data['prevId'], data['currId'], data['configType'])
    else :
        return compareConfigurationDataSelectedMeter(data['prevId'], data['currId'], data['configType'], data['selectedMeter'])

#############################################################################################################################################################


from flask import Flask, request, send_file
from markupsafe import escape
from fetchMeterDataByParam import fetchMeterDataByParam
from fetchActiveReactiveByParam import fetchActiveReactiveByParam
from flask_cors import CORS, cross_origin
from supportingFunctions import *
from meterUtility import MeterUtil
from meterComponents import separateComponentsTillLastMeter
import json
from configurationHistoryUtil import getConfigDataChangeHistory, compareConfigurationDataAllMeter, compareConfigurationDataSelectedMeter, downloadConfigurationFile
from dbConnectorUtility import DBConnectorUtil
from componentwiseDataUtil import componentWiseData
from templateWiseDataUtil import templateWiseData
import pandas as pd
import os

UPLOAD_FOLDER = 'ConfigurationFiles'
ALLOWED_EXTENSIONS = {'xlsx'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
CORS(app)

################################################################ Testing Purpose #############################################################################
@app.route('/')
def index():
    return 'Index Page'


@app.route("/hi/<name>")
def hello(name):
    return json.dumps(separateComponentsTillLastMeter('RN-95'))

#############################################################################################################################################################

class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime):
            return (str(z))
        else:
            return super().default(z)

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
    print(data)

    return json.dumps(fetchMeterDataByParam(startDateTime = data['startDateTime'], endDateTime = data['endDateTime'], selectedMeters = data['selectedMeters'], multiplierData = data['multiplierData'], fetchBy = fetchBy), cls=DateTimeEncoder)


@app.route("/fetchMeterDataInExcel", methods=["GET","POST"])
def fetchMeterDataInExcel():
    # data = dict(request)
    data = dict(request.form) 
    print(data)
    # print(type(request))
    # startDateTime = data['startDateTime'], endDateTime = data['endDateTime'], selectedMeters = data['selectedMeters'], fetchBy = fetchBy
    # print(data)
    startDateTime = data['startDateTime']
    endDateTime = data['endDateTime']
    selectedMeters = json.loads(data['selectedMeters'])
    multiplierData = json.loads(data['multiplierData'])
    fetchBy = data['fetchBy']

    return fetchMeterDataByParam(startDateTime, endDateTime, selectedMeters, multiplierData, fetchBy, excelOnly = True)

#############################################################################################################################################################

################################################################ Fetching Active Reactive data ###############################################################

@app.route("/fetchActiveReactive/<string:fetchBy>", methods=['GET', 'POST'])
def fetchActiveReactive(fetchBy):
    data = request.get_json()
    # print(data)
    return json.dumps(fetchActiveReactiveByParam(startDateTime = data['startDateTime'], endDateTime = data['endDateTime'], selectedMeters = data['selectedMeters'], multiplierData = data['multiplierData'], fetchBy = fetchBy, energyType = data['energyType']), cls=DateTimeEncoder)

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
    multiplierData = json.loads(data['multiplierData'])
    fetchBy = data['fetchBy']
    energyType = data['energyType']

    return fetchActiveReactiveByParam(startDateTime, endDateTime, selectedMeters, multiplierData, fetchBy, energyType, excelOnly = True)

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
    
    return json.dumps(getConfigDataChangeHistory(configType, selectedMeter, startDateTime, endDateTime), cls=DateTimeEncoder)

@app.route("/compareConfigurations", methods=['GET', 'POST'])
def compareConfigurations():
   
    # print("I am here")
    data = dict(request.form) 

    print(data)

    if(data['selectedMeter'] == "Any") :
        return json.dumps(compareConfigurationDataAllMeter(data['prevId'], data['currId'], data['configType']))
    else :
        return json.dumps(compareConfigurationDataSelectedMeter(data['prevId'], data['currId'], data['configType'], data['selectedMeter']))

@app.route("/downloadConfiguration", methods=['GET', 'POST'])
def downloadConfiguration():
   
    print("downloadConfiguration is called")
    data = dict(request.form) 
    print(data)

    configType = data['configType']
    configId = data['configId']
    startDate = data['startDate'].split(" ")[0]  # "2023-04-21 00:00:00"
    endDate = data['endDate'].split(" ")[0]
    
    return downloadConfigurationFile(configType, configId, startDate, endDate)

 
#############################################################################################################################################################

############################################# Component-wise Data Fetching ##################################################################################

@app.route("/fetchComponentWiseData/<string:fetchBy>", methods=['GET', 'POST'])
def fetchComponentWiseData(fetchBy):
    data = request.get_json()
    print(data)

    # data = dict(request.form) 

    # print(data) {'configType': 'masterData', 'selectedMeter': 'Any', 'startDateTime': '03-04-2023', 'endDateTime': '03-04-2023'}


    configType = "fictcfgData"
    selectedMeter = data['selectedMeters'][0]['name']
    multiplierData = data['multiplierData']
    startDateTime = data['startDateTime'].split(" ")[0]
    endDateTime = data['endDateTime'].split(" ")[0] # dd-mm-YYYY ex. '03-04-2023'.
    # componentType = "Recursive/Level-1"
    componentType = data['componentType']

    return json.dumps(componentWiseData(configType, selectedMeter,multiplierData, startDateTime, endDateTime, componentType, fetchBy), cls=DateTimeEncoder)

@app.route("/fetchComponentWiseDataInExcel", methods=['GET', 'POST'])
def fetchComponentWiseDataInExcel():
    print("fetchComponentWiseDataInExcel is called")
    data = dict(request.form) 
    print(data)

    # data = dict(request.form) 

    # print(data) {'configType': 'masterData', 'selectedMeter': 'Any', 'startDateTime': '03-04-2023', 'endDateTime': '03-04-2023'}


    configType = "fictcfgData"
    selectedMeter = json.loads(data['selectedMeters'])[0]['name']
    multiplierData = json.loads(data['multiplierData'])
    startDateTime = data['startDateTime'].split(" ")[0]
    endDateTime = data['endDateTime'].split(" ")[0] # dd-mm-YYYY ex. '03-04-2023'.
    # componentType = "Recursive/Level-1"
    componentType = data['componentType']

    return componentWiseData(configType, selectedMeter, multiplierData, startDateTime, endDateTime, componentType, fetchBy = "meterID", excelOnly = True)

#############################################################################################################################################################

############################################# Template-wise Data Fetching ###################################################################################

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/changeTemplateConfiguration" , methods=['GET','POST'])
def changeTemplateConfiguration():
    # print("Here")
    # print(request)
    # print(request.files)
    templateFile = request.files['file']

    if templateFile and allowed_file(templateFile.filename):
        templateFile.save(os.path.join(app.config['UPLOAD_FOLDER'], 'TemplatesConfiguration.xlsx'))
        return json.dumps({'success':True}), 200, {'ContentType':'application/json'} 

    return json.dumps({'success':False}), 500, {'ContentType':'application/json'}

@app.route("/downloadTemplateConfiguration" , methods=['GET','POST'])
def downloadTemplateConfiguration():
    return send_file('ConfigurationFiles/TemplatesConfiguration.xlsx', as_attachment=True, download_name="TemplatesConfiguration.xlsx")


@app.route("/getTemplateNames" , methods=['GET'])
def getTemplateNames():

    df = pd.read_excel('ConfigurationFiles/TemplatesConfiguration.xlsx', None)
    sheet_names = df.keys()
    templates = []
    for sheet_name in sheet_names :
        templates.append({'name' : sheet_name, 'code' : sheet_name, 'value' : sheet_name})

    # equationDetails = []


    # for sheet_name in sheet_names :
    # #     print(sheet_name)
    #     equationDetail = {'Template' : sheet_name, 'EquationDetails' : []}
    #     data = pd.read_excel('ConfigurationFiles/TemplatesConfiguration.xlsx', sheet_name=sheet_name)
    #     for index, row in data.iterrows():
    #         equationDetail['EquationDetails'].append({"Name" : row["Statistics"],"Equation" : row["Equation"]})
            
    #     equationDetails.append(equationDetail)

    # return json.dumps(equationDetails)
    return json.dumps(templates)


@app.route("/fetchTemplateWiseData/<string:fetchBy>", methods=['GET', 'POST'])
def fetchTemplateWiseData(fetchBy):
    data = request.get_json()
    print(data)

    # data = dict(request.form) 

    # print(data) {'configType': 'masterData', 'selectedMeter': 'Any', 'startDateTime': '03-04-2023', 'endDateTime': '03-04-2023'}


    selectedTemplate = data['selectedMeters'][0]
    startDateTime = data['startDateTime'].split(" ")[0]
    endDateTime = data['endDateTime'].split(" ")[0] # dd-mm-YYYY ex. '03-04-2023'.

    return json.dumps(templateWiseData(selectedTemplate, startDateTime, endDateTime), cls=DateTimeEncoder)

@app.route("/fetchTemplateDataInExcel", methods=['GET', 'POST'])
def fetchTemplateDataInExcel():
    print("fetchTemplateDataInExcel is called")
    data = dict(request.form) 
    print(data)

    # data = dict(request.form) 

    # print(data) {'configType': 'masterData', 'selectedMeter': 'Any', 'startDateTime': '03-04-2023', 'endDateTime': '03-04-2023'}

    selectedTemplate = json.loads(data['selectedMeters'])[0]
    startDateTime = data['startDateTime'].split(" ")[0]
    endDateTime = data['endDateTime'].split(" ")[0] # dd-mm-YYYY ex. '03-04-2023'.

    return templateWiseData(selectedTemplate, startDateTime, endDateTime, excelOnly = True)


#############################################################################################################################################################


############################################# Dummy Testing #################################################################################################

@app.route("/getConfigurationChangeHistoryDummy", methods=['GET', 'POST'])
def getConfigurationChangeHistoryDummy():
    # data = request.get_json()
    # print(data)

    # data = dict(request.form) 

    # print(data) {'configType': 'masterData', 'selectedMeter': 'Any', 'startDateTime': '03-04-2023', 'endDateTime': '03-04-2023'}
    configInfo = {"masterData" : {"name" : "MASTER.DAT", "id" : "masterDataId", "data" : "realMeters"}, "fictdatData" : {"name" : "FICTMTRS.DAT", "id" : "fictdatDataId", "data" : "fictMeters"}, "fictcfgData" : {"name" : "FICTMTRS.CFG", "id" : "fictcfgDataID", "data" : "fictCFGs"} }


    configType = "fictcfgData"
    selectedMeter = 'OP-92'
    startDateTime = '27-12-2017'
    endDateTime = '31-01-2021' # dd-mm-YYYY ex. '03-04-2023'.

    return json.dumps(getConfigDataChangeHistory(configType, selectedMeter, startDateTime, endDateTime), cls=DateTimeEncoder)

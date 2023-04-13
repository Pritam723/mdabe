from meterComponents import separateComponents, separateComponentsTillLastMeter
from configurationHistoryUtil import getConfigDataChangeHistory

configInfo = {"masterData" : {"name" : "MASTER.DAT", "id" : "masterDataId", "data" : "realMeters"}, "fictdatData" : {"name" : "FICTMTRS.DAT", "id" : "fictdatDataId", "data" : "fictMeters"}, "fictcfgData" : {"name" : "FICTMTRS.CFG", "id" : "fictcfgDataID", "data" : "fictCFGs"} }

def componentWiseData(configType, selectedMeter, startDateTime, endDateTime, componentType):

    configDataChangeHistory = getConfigDataChangeHistory(configType, selectedMeter, startDateTime, endDateTime)

    unionAllMeters = set({})

    for item in configDataChangeHistory :
        print(str(item['startDate']) + " to " + str(item['endDate']) + " configID is " + str(item['configDataId']))
        print("###################################################################################")
        print("Printing the separate components")
        
        print(separateComponentsTillLastMeter(selectedMeter, item['configDataId']))
        unionAllMeters = unionAllMeters.union(separateComponentsTillLastMeter(selectedMeter, item['configDataId'])['components'])
        print("###################################################################################")

    print(list(unionAllMeters))

    return configDataChangeHistory

from meterUtility import MeterUtil
from configurationHistoryUtil import getConfigDataByID
# from dbConnectorUtility import DBConnectorUtil
import re
#################################################### Component Seperator Function ####################################################################
def separateComponents(meterID, fictcfgDataID, allTimeRealMeterIDs_collectionObj, allTimeFictMeterIDs_collectionObj, configData_collectionObj) :
    '''Component Seperator Function for next level'''
    equationSet = {}
    # allTimeRealMeterIDs_collectionObj = DBConnectorUtil(collection = 'allTimeRealMeterIDs').getCollectionObject()
    # allTimeFictMeterIDs_collectionObj = DBConnectorUtil(collection = 'allTimeFictMeterIDs').getCollectionObject()
    
    realMeterObj = allTimeRealMeterIDs_collectionObj.find_one({'name' : meterID}, {'_id' : 0, 'code' : 0})
    fictMeterObj = allTimeFictMeterIDs_collectionObj.find_one({'name' : meterID}, {'_id' : 0, 'code' : 0})
    
    if(realMeterObj is not None) :  # So it is a real meter
        meterEquation = meterID
        equationSet[meterID] = meterEquation

    elif(fictMeterObj is not None): # So it is a Fict meter
        
        meterEquation = getConfigDataByID(fictcfgDataID,"fictcfgData",configData_collectionObj).get(f'({meterID})')
        
        # Now it may happen that, meterID is in allTimeFictMeterIDs but for this 'fictcfgDataID', no entry exists.
        if(meterEquation is None): return {'components' : [], 'listOfEquations' : equationSet}
    
    else : # No meter entry for this ID
        return {'components' : [], 'listOfEquations' : equationSet}

    meterEquation = meterEquation.replace(' ','')
    meterEquation = meterEquation.replace(u'\xa0', u'') # Non breaking space.
    meterEquation = meterEquation.replace('\t','')
    meterEquation = meterEquation.replace('\n','')
    equationSet[meterID] = meterEquation

    meterIdPattern = re.compile(r'[A-Z]{2}-[0-9]{2}')
    components = re.findall(meterIdPattern, meterEquation)
    # print(len(components))
    # return components
    return {'components' : list(components), 'listOfEquations' : equationSet}

def separateComponentsTillLastMeter(meterID, fictcfgDataID, allTimeRealMeterIDs_collectionObj, allTimeFictMeterIDs_collectionObj, configData_collectionObj) :
    '''Component Seperator Function till last level'''

    equationSet = {}
    previous_components = set({meterID})
    next_components = set({})

    while(True) :

        for previous_component in previous_components :

            try :
                # findEq = fictMeterDict[f'({previous_component})'].strip()
                findEq = getConfigDataByID(fictcfgDataID,"fictcfgData",configData_collectionObj)[f'({previous_component})'].strip()
                # print(f'{previous_component} = {findEq}')
                equationSet[previous_component] = findEq

            except KeyError as e :
                # Basically No entry is there for this ID. So most probably it is a Real Meter.
                pass

            separated_components = separateComponents(previous_component, fictcfgDataID, allTimeRealMeterIDs_collectionObj, allTimeFictMeterIDs_collectionObj, configData_collectionObj)['components']
            next_components = next_components.union(separated_components)

        if(next_components == previous_components) :
            break

        previous_components = next_components
        next_components = set({})

    return {'components' : list(previous_components), 'listOfEquations' : equationSet}
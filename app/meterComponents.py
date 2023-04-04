from meterUtility import MeterUtil
import re
#################################################### Component Seperator Function ####################################################################
def separateComponents(meterID, meterUtilObj) :
    '''Component Seperator Function for next level'''


    if(meterUtilObj.getMeterInfoById(meterID) is not None) :  # So it is a real meter
        meterEquation = meterID
    elif(meterUtilObj.getFictMeterInfoById(meterID) is not None): # So it is a Fict meter
        meterEquation = meterUtilObj.getFictitiousMeterEqutation('('+ meterID +')')     # fictMeterDict['('+ meterID +')']
    else : # No meter entry for this ID
        return []



    meterEquation = meterEquation.replace(' ','')
    meterEquation = meterEquation.replace(u'\xa0', u'') # Non breaking space.
    meterEquation = meterEquation.replace('\t','')
    meterEquation = meterEquation.replace('\n','')

    meterIdPattern = re.compile(r'[A-Z]{2}-[0-9]{2}')
    components = re.findall(meterIdPattern, meterEquation)
    # print(len(components))
    return components

def separateComponentsTillLastMeter(meterID) :
    '''Component Seperator Function till last level'''
    
    meterUtilObj = MeterUtil()

    equationSet = {}
    previous_components = set({meterID})
    next_components = set({})

    while(True) :

        for previous_component in previous_components :

            try :
                findEq = meterUtilObj.getFictitiousMeterEqutation(f'({previous_component})').strip()
                # print(f'{previous_component} = {findEq}')
                equationSet[previous_component] = findEq

            except KeyError as e :
                # Basically No entry is there for this ID. So most probably it is a Real Meter.
                pass

            separated_components = separateComponents(previous_component, meterUtilObj)
            next_components = next_components.union(separated_components)

        if(next_components == previous_components) :
            break

        previous_components = next_components
        next_components = set({})

    return {'components' : list(previous_components), 'listOfEquations' : equationSet}
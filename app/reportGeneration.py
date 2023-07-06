import os
from supportingFunctions import *
import pandas as pd
import json
from datetime import datetime

#################################################### Evaluate Function ####################################################################
def ffOperation(f1,f2,op) :  # Both operands are float
    
    if(f1 is None or f2 is None) : return None
    
    if op == '+': return float(f1) + float(f2)
    if op == '-': return float(f1) - float(f2)
    if op == '*': return float(f1) * float(f2)
    if op == '/': 
        if(float(f2) == float(0)) : return None
        else : return float(f1)/float(f2)

def ll_Operation(a,b,op) : # Both operands are list
    returnList = [e for e in list(map(lambda x,y: ffOperation(x,y,op), a, b))]
    return returnList

# Function to perform arithmetic operations.
def applyOp(a, b, op):
    print("Inside applyOp")

    if(isinstance(a, list) and isinstance(b, list)) :
        # print("1")
        return(ll_Operation(a,b,op))

    elif(isinstance(a, float) and isinstance(b, float)) :
        # print("2")
        return(ffOperation(a,b,op))

    if(isinstance(a, list) and isinstance(b, float)) :
        # print("3")
        customList = [b] * len(a)
        return(ll_Operation(a,customList,op))

    elif(isinstance(a, float) and isinstance(b, list)) :
        # print("4")
        customList = [a] * len(b)
        return(ll_Operation(customList,b,op))
    
    else :
        # print("5")
        pass

# Python3 program to evaluate a given expression where tokens are separated by space.
# Function to find precedence of operators.
def precedence(op):
    
    if op == '+' or op == '-':
        return 1
    if op == '*' or op == '/':
        return 2
    return 0

# Function that returns value of expression after evaluation.
def evaluate(tokens, yAxisData):
    
    # stack to store integer values.
    values = []
    
    # stack to store operators.
    ops = []
    i = 0

    while i < len(tokens):
        
        # Current token is a whitespace,
        # skip it.
        if tokens[i] == ' ':
            i += 1
            continue
        
        # Current token is an opening 
        # brace, push it to 'ops'
        elif tokens[i] == '(':
            ops.append(tokens[i])
        # print("Printing 1st : " +str(ops))
        # Current token is a number, push 
        # it to stack for numbers.

        # Current token is a meter ID, push 
        # it to stack for numbers.
        elif tokens[i].isalpha() :
            val = tokens[i : i+5].replace("_","-") 

            values.append(yAxisData[val])

            
            i+=4              
            
        elif(tokens[i].isdigit() or tokens[i] == '.') :
            val = ""
            
            # There may be more than one
            # digits in the number.
            while (i < len(tokens) and (tokens[i].isdigit() or tokens[i] == '.')):             
                val = val + tokens[i]
                i += 1
            
            values.append(float(val))
            # print(values) 
            # right now the i points to 
            # the character next to the digit,
            # since the for loop also increases 
            # the i, we would skip one 
            #  token position; we need to 
            # decrease the value of i by 1 to
            # correct the offset.
            i-=1
        
        # Closing brace encountered, 
        # solve entire brace.
        elif tokens[i] == ')':
        
            while len(ops) != 0 and ops[-1] != '(':
            
                val2 = values.pop()
                val1 = values.pop()
                op = ops.pop()
    #                 print(ops)

                values.append(applyOp(val1, val2, op))
            
            # pop opening brace.
            ops.pop()
    #             print(ops)
        # Current token is an operator.
        else:
        
            # While top of 'ops' has same or 
            # greater precedence to current 
            # token, which is an operator. 
            # Apply operator on top of 'ops' 
            # to top two elements in values stack.
            while (len(ops) != 0 and
                precedence(ops[-1]) >=
                precedence(tokens[i])):
                        
                val2 = values.pop()
                val1 = values.pop()
                op = ops.pop()
                
                values.append(applyOp(val1, val2, op))
            
            # Push current token to 'ops'.
            ops.append(tokens[i])
        
        i += 1
    
    # Entire expression has been parsed 
    # at this point, apply remaining ops 
    # to remaining values.
    while len(ops) != 0:
        
        val2 = values.pop()
        val1 = values.pop()
        op = ops.pop()
                
        values.append(applyOp(val1, val2, op))
    
    # Top of 'values' contains result,
    # return it.
    return values[-1]


def getEquationValue(eq, yAxisData, no_of_days) :
    # eq = 'FK-01 + FK-02'

    # But in case eq is only float like eq = '1000', then handle it separately.
    if(isFloat(eq)) :
        return [float(eq)] * 96 * no_of_days

    myExp = eq    # "+3*MN-17+(MN-18)" . # This may throw KeyEroor if no equation exists. 
    myExp = myExp.replace(' ','')
    myExp = myExp.replace('\t','')
    
    # print(myExp)

    if(myExp[0] == '+' or myExp[0] == '-') :
        myExp = '0' + myExp

    myExp = re.sub('([A-Z]{2})-([0-9]{2})', r'\1_\2', myExp)

    yAxisDataForThisEquation = evaluate(myExp, yAxisData)

    return yAxisDataForThisEquation
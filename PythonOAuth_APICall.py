#-------------------------------------------------------------------------------------
# 
# ProgName: Python_Oauth_API_JSON2FileMaer
# ProgDate: 2023/6/5
# ProgDesc: 
#      1. 讀取FILEMAKER的取得KeyDataABC Shop ID
#.     2. 使用OAuth取得Token Key, 叫用相關API 取得回傳JSON Data, Parse資料存入FileMaker
#      2. 讀取完需記錄本次執行日期
#      3. 每日定期執行一次
#
# Modification Hist:
# RemarkID    date     Description   Remark
# 20230605JL  20230605 Init
#-----------------------------------------------------------------------------------------

import fmrest
import pymssql
import json
import sys
import os
import shutil
import unicodedata
import itertools
import time
import multiprocessing as mp
import requests
import json

from requests_oauthlib import OAuth2Session
from requests import Request, Session
from multiprocessing import Process, Pool
from os import listdir
from os.path import isfile, isdir, join
from datetime import datetime, timedelta
from pathlib import Path
from configparser import ConfigParser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# 20221129JL sub for truncated the controlcode in the string
#import unicodedata
def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")

# 20240609 Jeff lai extract_jsondata_key_value
def extract_key_value(json_data, key):
    """Extracts a specific key-value pair from a JSON data"""
    data = json.loads(json_data)
    value = data.get(key)
    return value
    
# 20221129JL sub : Log the status/error on execution
def log2file(strlog,iniFile):
    Conf = ConfigParser()	
    Conf.read(iniFile,encoding="utf-8")
    Log = Conf['Log']
    now = datetime.now() # current date and time
    datestr = now.strftime("%Y%m%d")
    date_time = now.strftime("%Y/%m/%d, %H:%M:%S.%f")
    logpath = Log['LogPath']
    Log_pf = Log['log_pf']
    logfilename = Log_pf + '-' + datestr + '.log' 
    logfile = join( logpath , logfilename )
    #print(logfile)
    lf = open(logfile,'a+',encoding='UTF8', newline='')
    lf.write( date_time + "==>>" + str(strlog) +'\n' )
    #print(date_time + "==>>" + str(strlog) +'\n')
    lf.close()
    
#20221129JL main prog Start
def CDEDashbboardAPI(ABCShopID,pDate,iniFile,APINo):
    
    WINDOWS_LINE_ENDING = str(b'\r\n')
    UNIX_LINE_ENDING = b'\n'
    #prgIniFile = "ABC_Mid_transactions.ini"
    Conf = ConfigParser()	
    Conf.read(iniFile,encoding="utf-8")
    
    eSystem = Conf['eSystem']
    fmsserver=eSystem['fmsserver'] 
    fmsuser=eSystem['fmsuser'] 
    fmspasswd = eSystem['fmspasswd'] 
    fmsDB = eSystem['fmsDB'] 
    fmsLayout=eSystem['fmslayoutSaveReult'] 
    print(fmsLayout)
    
    CDEDashboard=Conf['CDEDashboard'] 
    username = CDEDashboard['username']    #
    password = CDEDashboard['password']    #''
    client_id = CDEDashboard['client_id']    #'
    client_secret = CDEDashboard['client_secret']    #''
    authorize_url = CDEDashboard['authorize_url']    #
    token_url = CDEDashboard['token_url']    #
    redirect_uri = CDEDashboard['redirect_uri']    #
    application_url = CDEDashboard['application_url']    #
    scope = CDEDashboard['scope']    #'Dashboard'
    
    API=Conf['API'] 
    api_url = API['api_url']    
    api_url = api_url +"fromDate="+ pDate +"&toDate="+ pDate +"&shopIds="+ str(ABCShopID)
    payload = "grant_type=password&client_id="+client_id+"&client_secret="+client_secret+"&username="+username+"&password="+password+"&scope="+scope

    headers = { 'accept': "application/json", 'content-type': "application/x-www-form-urlencoded" }
    response = requests.request("POST", token_url, data=payload, headers=headers)
    j = response.json()

    token = j['access_token']
    #print(token)
    headers1 = {"Authorization": "Bearer {}".format(token) }
    response1 = requests.get(api_url, headers=headers1 )
    #dict define of the report 
    dictReturnJsonSection={'chartWeek':'chartWeek', 'chartHour':'chartHour',
                           'chartAmount':'chartAmount','totalNums':'totalNums',
                           'discounts':'discounts','promotions':'promotions',
                           'diningOptions':'diningOptions','payments':'payments',
                           'categoryas':'categoryas','categorybs':'categorybs',
                           'categorycs':'categorycs','categoryds':'categoryds'
                           }
    dictWeekday = {'Sun':0,'Mon':1,'Tue':2,'Wed':3,'Thu':4,'Fri':5,'Sat':6}
    dt_object = datetime.strptime(pDate, "%Y-%m-%d")
    oriweekday = dt_object.strftime("%a")
    fromDate = pDate
    
    jsonstr = response1.json()
    
    fmsa = fmrest.Server(fmsserver,user=fmsuser,password=fmspasswd,database=fmsDB,layout=fmsLayout,api_version='v1')
    try:
        fmsa.login()
    except:
        try:
            fmsa.login()
        except:
            ErrorStr ="Unexpected login error:"+ str(sys.exc_info())+ "\n" 
            print(ErrorStr)
            log2file(ErrorStr ,iniFile)
    
    for DataSectionKey in dictReturnJsonSection:
        DataSection = jsonstr[DataSectionKey]
        if DataSectionKey == list(dictReturnJsonSection)[0]: 
            KeyWeekday = dictWeekday[oriweekday]
            FMSjsonstr="{'ABCShopID':" + str(ABCShopID) + "" 
            ABCDate = str(fromDate[5:7])+"/"+str(fromDate[8:10])+"/"+str(fromDate[0:4])
            FMSjsonstr = FMSjsonstr + ",'ABCDate':'" + ABCDate + "'"
            FMSjsonstr = FMSjsonstr + ",'ABCSummaryCategory':'" + DataSectionKey + "'"
            JsonWeekdayNetSale = DataSection[KeyWeekday]
            for key,values in JsonWeekdayNetSale.items():
                if values is None:
                    values = ""
                if type(values) == str:
                    #print('your variable is a string')
                    FMSjsonstr = FMSjsonstr + ",'" + key + "':'" + values + "'"
                else:
                    #print('your variable IS NOT a string')
                    FMSjsonstr = FMSjsonstr + ",'" + key + "':" + str(values) 
            FMSjsonstr = FMSjsonstr+ "}" 
            FMSjsonstr = remove_control_characters(FMSjsonstr.replace("'","\""))
            print(FMSjsonstr)
            #ReportAdd2FMS(FMSjsonstr,prgIniFile )
            FMSjsonstr = json.loads(FMSjsonstr)
            try:
                NREC = fmsa.create_record(FMSjsonstr)
            except:
                time.sleep(1)
                try:
                    NREC = fmsa.create_record(FMSjsonstr)
                except:
                    # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
                    #if str(sys.exc_info()[1]).find('504') == -1 :
                    ErrorStr ="Unexpected error:"+ str(sys.exc_info())+ "\n" + str(FMSjsonstr)
                    print(ErrorStr)
                    log2file(ErrorStr ,iniFile)
        elif DataSectionKey == list(dictReturnJsonSection)[1]:
            lenJsonHrNetsale = len(DataSection)
            for i in range(lenJsonHrNetsale):
                JsonHourNetSale = DataSection[i]
                FMSjsonstr="{'ABCShopID':" + str(ABCShopID) + "" 
                ABCDate = str(fromDate[5:7])+"/"+str(fromDate[8:10])+"/"+str(fromDate[0:4])
                FMSjsonstr = FMSjsonstr + ",'ABCDate':'" + ABCDate + "'"
                FMSjsonstr = FMSjsonstr + ",'ABCSummaryCategory':'" + DataSectionKey + "'"
                for key,values in JsonHourNetSale.items():
  #                 print(key+ "  ==> " + str(values))
                    if values is None:
                        print (values)
                        values = " "
                    if type(values) == str:
                        FMSjsonstr = FMSjsonstr + ",'" + key + "':'" + values + "'"
                    else:
                        FMSjsonstr = FMSjsonstr + ",'" + key + "':" + str(values)
                FMSjsonstr = FMSjsonstr+ "}" 
                FMSjsonstr = remove_control_characters(FMSjsonstr.replace("'","\""))
                print(FMSjsonstr)
                #ReportAdd2FMS(FMSjsonstr,prgIniFile 
                FMSjsonstr = json.loads(FMSjsonstr)
                try:
                    NREC = fmsa.create_record(FMSjsonstr)
                except:
                    time.sleep(1)
                    try:
                        NREC = fmsa.create_record(FMSjsonstr)
                    except:
                        time.sleep(1)
                        try:
                            NREC = fmsa.create_record(FMSjsonstr)
                        except:
                            # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
                            #if str(sys.exc_info()[1]).find('504') == -1 :
                            ErrorStr ="Unexpected error:"+ str(sys.exc_info())+ "\n" + str(FMSjsonstr)
                            print(ErrorStr)
                            log2file(ErrorStr ,iniFile)
        elif DataSectionKey == list(dictReturnJsonSection)[2]:
            lenJsonAmountRateNetsale = len(DataSection)
            
            for i in range(lenJsonAmountRateNetsale):
                FMSjsonstr="{'ABCShopID':" + str(ABCShopID) + "" 
                ABCDate = str(fromDate[5:7])+"/"+str(fromDate[8:10])+"/"+str(fromDate[0:4])
                FMSjsonstr = FMSjsonstr + ",'ABCDate':'" + ABCDate + "'"
                FMSjsonstr = FMSjsonstr + ",'ABCSummaryCategory':'" + DataSectionKey + "'"
                JsonAmountRateNetSale = DataSection[i]
                for key,values in JsonAmountRateNetSale.items():
                    #print(key+ "  ==> " + str(values))
                    if values is None:
                        print(values)
                        values = 0
                    if type(values) == str:
                        #print('your variable is a string')
                        FMSjsonstr = FMSjsonstr + ",'" + key + "':'" + values + "'"
                    else:
                        #print('your variable IS NOT a string')
                        FMSjsonstr = FMSjsonstr + ",'" + key + "':" + str(values) 
                FMSjsonstr = FMSjsonstr+ "}" 
                FMSjsonstr = remove_control_characters(FMSjsonstr.replace("'","\""))
                print(FMSjsonstr)
                #ReportAdd2FMS(FMSjsonstr,prgIniFile )
                FMSjsonstr = json.loads(FMSjsonstr)
                try:
                    NREC = fmsa.create_record(FMSjsonstr)
                except:
                    time.sleep(1)
                    try:
                        NREC = fmsa.create_record(FMSjsonstr)
                    except:
                        # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
                        #if str(sys.exc_info()[1]).find('504') == -1 :
                        ErrorStr ="Unexpected error:"+ str(sys.exc_info())+ "\n" + str(FMSjsonstr)
                        print(ErrorStr)
                        log2file(ErrorStr ,iniFile)
        else:
            FMSjsonstr={}
            if DataSection != []:
                try:
                    test=DataSection[0]
                    lenJsonOtherData = len(DataSection)
                    for i in range(lenJsonOtherData):
                        #print("I=? " + str(i))
                        FMSjsonstr="{'ABCShopID':" + str(ABCShopID) + "" 
                        ABCDate = str(fromDate[5:7])+"/"+str(fromDate[8:10])+"/"+str(fromDate[0:4])
                        FMSjsonstr = FMSjsonstr + ",'ABCDate':'" + ABCDate + "'"
                        FMSjsonstr = FMSjsonstr + ",'ABCSummaryCategory':'" + DataSectionKey + "'"
                        JsonotherData = DataSection[i]
                        for key,values in JsonotherData.items():
                            #print(key+ "  ==> " + str(values))
                            if values is None:
                                #print(values)
                                values = 0
                            if type(values) == str:
                                #print('your variable is a string')
                                FMSjsonstr = FMSjsonstr + ",'" + key + "':'" + values + "'"
                            else:
                                #print('your variable IS NOT a string')
                                FMSjsonstr = FMSjsonstr + ",'" + key + "':" + str(values) 
                        FMSjsonstr = FMSjsonstr+ "}" 
                        FMSjsonstr = remove_control_characters(FMSjsonstr.replace("'","\""))
                        print(FMSjsonstr)
                        #ReportAdd2FMS(FMSjsonstr,prgIniFile )
                        FMSjsonstr = json.loads(FMSjsonstr)
                        try:
                            NREC = fmsa.create_record(FMSjsonstr)
                        except:
                            time.sleep(1)
                            try:
                                NREC = fmsa.create_record(FMSjsonstr)
                            except:
                                # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
                                #if str(sys.exc_info()[1]).find('504') == -1 :
                                ErrorStr ="Unexpected error:"+ str(sys.exc_info())+ "\n" + str(FMSjsonstr)
                                print(ErrorStr)
                                log2file(ErrorStr ,iniFile)
                except:
                    JsonotherData = DataSection
                    FMSjsonstr="{'ABCShopID':" + str(ABCShopID) + "" 
                    ABCDate = str(fromDate[5:7])+"/"+str(fromDate[8:10])+"/"+str(fromDate[0:4])
                    FMSjsonstr = FMSjsonstr + ",'ABCDate':'" + ABCDate + "'"
                    FMSjsonstr = FMSjsonstr + ",'ABCSummaryCategory':'" + DataSectionKey + "'"
                    for key,values in JsonotherData.items():
                        print(key+ "  ==> " + str(values))
                        if values is None:
                            print(values)
                            values = 0
                        if type(values) == str:
                            #print('your variable is a string')
                            FMSjsonstr = FMSjsonstr + ",'" + key + "':'" + values + "'"
                        else:
                            #print('your variable IS NOT a string')
                            FMSjsonstr = FMSjsonstr + ",'" + key + "':" + str(values) 
                    FMSjsonstr = FMSjsonstr+ "}" 
                    FMSjsonstr = remove_control_characters(FMSjsonstr.replace("'","\""))
                    print(FMSjsonstr)
                    #ReportAdd2FMS(FMSjsonstr,prgIniFile )
                    FMSjsonstr = json.loads(FMSjsonstr)
                    try:
                        NREC = fmsa.create_record(FMSjsonstr)
                    except:
                        time.sleep(1)
                        try:
                            NREC = fmsa.create_record(FMSjsonstr)
                        except:
                            # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
                            #if str(sys.exc_info()[1]).find('504') == -1 :
                            ErrorStr ="Unexpected error:"+ str(sys.exc_info())+ "\n" + str(FMSjsonstr)
                            print(ErrorStr)
                            log2file(ErrorStr ,iniFile)
        
    #log2file("Child Process"+processid +":Prog End=================================" ,prgIniFile)
    #ENDTS = datetime.now()

    #log2file("Process Time=>"+ str(StartTS) + " TO " + str(ENDTS), prgIniFile)
    fmsa.logout()

def ReportAdd2FMS(ReportData,iniFile): 

    WINDOWS_LINE_ENDING = str(b'\r\n')
    UNIX_LINE_ENDING = b'\n'
    #prgIniFile = "ABC_Mid_transactions.ini"
    Conf = ConfigParser()	
    Conf.read(iniFile,encoding="utf-8")
    
    eSystem = Conf['eSystem']
    fmsserver=eSystem['fmsserver'] 
    fmsuser=eSystem['fmsuser'] 
    fmspasswd = eSystem['fmspasswd'] 
    fmsDB = eSystem['fmsDB'] 
    fmsLayout=eSystem['fmslayoutSaveReult'] 
    print(fmsLayout)
    
    fmsa = fmrest.Server(fmsserver,user=fmsuser,password=fmspasswd,database=fmsDB,layout=fmsLayout,api_version='v1')
    try:
        fmsa.login()
    except:
        ErrorStr ="Login 2 FMS SSERVER Fail. Unexpected error:"+ str(sys.exc_info())+ "\n"
        log2file(ErrorStr, iniFile)
        time.sleep( 3 )
        fmsa.login()
    
    #FMSjsonstr = json.loads(ReportData)
    #FMSjsonstr = json.dumps(ReportData)
    FMSjsonstr = json.loads(ReportData)
    try:
        NREC = fmsa.create_record(FMSjsonstr)
    except:
        # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
        #if str(sys.exc_info()[1]).find('504') == -1 :
        ErrorStr ="Unexpected error:"+ str(sys.exc_info())+ "\n" + str(FMSjsonstr)
        print(ErrorStr)
        log2file(ErrorStr ,iniFile)
    fmsa.logout()
    return
     

    
def getEsystemABCshopid():
    WINDOWS_LINE_ENDING = str(b'\r\n')
    UNIX_LINE_ENDING = b'\n'
    prgIniFile = "CDEDashboardReportAPIv3.ini"
    Conf = ConfigParser()	
    Conf.read(prgIniFile ,encoding="utf-8")
    eSystem = Conf['eSystem']
    fmsserver=eSystem['fmsserver'] 
    fmsuser=eSystem['fmsuser'] 
    fmspasswd = eSystem['fmspasswd'] 
    fmsDB = eSystem['fmsDB'] 
    fmsLayout=eSystem['fmslayoutReadABCID']  
    
    API=Conf['API'] 
    fromDate = API['fromDate']    #2023-05-15
    toDate = API['toDate']    #2023-05-15
    fms = fmrest.Server(fmsserver,user=fmsuser,password=fmspasswd,database=fmsDB,layout=fmsLayout,api_version='v1')
    fms.login()

    processid = str(os.getpid())
    log2file("Child Process:"+processid +"==>Prog Start================================" ,prgIniFile)
    StartTS = datetime.now()
    
    #find_query = [{'status': 'In Business','ABCShopID': '98'}]
    find_query = [{'status': 'In Business'}]
    order_by = [{'fieldName': 'ABCShopID', 'sortOrder': 'descend'}] 
    #order_by = [{'fieldName': 'ABCShopID', 'sortOrder': 'ascend'}]
    foundset = fms.find(find_query, sort=order_by)
    ListABCShopid = []
    # Read data from eSystem:Layout=> fmslayoutReadABCID  to get ABCID
    record = fms.get_records()
    for r in foundset:
        if  r.ABCShopID != "" :
            print ("GlobalShopID: "+str(r.ContractGlobal_ID)+" Corporate Name: "+str(r.CorporateName)+" GlobalShopName: "+ str(r.ContractGlobal_StoreName) +" ABCShopID: "+str(r.ABCShopID))
            #CDEDashbboardAPI(r.ABCShopID,prgIniFile,1)
            ListABCShopid.append(r.ABCShopID)
            #sleep(2)
            #break

    # END
    
    fms.logout()
    
    periodstartdate = datetime.strptime(fromDate, '%Y-%m-%d').date() # fromDate
    periodenddate = datetime.strptime(toDate, '%Y-%m-%d').date()     # todate
    
    if periodstartdate > periodenddate:
        print("Wrong Period criteria")
        quit()
        
    for nday in range(int ((periodenddate - periodstartdate).days)+1):
        processdate = periodstartdate + timedelta(nday)
        print(processdate)
        processdate = processdate.strftime("%Y-%m-%d")
        for ShopID in ListABCShopid:
            print(ShopID)
            CDEDashbboardAPI(ShopID,processdate,prgIniFile,1)
            time.sleep(1)
        
    ENDTS = datetime.now()
    log2file("Child Process:"+processid +"==>Process Time=>"+ str(StartTS) + " TO " + str(ENDTS), prgIniFile)
   
if __name__=='__main__':
    
    prgIniFile = "CDEDashboardReportAPI.ini"
    Conf = ConfigParser()	
    Conf.read(prgIniFile ,encoding="utf-8")

    getEsystemABCshopid()
    quit()    


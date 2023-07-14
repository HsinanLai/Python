#-------------------------------------------------------------------------------------
# ProgName: Python Multi Thread and JSON process and Transfer Data from MSSQL TO Filemaker by ODATA 
# ProgDate: 2022/12/1
# ProgDesc: 
#      1. 讀取MSSQL 的Transaction Data 的資料轉存至FileMaker
#      2. 讀取完需計錄下次讀取起點
#      3. 力求資料變更時時讀入(約每十分鐘讀取一次)
#      4. 增加平行處理能力 Multi Thread and Parallel Process
#
# Modification Hist:
# RemarkID    date     Description   Remark
# 20221129JL 20221129 Init
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

from multiprocessing import Process, Pool
from os import listdir
from os.path import isfile, isdir, join
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# 20221129JL sub for truncated the controlcode in the string
#import unicodedata
def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")
    
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
    
# 20221201 JL sub : Log the error ID to reexcute
def redofile(strlog,refilename,iniFile):
    Conf = ConfigParser()	
    Conf.read(iniFile,encoding="utf-8")
    Log = Conf['Log']
    #now = datetime.now() # current date and time
    #datestr = now.strftime("%Y%m%d")
    #date_time = now.strftime("%Y/%m/%d, %H:%M:%S.%f")
    logpath = Log['LogPath']
    #Log_pf = Log['log_pf']
    logfilename = refilename + '.csv' 
    logfile = join( logpath , logfilename )
    #print(logfile)
    lf = open(logfile,'a+',encoding='UTF8', newline='')
    lf.write( str(strlog) +'\n' )
    #print(date_time + "==>>" + str(strlog) +'\n')
    lf.close()

# 20221129JL sub : mail the Log to the admin  1Time/Hrs
def sendmail(mailmsg,iniFile):
    now = datetime.now() # current date and time
    date_time = now.strftime("%Y/%m/%d, %H:%M:%S.%f")
    date_time_hr = int(now.strftime("%H"))
    date_time_min = int(now.strftime("%M"))
    ##if date_time_hr < 23 :
    #if date_time_min > 10 :
    #   print(" mail send once per hour")
    ##   return True
    # Step 2 - Create message object instance
    msg = MIMEMultipart()
    # Step 3 - Create message body
    message =  date_time + ">>>" + mailmsg
    MConf = ConfigParser()	
    MConf.read(iniFile,encoding="utf-8")
    SMTP = MConf['SMTP']
    # Step 4 - Declare SMTP credentials
    password = SMTP['SMTPpass'] 
    username = SMTP['SMTPuser'] #"abc@abc.com or abc"
    smtphost = SMTP['SMTPhost'] #"server:port ex: smtp.office365.com:587"
    # Step 5 - Declare message elements
    EmailNotify = SMTP['EmailNotify']  # Does it need notify the admin
    if ( EmailNotify != 'Yes' ): 
        print('no need notify')
        return True
    msg['From'] = SMTP['SMTPFrom']  # "abc@abc.com"
    recstr = SMTP['SMTPTo']    # 
    recipent= recstr.split(',')
    msg['To'] = ", ".join(recipent)
    #print (  msg['To'] )  
    
    Log = MConf['Log']
    now = datetime.now() # current date and time
    datestr = now.strftime("%Y%m%d")
    logpath = Log['LogPath']
    Log_pf = Log['logfile-prefix']
    logfilename = Log_pf + '-' + datestr + '.log' 
    logfile = join( logpath , logfilename )
    #msg.attach(MIMEText(open(logfile).read()))
    
    # 構造附件
    att = MIMEText(open(logfile, "rb").read(), "base64", "utf-8")
    att["Content-Type"] = "application/octet-stream"
    # 附件名稱為中文時的寫法
    att.add_header("Content-Disposition", "attachment", filename=("utf-8", "", logfile))
    # 附件名稱非中文時的寫法
    # att["Content-Disposition"] = ‘attachment; filename="test.html")‘
    msg.attach(att)
    
    msg['Subject'] = "[TCPOS Transaction] " + date_time + ": Batch process Complete "
    #print (msg['Subject'])
    # Step 6 - Add the message body to the object instance
    msg.attach(MIMEText(message, 'plain'))
    # Step 7 - Create the server connection
    server = smtplib.SMTP(smtphost)
    # Step 8 - Switch the connection over to TLS encryption
    server.starttls() 
    # Step 9 - Authenticate with the server
    server.login(username, password)
    # Step 10 - Send the message
    # server.sendmail(msg['From'], msg['To'], msg.as_string())
    server.sendmail(msg['From'], recipent, msg.as_string())
    # Step 11 - Disconnect
    server.quit()


#20221129JL main prog Start
def forkprocess(offsetid,startid,maxid,limit):
    WINDOWS_LINE_ENDING = str(b'\r\n')
    UNIX_LINE_ENDING = b'\n'
    prgIniFile = "TCPos_Mid_transactions.ini"
    Conf = ConfigParser()	
    Conf.read(prgIniFile ,encoding="utf-8")
    eSystem = Conf['eSystem']
    fmsserver=eSystem['fmsserver'] 
    fmsuser=eSystem['fmsuser'] 
    fmspasswd = eSystem['fmspasswd'] 
    fmsDB = eSystem['fmsDB'] 
    fmsLayout=eSystem['fmsLayout'] 

    TCPOS = Conf['TCPOS']
    TCPosServer = TCPOS['TCPosServer'] 
    TCPosUser = TCPOS['TCPosUser'] 
    TCPosPasswd = TCPOS['TCPosPasswd'] 
    TCPosDB = TCPOS['TCPosDB'] 

    #Tran_info = Conf['TranInfo']
    #NextID = Tran_info['NextID'] 

    fms = fmrest.Server(fmsserver,user=fmsuser,password=fmspasswd,database=fmsDB,layout=fmsLayout,api_version='v1')
    fms.login()

    #print (pymssql)
    conn = pymssql.connect(server=TCPosServer, user=TCPosUser, password=TCPosPasswd, database=TCPosDB)
    #print(conn)
    #NextID = para_id
    processid = str(os.getpid())
    log2file("Child Process:"+processid +"==>Prog Start================================" ,prgIniFile)
    StartTS = datetime.now()
    sql = "select * from middle_transactions where id >"+ str(startid)+" and id <="+ str(maxid)
    sql = sql +" order by id OFFSET " + str(offsetid) + " ROWS FETCH NEXT " + str(limit) + " ROWS ONLY"
    log2file(sql,prgIniFile)
    #sql ='select * from middle_promotions'
    curr = conn.cursor(as_dict=True)
    #print(curr)
    curr.execute(sql)
    row = curr.fetchone()
    num_fields=len(curr.description)
    #cursor description[1] Dtaa Type
    #1 : STRING type
    #2 : BINARY type
    #3 : NUMBER type( either an integer or a floating point value. )
    #4 : DATETIME type
    #5 : ROWID type/DECIMAL
    #for i in curr.description:
    #    print(i)
    while row:
        #print(row['valid_from'])
        jsonstr="{"
        for i in curr.description:
            if str(row[i[0]]) == 'None':
                match i[1]:
                    case 1:
                        jsonstr= jsonstr + '"' + str(i[0]) + '":"",' 
                    case 3:
                        jsonstr= jsonstr + '"' + str(i[0]) + '":0,' 
                    case 5:
                        jsonstr= jsonstr + '"' + str(i[0]) + '":0,' 
                    case 4:
                        jsonstr= jsonstr + '"' + str(i[0]) + '":"01/01/1900 00:00:00",' 
                        #jsonstr= jsonstr + '"' + str(i[0]) + '":"00/00/0000 00:00:00",' 
            else:
                #print(str(i[0])+"==>"+str(row[i[0]]))
                match i[1]:
                    case 1:
                        retstr = str(row[i[0]]).replace(chr(10),'¶')
                        jsonstr= jsonstr + '"' + str(i[0]) + '":"' + remove_control_characters(retstr) + '",'
                    case 3:
                        jsonstr= jsonstr + '"' + str(i[0]) + '":' + str(row[i[0]]) + ','
                    case 5:
                        jsonstr= jsonstr + '"' + str(i[0]) + '":' + str(row[i[0]]) + ','
                    case 4:
                        datestr = remove_control_characters(str(row[i[0]]).replace(" ",""))
                        #print(datestr)
                        #print(datestr[0:4])
                        datestr = str(datestr[5:7])+"/"+str(datestr[8:10])+"/"+str(datestr[0:4])+" "+str(datestr[10:18])
                        #hours= int(datestr[14:15])
                        #if hours < 12:
                        jsonstr= jsonstr + '"' + str(i[0]) + '":"' + datestr + '",'
                        #else:
                        #    jsonstr= jsonstr + '"' + str(i[0]) + '":"' + datestr + '",'
                #jsonstr= jsonstr + '"' + str(i[0]) + '":"' + str(row[i[0]]).replace(chr(9),"_").replace(WINDOWS_LINE_ENDING,'¶') + '",'
                #retstr = str(row[i[0]]).replace(chr(10),'¶')
                #jsonstr= jsonstr + '"' + str(i[0]) + '":"' + remove_control_characters(str(row[i[0]])) + '",'
                #jsonstr= jsonstr + '"' + str(i[0]) + '":"' + remove_control_characters(retstr) + '",'
        retryID = row['id']
        
        jsonstr= jsonstr[0:int(-1)] + "}"
        #print(jsonstr)
        jsondata = json.loads(jsonstr)
        #log2file(jsondata ,prgIniFile)
        try:
            NREC = fms.create_record(jsondata)
            #print(NREC)
        except:
            # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
            if str(sys.exc_info()[1]).find('504') == -1 :
                ErrorStr ="Unexpected error:"+ str(sys.exc_info())+ "\n" + str(jsondata)
                log2file(ErrorStr ,prgIniFile)
                strRedo = "id,"+str(retryID)+",ErrorDesc,"+str(sys.exc_info()[1])
                redofilename = processid+'_retryid'
                redofile(strRedo,redofilename,prgIniFile)
        row = curr.fetchone()

    fms.logout()
    conn.close()
    #log2file("Child Process"+processid +":Prog End=================================" ,prgIniFile)
    ENDTS = datetime.now()

    log2file("Child Process:"+processid +"==>Process Time=>"+ str(StartTS) + " TO " + str(ENDTS), prgIniFile)
    

    #for row in curr:
    #    print(row)
def multi_run_wrapper(args):
   return forkprocess(*args)
   
if __name__=='__main__':

    prgIniFile = "TCPos_Mid_transactions.ini"
    Conf = ConfigParser()	
    Conf.read(prgIniFile ,encoding="utf-8")
    TCPOS = Conf['TCPOS']
    TCPosServer = TCPOS['TCPosServer'] 
    TCPosUser = TCPOS['TCPosUser'] 
    TCPosPasswd = TCPOS['TCPosPasswd'] 
    TCPosDB = TCPOS['TCPosDB'] 
    
    Tran_info = Conf['TranInfo']
    NextID = Tran_info['NextID'] 
    #forkprocess(0,NextID,int(NextID)+1,1)
    limit = 1000
    processnum = 10
    processrec = int(limit*processnum)
    re_mp = 11 
    for re_mp_cnt in range(re_mp):
        Tran_info = Conf['TranInfo']
        NextID = Tran_info['NextID'] 
        mainconn = pymssql.connect(server=TCPosServer, user=TCPosUser, password=TCPosPasswd, database=TCPosDB)
        cntsql ='select id from middle_transactions where id >'+ str(NextID) 
        cntsql = cntsql+' order by id offset '+ str(processrec) +' ROWS FETCH NEXT 1 ROWS ONLY '
        cntcurr = mainconn.cursor()
        cntcurr.execute(cntsql)
      
        cntrow = cntcurr.fetchone()
        print(cntrow)
        maxid = cntrow[0]
        mainconn.close()
        startid = int(NextID)
       
        StartTS = datetime.now()
        log2file("===Main(Parent) Prog Start==>" + str(StartTS)+"-------------------------------" ,prgIniFile)
        inputs=[]
        
        for i in range(processnum):
            inputs.append((limit*(i),startid,maxid,limit))
            #inputs = [(limit*0,startid,maxid,limit),(limit*1,startid,maxid,limit),(limit*2,startid,maxid,limit),(limit*3,startid,maxid,limit),(limit*4,startid,maxid,limit),(limit*5,startid,maxid,limit)]
        print(str(re_mp_cnt)+"==>"+str(inputs)+"\n")
        log2file(str(inputs), prgIniFile)
        pool = mp.Pool(processnum)

        #pool_outputs = pool.map_async(forkprocess, inputs)
        pool_outputs = pool.map_async( multi_run_wrapper, inputs)
        print('將不會阻塞並和 pool.map_async 並行觸發')

        pool.close()
        pool.join()
        LastRunTS = str(datetime.now()) 
        Conf.set("TranInfo","LastRunTS", LastRunTS) 
        Conf.set("TranInfo","NextID", str(maxid))
        #ENDTS = datetime.now()
        log2file("main(Parent) Process(END) Time=>"+ str(StartTS) + " TO " + str(LastRunTS) +"\n Num of Process:"+str(processnum)+"\n Ttal Recs:"+str(processrec), prgIniFile)
        # Writing our configuration file to 'example.ini'
        try:
            with open(prgIniFile, 'w',encoding='UTF8') as configfile:
                Conf.write(configfile)
        except:
            # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
            print("WriteConfig:",  str(NextID))
        print("program will resume after 1 sec")
        time.sleep(1)

       

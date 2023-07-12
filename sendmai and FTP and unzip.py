#import paramiko
import os
import sys
import shutil
from os import listdir
from os.path import isfile, isdir, join
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from zipfile import ZipFile
import smtplib
import time
import fnmatch
import csv
import sqlite3
import uuid


def unzip(filename, temppath):
    #temppath='temp'
    with ZipFile(filename, 'r') as zip:
        zip.extractall(temppath)
        log2file('File is unzipped in temp folder')
    
def log2file(strlog):
    Conf = ConfigParser()	
    Conf.read("sendmail2.ini")
    Log = Conf['Log']
    SFTP = Conf['SFTP']
    PLine = FileInfo['PLine']
    now = datetime.now() # current date and time
    datestr = now.strftime("%Y%m%d")
    date_time = now.strftime("%Y/%m/%d, %H:%M:%S.%f")
    logpath = Log['LogPath']
    logfilename = 'Hot-AOI-' + PLine + '-' + datestr + '.log' 
    logfile = join( logpath , logfilename )
    #print(logfile)
    lf = open(logfile,'a+',encoding='UTF8', newline='')
    lf.write( date_time + ">>" + strlog +'\n' )
    lf.close()
    
def sendmail(mailmsg):
    now = datetime.now() # current date and time
    date_time = now.strftime("%Y/%m/%d, %H:%M:%S.%f")
    date_time_hr = int(now.strftime("%H"))
    date_time_min = int(now.strftime("%M"))
    #if date_time_hr < 23 :hs
    #    if date_time_min < 50 :
    #       print(" mail send once per day")
    #       return True
    # Step 2 - Create message object instance
    msg = MIMEMultipart()
    # Step 3 - Create message body
    message =  date_time + ">>>" + mailmsg
    MConf = ConfigParser()	
    MConf.read("sendmail2.ini")
    SMTP = MConf['SMTP']
    password = SMTP['SMTPpass']
    # Step 4 - Declare SMTP credentials
    password = SMTP['SMTPpass'] 
    username = SMTP['SMTPuser'] #"username@ucompname.com"
    smtphost = SMTP['SMTPhost'] #"smtp.office365.com:587"
    print(smtphost)
    # Step 5 - Declare message elements
    #EmailNotify = SMTP['EmailNotify']  #"username@ucompname.com"
    #if ( EmailNotify != 'Yes' ): 
    #    print('no need notify')
    #    return True
    msg['From'] = SMTP['SMTPFrom']  #"username@ucompname.com"
    recstr = SMTP['SMTPTo']    #"username@ucompname.com"
    recipent= recstr.split(',')
    msg['To'] = ", ".join(recipent)
    #print (  msg['To'] )  
    
    Log = MConf['Log']
    now = datetime.now() # current date and time
    datestr = now.strftime("%Y%m%d")
    logpath = Log['LogPath']
    PLine = SMTP['PLine']
    #logfilename = 'Hot-AOI-' + PLine + '-' + datestr + '.log' 
    #logfile = join( logpath , logfilename )
    #msg.attach(MIMEText(open(logfile).read()))
    
    # 構造附件
    #att = MIMEText(open(logfile, "rb").read(), "base64", "utf-8")
    #att["Content-Type"] = "application/octet-stream"
    # 附件名稱為中文時的寫法
    #att.add_header("Content-Disposition", "attachment", filename=("utf-8", "", logfile))
    # 附件名稱非中文時的寫法
    # att["Content-Disposition"] = ‘attachment; filename="test.html")‘
    #msg.attach(att)
    
    msg['Subject'] = "[IT Batch Program] " + PLine + " - " + date_time + ": Data process of HOT AOI for AISymphoy Complete "
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
    
def sendmailnofile(mailmsg):
    now = datetime.now() # current date and time
    date_time = now.strftime("%Y/%m/%d, %H:%M:%S.%f")
    date_time_hr = int(now.strftime("%H"))
    date_time_min = int(now.strftime("%M"))
    #if date_time_hr < 23 :
    #if date_time_min > 10 :
    #   print(" mail send once per day")
    #   return True
    # Step 2 - Create message object instance
    msg = MIMEMultipart()
    # Step 3 - Create message body
    message =  date_time + ">>>" + mailmsg
    MConf = ConfigParser()	
    MConf.read("sendmail2.ini")
    SMTP = MConf['SMTP']
    password = SMTP['SMTPpass']
    # Step 4 - Declare SMTP credentials
    password = SMTP['SMTPpass'] 
    username = SMTP['SMTPuser'] #"username@ucompname.com"
    
    smtphost = SMTP['SMTPhost'] #"smtp.office365.com:587"
    # Step 5 - Declare message elements
    EmailNotify = SMTP['EmailNotify']  #"username@ucompname.com"
    if ( EmailNotify != 'Yes' ): 
        print('no need notify')
        return True
    msg['From'] = SMTP['SMTPFrom']  #"username@ucompname.com"
    recstr = SMTP['SMTPTo']    #"username@ucompname.com"
    #print('rec str ==> ' + recstr)
    recipent= recstr.split(',')
    msg['To'] = ", ".join(recipent)
    #print (  msg['To'] )  
    
    Log = MConf['Log']
    now = datetime.now() # current date and time
    datestr = now.strftime("%Y%m%d")
    logpath = Log['LogPath']
    PLine = SMTP['PLine']
    logfilename = 'Hot-AOI-' + PLine + '-' + datestr + '.log' 
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
    
    msg['Subject'] = "[IT Batch Program] " + PLine + " - " + date_time + ": No Zip file(report) in Directory "
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
    
def sftp():
    now = datetime.now() # current date and time
    datestr = now.strftime("%Y%m%d")
   
    #Read Config data
    Config1 = ConfigParser()	
    Config1.read("sendmail2.ini")
    SFTP = Config1['SFTP']
    SFTPip = SFTP['SFTPip']
    SFTPUser = SFTP['SFTPUser']
    SFTPPasswd = SFTP['SFTPPasswd']
    SFTPPort = SFTP['SFTPPort']
    FileInfo= Config1['FileInfo']
    csvPath = FileInfo['csvPath']     #"//192.168.66.50/HotDataAI$"
    SFTPtargetpath = FileInfo['SFTPTargetPath']   #"/process_data/testftp/"
    PLine = FileInfo['PLine']   #"/process_data/testftp/"
    exeStatus=True
    count = 1
    while True:
        try:
            client = paramiko.SSHClient()   # 获取SSHClient实例
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            time.sleep(3) 
            client.connect(SFTPip,  username=SFTPUser, password=SFTPPasswd, port=SFTPPort, timeout=10 ) #SSH服务端
            #client.connect(SFTPip,  username=SFTPUser, password=SFTPPasswd, port=SFTPPort ) #SSH服务端
            transport = client.get_transport()   # 获取Transport实例
            
        # 创建sftp对象，SFTPClient是定义怎么传输文件、怎么交互文件
            sftp = paramiko.SFTPClient.from_transport(transport)
            print("sftp connected")
            break
        except Exception as e:
            #print(e)
            log2file(' error:' + e.args[0] + " , retry:" + str(count) )
            if count < 5: 
                time.sleep(1) 
                print("retry>>>" , count)
                count = count+1 
                client.close()
                continue
            else:
                log2file(  ' error:' + e.args[0]  )
                client.close()
                exeStatus=False
                return False
                break
    if exeStatus: 
        log2file(  ' SFTP connected successful' )
    
    files = fnmatch.filter(listdir(csvPath), '*.csv')
    for f in files:
        fullpath = join(csvPath, f)
        print("csvfile: "+fullpath)
        try: 
            #tempdatestr = now.strftime("%Y%m%d%H%M%S%f")
            my_uuid = uuid.uuid4()
            strtcsv = 'temp-' + str(my_uuid) + '.csv'
            tempfile = join(csvPath, strtcsv)
            log2file( tempfile + ' tempfile name!!'  )
            shutil.copyfile(fullpath, tempfile)
            targertfile = join(SFTPtargetpath, f)
            sftp.put(tempfile, targertfile)
            log2file( fullpath + ' put to SFTP server successful!!'  )
            log2file( targertfile + ' on SFTP server!!')
            os.remove(fullpath)
            os.remove(tempfile)
            log2file(fullpath + ' remove from client successful!!'  )
        except Exception as e:
            log2file( ' error:' + e.args[0]  )
            exeStatus=False
            client.close()
            return False
    #关闭连接
    client.close()
    return True
sendmail('Process Successful')
exit()


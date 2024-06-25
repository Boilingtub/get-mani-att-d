import sys
import base64
import mysql.connector
import os
from Google import Create_Service

class CONFIG:
   def LoadCFG(cfgfilepath):
     parms = []
     with open(cfgfilepath) as cfg:
         for line in cfg:
           if '>>>' in line: 
            parms.append(CONFIG.CutCFGinfo(line))
         return parms

   def CutCFGinfo(string):
     pos = string.index('>>>')
     string = string[:-1]
     return string[pos+3:] 


class DataManipulation:
   
   def Balance(InStr):
       result = []

       substr = 'transactions performed on'
       pos = InStr.index(substr) + len(substr)
       result.append(DataManipulation.convertdate(InStr[pos+3:pos+11]))
        
       substr = 'Opening Balance was'
       pos = InStr.index(substr) + len(substr)
       result.append(InStr[pos+2:InStr.index('There is a total of')-9])

       substr = 'There is a total of'
       pos = InStr.index(substr) + len(substr)
       result.append(InStr[pos+2:InStr.index('successful Topup transactions')-2])
       
       substr = 'Topup amount of all the transaction is'
       pos = InStr.index(substr) + len(substr)
       result.append(InStr[pos+2:InStr.index('Your Closing Balance is')-9])

       substr = 'Your Closing Balance is'
       pos = InStr.index(substr) + len(substr)
       transaction_value = result.append(InStr[pos+2:InStr.rfind('Best Regards Digicel Team')-12])

       return result


   def Transaction(InStr):
       InStr = InStr + ','
       result = [] 
       while InStr.rfind(',') > 0:       
        result.append(InStr[:InStr.index(',')])
        InStr = InStr[InStr.index(',')+1:]
       result[1] = DataManipulation.convertdate(result[1])
       return result
   
   def convertdate(InStr):
       finstr = "20" + InStr[-2:] + "-" + InStr[:2] + "-" + InStr[3:5]   
       return finstr
     
        


class switch:
     def Island(x):
        if x == "airvantage":
         return list(('00','JMD'))
        if x == "airvantagebbs":
         return list(('01','BBS'))



class Getmail:

   def DownloadAttachment(UserAdress , downloadpath , filter):
        msgdict = service.users().messages().list(userId=UserAdress,q=filter).execute()
        print(msgdict)
        if msgdict['resultSizeEstimate'] == 0:
         print('No messages found')
         exit()
        msgid = msgdict['messages'][0]  #for loop if more than first message is needed
        msg = service.users().messages().get(userId=UserAdress,id=msgid['id']).execute()
        for part in msg['payload']['parts']:
              if part['filename']:
                 if 'data' in part['body']:
                   data = part['body']['data']
                 else:
                   attachment = service.users().messages().attachments().get(userId=UserAdress,messageId=msgid['id'],id=part['body']['attachmentId']).execute()   
                   data = attachment['data'] 
                 filedata = base64.urlsafe_b64decode(data.encode('UTF-8'))     
                 downloaddest = downloadpath + part['filename'] 
                 global attpath
                 attpath = downloaddest
                 with open(downloaddest , 'wb') as file:
                    file.write(filedata)
                    print('Attachment Success')

   def GetEmailBodyText(UserAdress , filter):
        msgdict = service.users().messages().list(userId=UserAdress,q=filter).execute()
        msgid = msgdict['messages'][0]  #for loop if more than first message is needed
        msg = service.users().messages().get(userId=UserAdress,id=msgid['id'],format="full").execute()
        for part in msg['payload']['parts']: 
            if 'parts' in part:
               for subpart in part['parts']:
                   if 'body' in subpart:
                      return base64.urlsafe_b64decode(subpart['body']['data']).decode("UTF-8")                    
                    
   def ManipulateAttachment():
       pos = attpath.rfind(".")
       filetype = attpath[pos:]
       if filetype == ".zip":
          os.system("unzip " + attpath)
          global extattpath
          extattpath = attpath[:pos] + ".csv"
          os.system("rm " + attpath)
          with open(extattpath) as f:
             line = f.readline().rstrip()
             linedata = DataManipulation.Transaction(line)
             os.system("sed -i \'s#,"+linedata[3]+","+linedata[4]+",#,#g\' " + extattpath + "\n" + "sed -i \'s#," + linedata[7] + ".*$##g\' " + extattpath)
          
   def ReadIntoDB(SQLhost , SQLusr , SQLpasswd, SQLdb):
        db = mysql.connector.connect(host = SQLhost, user = SQLusr, passwd = SQLpasswd, database = SQLdb)
        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS dts_balance_tbl (date DATE, island CHAR(2), opening_balance DECIMAL(20,2), transaction_count MEDIUMINT, transaction_value DECIMAL(20,2), closing_balance DECIMAL(20,2));")
        cursor.execute("CREATE TABLE IF NOT EXISTS dts_transaction_tbl (date DATE, time TIME, island CHAR(2), advanced_id BIGINT, msisdn_code BIGINT, loan_amount DECIMAL(10,2));")
        
        try:
         BalData = DataManipulation.Balance(Getmail.GetEmailBodyText(str(CFG[1]),str(CFG[3])))
         cursor.execute("INSERT INTO dts_balance_tbl (date, island, opening_balance, transaction_count, transaction_value, closing_balance) VALUES (%s, %s, %s, %s, %s, %s)",(BalData[0], islandinfo[0], BalData[1], BalData[2], BalData[3], BalData[4]))
                     
         with open(extattpath) as data:
               for line in data:
                      LineValues = DataManipulation.Transaction(line)
                      cursor.execute("INSERT INTO dts_transaction_tbl (date, time , island, advanced_id, msisdn_code, loan_amount) VALUES (%s,%s,%s,%s,%s,%s)",(LineValues[1],LineValues[2],islandinfo[0],LineValues[0],LineValues[3],LineValues[4]))

         db.commit()
         print('Database Success')
        except:
          db.rollback()
          print('Database ERROR : Error with Inserting New Records')
        
        db.close()
        os.system("rm " + extattpath)


global CFG
CFG = CONFIG.LoadCFG('get-mani-att-d.config')
CFG[3] = str(CFG[3])+"*"+sys.argv[1]+"*"+sys.argv[2]

CRED_FILE = str(CFG[0])
API_NAME = 'gmail'
API_VERSION = 'v1'
SCOPES = ['https://mail.google.com/']
service = Create_Service(CRED_FILE , API_NAME , API_VERSION , SCOPES)

global islandinfo
islandinfo = switch.Island(sys.argv[1])

if str(CFG[8]) == "true":  
 Getmail.DownloadAttachment(str(CFG[1]),str(CFG[2]),str(CFG[3]))
 Getmail.ManipulateAttachment()
 Getmail.ReadIntoDB(str(CFG[4]),str(CFG[5]),str(CFG[6]),str(CFG[7]))
else:
 Getmail.DownloadAttachment(str(CFG[1]),str(CFG[2]),str(CFG[3]))
 BodyText = Getmail.GetEmailBodyText(str(CFG[1]),str(CFG[3]))
 with open('EmailBodytext.txt' , 'w') as f:
      f.write(BodyText)

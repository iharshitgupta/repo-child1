#1. import
import re
import sys
import json
import logging
from datetime import datetime, timedelta
from pytz import timezone
import google.auth
from google.cloud import bigquery
from google.oauth2 import credentials
from google.oauth2 import service_account
from google.auth import impersonated_credentials
from google.cloud import storage
from google.cloud.bigquery import magics
from google.cloud import logging
import os
from os.path import exists
import Braze_Logging as CustomeLog
target_scopesbq = ['https://www.googleapis.com/auth/bigquery']
target_scopesbucket = ['https://www.googleapis.com/auth/devstorage.read_write']
import time

#Set TimeZone to tz = timezone('UTC')
try:
   tz = timezone('UTC')
   #1.0 - Read Arguments
   tmp_job_group_nm =sys.argv[1]
   print("Input Argument : "+tmp_job_group_nm)
except Exception as e:
   print("Error Setting up Timezone & Argumetns ::" + str(e))
   sys.exit(99)
   #exit()
#print(dir(BL))
try:
   #Build Log Path
   # #Log Starting
   logFileName=datetime.strftime(datetime.now(tz),'%Y_%m_%d_%H_%M_%S') +"_" +tmp_job_group_nm +".txt"
   logFilePath =os.getcwd() +"/"+ logFileName 
   CustomeLog.Create_Log(logFilePath)
   CustomeLog.Write_Log(logFilePath, "****************Job Started DateTime in UTC::" + datetime.strftime(datetime.now(tz),'%Y-%m-%d-%H-%M-%S'))
   CustomeLog.Write_Log(logFilePath, "Argument ::" + tmp_job_group_nm )
except Exception as e:
   print("Error writting intial log & Argument details ::" + str(e))   
   CustomeLog.Write_ErrorLog(logFilePath,"Error writting intial log & Argument details :: "+ str(e)) 

try:
   jsonconfigfile=open('Braze_Configuration.json',"r")
   jsonconfigdata = json.load(jsonconfigfile)
   ServiceAccount=jsonconfigdata["ServiceAccount"]
   ComputeProject=jsonconfigdata["ComputeProject"]
   BrazeBucketName=jsonconfigdata["BrazeBucketName"]
   BrazeEDPArchiveBucketName=jsonconfigdata["BrazeEDPArchiveBucketName"]
   BrazeConfigTableName=jsonconfigdata["BrazeConfigTableName"]
   ETLLogBucket=jsonconfigdata["ETLLogBucket"]
   prefix_root=jsonconfigdata["prefix_root"]
   CustomeLog.Write_Log(logFilePath,"ServiceAccount:{} , ComputeProject:{}, BrazeBucketName: {}, BrazeEDPArchiveBucketName: {}".format(ServiceAccount, ComputeProject, BrazeBucketName, BrazeEDPArchiveBucketName))
except Exception as e:
   print("Error while reading JSON File ::" + str(e))   
   CustomeLog.Write_ErrorLog(logFilePath,"Error while reading JSON File :: "+ str(e))   

#1.1 Read Google Project & Service accounts from JSON Configration
try:
   #1.2. Google Libs
   #login user credentials using gcloud auth login.
   credentials, project_id = google.auth.default()
   #instead use the login user credentials
   target_credentials = impersonated_credentials.Credentials(
      source_credentials = credentials,
      target_principal=ServiceAccount,
      target_scopes = target_scopesbq,
      delegates=[],
      lifetime=500)
   magics.context.credentials = target_credentials
   bigquery_client = bigquery.Client(project=ComputeProject,credentials=target_credentials)
   client = logging.Client(project=ComputeProject,credentials=target_credentials)
   #2.2 - Read Job Configration from BigQuery Config table for that Arguments

   bucketcredentials, bucketproject_id = google.auth.default()
   buckettarget_credentials = impersonated_credentials.Credentials(
      source_credentials = bucketcredentials,
      target_principal=ServiceAccount,   #this service account should have access to the storage bucket.
      target_scopes = target_scopesbucket,
      delegates=[],
      lifetime=500)
   storage_client = storage.Client(project=ComputeProject,credentials=buckettarget_credentials)   #using python storage client library to use impersonated credentials on the project where the bucket exists.
   job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO, schema_update_options='ALLOW_FIELD_ADDITION')
   source_bucket = storage_client.bucket(BrazeBucketName)
   destination_bucket = storage_client.bucket(BrazeEDPArchiveBucketName)
   CustomeLog.Write_Log(logFilePath,"****************Google credentials is all set")

except Exception as e:
    print("Error while intializing Google credentials ::" + str(e))    
    CustomeLog.Write_ErrorLog(logFilePath,"Error while intializing Google credentials :: "+ str(e))   
  
try:
   #2. Read BigQuery Configration from BQ DB Table for that specific group
   sqlquery="SELECT row_id ,job_group_nm ,gcp_project_nm ,gcp_bucket_nm ,braze_event_nm ,braze_event_filter_txt ,bq_stg_table_nm, last_event_read_utc_dts, is_etl_metadata_created_ind FROM "+BrazeConfigTableName+" WHERE job_group_nm='" +  tmp_job_group_nm +"' AND is_active_ind=true order by row_id;"
   CustomeLog.Write_Log(logFilePath,"Select SQL - "+sqlquery)
   query_job=bigquery_client.query(sqlquery)
   results = query_job.result()  #Not working, - Commented
   truncateresults = query_job.result()  #Not working, - Commented
   CustomeLog.Write_Log(logFilePath,"****************Read compelted from BigQuery configration table")
except Exception as e:

print("Error when readign Configration data ::"+ str(e))
    CustomeLog.Write_ErrorLog(logFilePath,"Error when readign Configration data ::" +str(e))
   
try:
   CustomeLog.Write_Log(logFilePath,"****************Truncate All STG table before starting ingestion")
   for row in truncateresults:
      if(row["is_etl_metadata_created_ind"]):
         TruncateSql="TRUNCATE TABLE "+row["bq_stg_table_nm"]
         CustomeLog.Write_Log(logFilePath,"Truncate SQL :: "+TruncateSql)
         TruncateQueryJob=bigquery_client.query(TruncateSql)
         TruncateQueryResults = TruncateQueryJob.result()     
         CustomeLog.Write_Log(logFilePath,"SUCCESS:: Truncate :: " + row["bq_stg_table_nm"])

CustomeLog.Write_Log(logFilePath,"SUCCESS - Truncate Before Ingestion")
   TruncateSql=""
   for row in results:
      row_id=row["row_id"]
      tmpevent_last_crawl_dts=row["last_event_read_utc_dts"]
      tmpbraze_event_nm=row["braze_event_nm"]
      tmpbraze_event_filter_txt=row["braze_event_filter_txt"]
      tmpbq_stg_table_nm=row["bq_stg_table_nm"]
      tmpis_etl_metadata_created_ind=row["is_etl_metadata_created_ind"]
      CustomeLog.Write_Log(logFilePath,"****************Loading Event:{} Avro file from bucket{} & location:{} after datetime: {}

#1.0.1 - Set Time Zone & Build Filter records
      tmpLatestBlobDatetime=(datetime.now(tz)-timedelta(days=1000))
      TodayFilter= datetime.strftime(datetime.now(tz),'%Y-%m-%d')
      OneDayBeforeFilter =datetime.strftime((datetime.now(tz)-timedelta(days=1)),'%Y-%m-%d')
      TwoDatBeforeFilter =datetime.strftime((datetime.now(tz)-timedelta(days=2)),'%Y-%m-%d')
      ThreeDatBeforeFilter =datetime.strftime((datetime.now(tz)-timedelta(days=3)),'%Y-%m-%d')
      FourDatBeforeFilter =datetime.strftime((datetime.now(tz)-timedelta(days=4)),'%Y-%m-%d')
      CurrentDateTime=datetime.now(tz)
      prefix_val = prefix_root+"/"+tmpbraze_event_filter_txt+"/"
      print("prefix val: "+prefix_val)
      print("Event Last Crawl Date:: "+datetime.strftime(tmpevent_last_crawl_dts,'%Y-%m-%d %H:%M:%S.%f'))      
      CustomeLog.Write_Log(logFilePath,"prefix val: "+prefix_val)

blobs = storage_client.list_blobs(BrazeBucketName, prefix=prefix_val)   #Get all Blob in that bucket
      RecordCount=0
      EventCommitCounter=0
      List_of_Uris=[]
      EventLoadStartTimeStamp =datetime.now(tz)

# is_STG_Table_Exists = False
      # CustomeLog.Write_Log(logFilePath,"START:: Checkign if STG table exists Before Truncate - "+tmpbq_stg_table_nm)
      # CheckTableSQL ="SELECT true from "+ tmpbq_stg_table_nm.split('.')[0]+"."+ tmpbq_stg_table_nm.split('.')[1]+".INFORMATION_SCHEMA.TABLES WHERE table_name='"+ tmpbq_stg_table_nm.split('.')[2]+"';"
      # CustomeLog.Write_Log(logFilePath,"CheckTableSQL - "+CheckTableSQL) 
      # CheckTableQueryJob=bigquery_client.query(CheckTableSQL)  
      # CheckTableQueryResults=CheckTableQueryJob.result() 
      # for row in CheckTableQueryResults:
      #    is_STG_Table_Exists=True  
      #    CustomeLog.Write_Log(logFilePath,"SUCCESS:: Table Exists Before Truncate- "+tmpbq_stg_table_nm)
      
      # if (is_STG_Table_Exists):

 #    #Truncate STG table for Incremental load 
      #    CustomeLog.Write_Log(logFilePath,"START:: Table Exists - Performing Truncate Before Ingest table :: " + tmpbq_stg_table_nm)
      #    TruncateSql="TRUNCATE TABLE "+tmpbq_stg_table_nm
      #    CustomeLog.Write_Log(logFilePath,"Truncate SQL - "+TruncateSql)
      #    TruncateQueryJob=bigquery_client.query(TruncateSql)
      #    TruncateQueryResults = TruncateQueryJob.result()     
      #    CustomeLog.Write_Log(logFilePath,"SUCCESS:: Truncate :: " + tmpbq_stg_table_nm)
      # else: 
      #    CustomeLog.Write_Log(logFilePath,"START:: Table NOT Exists - Not performing Truncate :: " + tmpbq_stg_table_nm)


      for blob in blobs:
         #if  (tmpbraze_event_filter_txt in blob.name) and (TodayFilter in blob.name or OneDayBeforeFilter in blob.name or TwoDatBeforeFilter in blob.name  or ThreeDatBeforeFilter in blob.name or FourDatBeforeFilter in blob.name ):
         if  (tmpbraze_event_filter_txt in blob.name):
            if(datetime.strptime(datetime.strftime(blob.updated,'%Y-%m-%d %H:%M:%S.%f'),'%Y-%m-%d %H:%M:%S.%f')>tmpevent_last_crawl_dts):
               CustomeLog.Write_Log(logFilePath,"START:: Append File :: {}".format(blob.name))
               RecordCount=RecordCount+1

      EventCommitCounter=EventCommitCounter+1
               #print("gs://"+BrazeBucketName+"/"+blob.name)
               #load_job = bigquery_client.load_table_from_uri("gs://"+BrazeBucketName+"/"+blob.name, tmpbq_stg_table_nm, job_config=job_config)  # Make an API request.
               List_of_Uris.append("gs://"+BrazeBucketName+"/"+blob.name)                              
               CustomeLog.Write_Log(logFilePath,"SUCCESS:: Append")
               #Copy event to Archive bucket in QA
               CustomeLog.Write_Log(logFilePath,"START:: Archive")
               source_bucket.copy_blob(blob, destination_bucket,blob.name)
               CustomeLog.Write_Log(logFilePath,"SUCCESS:: Archive")

if (blob.updated > tmpLatestBlobDatetime):
                  tmpLatestBlobDatetime=blob.updated
               if(EventCommitCounter >= 400):
                  print("Reached 400 Event - Commiting")
                  #CustomeLog.Write_Log(logFilePath,"START:: Ingestion for 400 event files, List of files:: {}".format(' :: '.join(List_of_Uris)))
                  CustomeLog.Write_Log(logFilePath,"START:: Ingestion for 400 event files")
                  load_job = bigquery_client.load_table_from_uri(List_of_Uris, tmpbq_stg_table_nm,job_config=job_config)
                  CustomeLog.Write_Log(logFilePath,"SUCCESS:: Ingestion")
                  CustomeLog.Write_Log(logFilePath,"Clear List of event files")
                  List_of_Uris.clear()

  EventCommitCounter=0
                  CustomeLog.Write_Log(logFilePath,"START:: Going to update records for event"+ tmpbraze_event_nm)
                  UpdateSql="UPDATE "+BrazeConfigTableName+"  SET last_event_read_utc_dts=CAST(\'" + datetime.strftime(tmpLatestBlobDatetime, '%Y-%m-%d %H:%M:%S.%f') +"\' AS DATETIME) WHERE braze_event_nm=\'" + tmpbraze_event_nm  +"\' and is_active_ind=true;"
                  print(UpdateSql)
                  CustomeLog.Write_Log(logFilePath,"Update SQL - "+UpdateSql)
                  try:                       
                      UpdateQueryJob=bigquery_client.query(UpdateSql)
                      LoadAvroresults = UpdateQueryJob.result()  #Not working, - Commented
                  except Exception as e: 

 try:
                          print("Retrying update as first try failed after 1 seconds")
                          CustomeLog.Write_Log(logFilePath,"Retrying update as first try failed after 1 seconds")
                          time.sleep(1)                          
                          UpdateQueryJob=bigquery_client.query(UpdateSql)
                          LoadAvroresults = UpdateQueryJob.result()  #Not working, - Commented
                      except Exception as e:

 try:
                              print("Retrying update as Second try failed after 3 seconds")
                              CustomeLog.Write_Log(logFilePath,"Retrying update as Second try failed after 3 seconds")
                              time.sleep(3)
                              UpdateQueryJob=bigquery_client.query(UpdateSql)
                              LoadAvroresults = UpdateQueryJob.result()  #Not working, - Commented
                          except Exception as e:

try:
                                  print("Retrying update as Third try failed after 5 seconds")
                                  CustomeLog.Write_Log(logFilePath,"Retrying update as Third try failed after 5 seconds")
                                  time.sleep(5)
                                  UpdateQueryJob=bigquery_client.query(UpdateSql)
                                  LoadAvroresults = UpdateQueryJob.result()  #Not working, - Commented
                              except Exception as e: 
                                  print("Error in update bigquery table ::"+ str(e))
                                  CustomeLog.Write_ErrorLog(logFilePath,"Error in update bigquery table :: "+str(e))
                  

CustomeLog.Write_Log(logFilePath,"SUCCESS:: Update")
                  
                  if(tmpis_etl_metadata_created_ind):
                     time.sleep(20) 
                     UpdateETLtimestampSQL="UPDATE `"+tmpbq_stg_table_nm +"` SET etl_created_dts=CAST(\'" + datetime.strftime(CurrentDateTime, '%Y-%m-%d %H:%M:%S.%f') +"\' AS DATETIME) WHERE etl_created_dts IS NULL;"
                     CustomeLog.Write_Log(logFilePath,"START:: updating etl_created_dts ")
                     CustomeLog.Write_Log(logFilePath,"UpdateETLtimestampSQL - "+UpdateETLtimestampSQL)
                     LoadETLtimestampJob=bigquery_client.query(UpdateETLtimestampSQL)
                     LoadETLtimestampResult=LoadETLtimestampJob.result()
                     CustomeLog.Write_Log(logFilePath,"SUCCESS:: updated")
                     print("etl_created_dts updated for table : "+tmpbq_stg_table_nm)


 #Ingest Event File to BQ STG table 
      if(len(List_of_Uris)>0):
          CustomeLog.Write_Log(logFilePath,"****************START:: Ingestion Start for below files:: {}".format(' :: '.join(List_of_Uris)))
          load_job = bigquery_client.load_table_from_uri(List_of_Uris, tmpbq_stg_table_nm,job_config=job_config)           
          CustomeLog.Write_Log(logFilePath,"SUCCESS:: Ingestion")
      else:
          CustomeLog.Write_Log(logFilePath,"****************No New Event to load ")

 #Update Last timestamp for that DB
      CustomeLog.Write_Log(logFilePath,"START:: Going to update records for event"+ tmpbraze_event_nm)
      
      #print(tmpevent_last_crawl_dts)
      #print(tmpLatestBlobDatetime)
      if(tmpevent_last_crawl_dts > datetime.strptime(datetime.strftime(tmpLatestBlobDatetime,'%Y-%m-%d %H:%M:%S.%f'),'%Y-%m-%d %H:%M:%S.%f')): 
         tmpLatestBlobDatetime=tmpevent_last_crawl_dts
      UpdateSql="UPDATE "+BrazeConfigTableName+"  SET last_event_read_utc_dts=CAST(\'" + datetime.strftime(tmpLatestBlobDatetime, '%Y-%m-%d %H:%M:%S.%f') +"\' AS DATETIME) WHERE braze_event_nm=\'" + tmpbraze_event_nm  +"\' and is_active_ind=true;"            
      CustomeLog.Write_Log(logFilePath,"Update SQL - "+UpdateSql)

  try:
                                 
          UpdateQueryJob=bigquery_client.query(UpdateSql)
          LoadAvroresults = UpdateQueryJob.result()  #Not working, - Commented
      except Exception as e: 
          try:
              print("Retrying update as first try failed after 1 seconds")
              CustomeLog.Write_Log(logFilePath,"Retrying update as first try failed after 1 seconds")
              time.sleep(1)                          
              UpdateQueryJob=bigquery_client.query(UpdateSql)
              LoadAvroresults = UpdateQueryJob.result()  #Not working, - Commented
          except Exception as e:

 try:
                  print("Retrying update as Second try failed after 3 seconds")
                  CustomeLog.Write_Log(logFilePath,"Retrying update as Second try failed after 3 seconds")
                  time.sleep(3)
                  UpdateQueryJob=bigquery_client.query(UpdateSql)
                  LoadAvroresults = UpdateQueryJob.result()  #Not working, - Commented
              except Exception as e:

try:
                      print("Retrying update as Third try failed after 5 seconds")
                      CustomeLog.Write_Log(logFilePath,"Retrying update as Third try failed after 5 seconds")
                      time.sleep(5)
                      UpdateQueryJob=bigquery_client.query(UpdateSql)
                      LoadAvroresults = UpdateQueryJob.result()  #Not working, - Commented
                  except Exception as e: 
                      print("Error in update bigquery table ::"+ str(e))
                      CustomeLog.Write_ErrorLog(logFilePath,"Error in update bigquery table :: "+str(e))
          

CustomeLog.Write_Log(logFilePath,"SUCCESS:: Update")
      CustomeLog.Write_Log(logFilePath,"Number of File loaded:: "+str(RecordCount) )
      #Innser For loop done
      print(str(RecordCount) + " "+ tmpbraze_event_nm + " Events Loaded")
      CustomeLog.Write_Log(logFilePath,str(RecordCount) + " "+ tmpbraze_event_nm + " Events Loaded")
      

 print("tmpis_etl_metadata_created_ind "+ str(tmpis_etl_metadata_created_ind))
      #Check if Table exists 
      is_STG_Table_Exists = False
      #time.sleep(60) 
      CustomeLog.Write_Log(logFilePath,"START:: Checkign if STG table exists - "+tmpbq_stg_table_nm)
      CheckTableSQL ="SELECT true from "+ tmpbq_stg_table_nm.split('.')[0]+"."+ tmpbq_stg_table_nm.split('.')[1]+".INFORMATION_SCHEMA.TABLES WHERE table_name='"+ tmpbq_stg_table_nm.split('.')[2]+"';"
      CustomeLog.Write_Log(logFilePath,"CheckTableSQL - "+CheckTableSQL) 
      CheckTableQueryJob=bigquery_client.query(CheckTableSQL)  
      CheckTableQueryResults=CheckTableQueryJob.result() 

 for row in CheckTableQueryResults:
         is_STG_Table_Exists=True  
         CustomeLog.Write_Log(logFilePath,"SUCCESS:: Table Exists - "+tmpbq_stg_table_nm)
      if((not tmpis_etl_metadata_created_ind) and is_STG_Table_Exists ):        
         print("****************Adding additional column")
         CustomeLog.Write_Log(logFilePath,"START:: Alter Table to add etl timestamp column -etl_created_dts")
         AlterSTGtableSQL="ALTER TABLE `"+ tmpbq_stg_table_nm+"` ADD COLUMN etl_created_dts datetime;"
         CustomeLog.Write_Log(logFilePath,"AlterSTGtableSQL - "+AlterSTGtableSQL)   
         AlterSTGTableQueryJob=bigquery_client.query(AlterSTGtableSQL)  
         AlterSTGTableQueryResults=AlterSTGTableQueryJob.result() 
         CustomeLog.Write_Log(logFilePath,"SUCCESS:: Alter Table")

CustomeLog.Write_Log(logFilePath,"START:: Revert is_etl_metadata_created_ind to TRUE for table "+tmpbq_stg_table_nm)
         RevertFlagSQL="UPDATE "  +BrazeConfigTableName +" SET is_etl_metadata_created_ind=True WHERE bq_stg_table_nm='"+ tmpbq_stg_table_nm +"';"    
         CustomeLog.Write_Log(logFilePath,"RevertFlagSQL - "+RevertFlagSQL)   
         RevertFlagQueryJob=bigquery_client.query(RevertFlagSQL)  
         RevertFlagQueryResult=RevertFlagQueryJob.result() 
         CustomeLog.Write_Log(logFilePath,"SUCCESS:: Reverted")
         print("Adding additional column - Done")

#UpdateETLtimestampSQL="UPDATE `"+tmpbq_stg_table_nm +"` SET etl_created_dts=CURRENT_DATETIME('UTC') WHERE etl_created_dts IS NULL;"
         UpdateETLtimestampSQL="UPDATE `"+tmpbq_stg_table_nm +"` SET etl_created_dts=CAST(\'" + datetime.strftime(CurrentDateTime, '%Y-%m-%d %H:%M:%S.%f') +"\' AS DATETIME) WHERE etl_created_dts IS NULL;"
         CustomeLog.Write_Log(logFilePath,"START:: updating etl_created_dts ")
         CustomeLog.Write_Log(logFilePath,"UpdateETLtimestampSQL - "+UpdateETLtimestampSQL)
         #time.sleep(60)  
         LoadETLtimestampJob=bigquery_client.query(UpdateETLtimestampSQL)
         LoadETLtimestampResult=LoadETLtimestampJob.result()
         CustomeLog.Write_Log(logFilePath,"SUCCESS:: updated")
         print("etl_created_dts updated for table : "+tmpbq_stg_table_nm)

  else : 
         print("****************additional column already presents")
      if(is_STG_Table_Exists):
         #UpdateETLtimestampSQL="UPDATE `"+tmpbq_stg_table_nm +"` SET etl_created_dts=CURRENT_DATETIME('UTC') WHERE etl_created_dts IS NULL;"
         UpdateETLtimestampSQL="UPDATE `"+tmpbq_stg_table_nm +"` SET etl_created_dts=CAST(\'" + datetime.strftime(CurrentDateTime, '%Y-%m-%d %H:%M:%S.%f') +"\' AS DATETIME) WHERE etl_created_dts IS NULL;"
         CustomeLog.Write_Log(logFilePath,"START:: updating etl_created_dts ")
         CustomeLog.Write_Log(logFilePath,"UpdateETLtimestampSQL - "+UpdateETLtimestampSQL)
         time.sleep(20) 
         LoadETLtimestampJob=bigquery_client.query(UpdateETLtimestampSQL)
         LoadETLtimestampResult=LoadETLtimestampJob.result()
         CustomeLog.Write_Log(logFilePath,"SUCCESS:: updated")
         print("etl_created_dts updated for table : "+tmpbq_stg_table_nm)

 CustomeLog.Write_Log(logFilePath,"Event Load Start ::"+datetime.strftime(EventLoadStartTimeStamp, '%Y-%m-%d %H:%M:%S.%f'))
      CustomeLog.Write_Log(logFilePath,"Event Load End ::"+datetime.strftime(datetime.now(tz), '%Y-%m-%d %H:%M:%S.%f'))
      
except Exception as e:
   print("Error in ingestion, Archive file ::"+ str(e))
   CustomeLog.Write_ErrorLog(logFilePath,"Error in ingestion, Archive file ::"+str(e)) 

try : 
   print("START :Calling Function to upload Log File to Blob :: "+ logFilePath)
   CustomeLog.Write_Log(logFilePath,"****************Run Success - Exit*******************")    
   CustomeLog.Write_Log(logFilePath,"START :Calling Function to upload Log File to Blob :: "+ logFilePath)
   CustomeLog.Upload_Log_File(logFileName, logFilePath)
   print("SUCCESS:: Log File uploaded :: "+ logFilePath)
   #Log_bucket = storage_client.bucket(ETLLogBucket)
   #LogBlob=Log_bucket.blob("BrazeSTGIngestionLOG/"+logFileName)
   #LogBlob.upload_from_filename(logFilePath)

except Exception as e:
   print("Error in uploading blob file ::"+str(e))
   CustomeLog.Write_ErrorLog(logFilePath,"Error in uploading blob file ::" +str(e))
print("****************Run Success - Exit*******************")
sys.exit(0)
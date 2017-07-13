# import os
# import boto3
# import botocore
# import io
# import zipfile
# import pandas as pd
# import datetime
# import shutil
# import sys
# import logging
# import rds_config
# import pymysql
# import csv
#
# def recogniseCelebs(srcBucket,srcKey):
#     client = boto3.client('rekognition')
#     resource = boto3.resource('s3')
#     s3 = boto3.resource('s3')
#     localFilename = '/tmp/{}'.format(os.path.basename(srcKey))
#     try:
#         s3.Bucket(srcBucket).download_file(srcKey,localFilename)
#     except botocore.exceptions.ClientError as e:
#         if e.response['Error']['Code'] == "404":
#             print("The object does not exist.")
#         else:
#             raise
#     with zipfile.ZipFile(localFilename) as zip_file:
#         for member in zip_file.namelist():
#             filename = os.path.basename(member)
#             # skip directories
#             if not filename:
#                 continue
#
#             # copy file (taken from zipfile's extract)
#             source = zip_file.open(member)
#             target = file(os.path.join('/tmp/', filename), "wb")
#             with source, target:
#                 shutil.copyfileobj(source, target)
#
#     #Create DataFrame
#     colNames = ['VideoName','ImageName','ISO','TimeStamp','Celebrities','MatchConfidence']
#     df = pd.DataFrame(columns=colNames)
#
#     try:
#         #Getting video name from the path of zip file with images
#         videoName = str(srcKey.rsplit("/",2)[1])
#         for imgFile in os.listdir('/tmp/'):
#             if imgFile.find("img") != -1:
#                 with open('/tmp/' + imgFile, "rb") as imageFile:
#                     slicedImage = imageFile.read()
#                     imgBytes = bytearray(slicedImage)
#                     response = client.recognize_celebrities(
#                         Image={
#                             'Bytes': imgBytes,
#                         }
#                     )
#                     #To add millisec to the timeformat
#                     mili = '000'
#                     ID = os.path.basename(imgFile)
#                     imageName = format(os.path.basename(imgFile))
#
#                     #Get the seconds information of the frame
#                     time = float(ID.split("_",1)[0]) - 1
#                     iso = str(datetime.timedelta(seconds=time))
#                     strTime = iso.rsplit(".",1)[0]
#                     try:
#                         mili = iso.rsplit(".",1)[1]
#                         mili = mili[:-3]
#                     except:
#                         pass
#                     iso = strTime+":"+mili
#                     print imageName
#                     for i in range (0,len(response['CelebrityFaces'])):
#                         celebName = response['CelebrityFaces'][i]['Name']
#                         confidence = float(response['CelebrityFaces'][i]['MatchConfidence'])
#                         df_toAppend = pd.DataFrame([[videoName,imageName,iso,time,celebName.encode('utf-8'),confidence]],columns=colNames)
#                         df = df.append(df_toAppend)
#     except e:
#         print e
#
#     df.reset_index(inplace=True,drop=True)
#     resultFileName = '/tmp/AWS_' + videoName + '_result.csv'
#     f = open("%s"%resultFileName,"w+")
#     f.close()
#     df.to_csv("%s"%resultFileName)
#     videoName = videoName.split(".")[0]
#     name = str(srcKey.rsplit("/",1)[0]+ "/" + videoName + "_" + "AWS_result.csv")
#     object = s3.Bucket(srcBucket).put_object(Body = open(resultFileName), Key = name)
#     print 'AWS_result.csv created and uploaded to s3'
#     insertIntoTable(resultFileName)
#
# def insertIntoTable(csvResult):
#     #rds settings
#     rds_host  = rds_config.db_endpoint
#     name = rds_config.db_username
#     password = rds_config.db_password
#     db_name = rds_config.db_name
#
#     logging.basicConfig()
#     logger = logging.getLogger()
#     logger.setLevel(logging.INFO)
#
#     try:
#         conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
#     except Exception as e:
#         print e
#         logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
#         sys.exit()
#
#     logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
#     csv_data = csv.reader(file(csvResult))
#     i=0
#     with conn.cursor() as cur:
#         for row in csv_data:
#             i = i+1
#             if i!=1:
#                 try:
#                     print row
#                     cur.execute("INSERT INTO AWSResults VALUES(%s, %s, %s, %s, %s, %s, %s)",row)
#                 except Exception as e:
#                     if str(type(e).__name__)=='ProgrammingError' and str(e.args[0])=='1146':
#                         cur.execute("create table AWSResults ( ColIndex BIGINT,VideoName VARCHAR(255),ImageName VARCHAR(255),ISO VARCHAR(255), TimeSt FLOAT,Celebrities VARCHAR(255),MatchConfidence FLOAT)")
#                         cur.execute("INSERT INTO AWSResults VALUES(%s, %s, %s, %s, %s, %s, %s)",row)
#                     print e
#         #close the connection to the database.
#         conn.commit()
#         cur.close()
#
# if __name__ == '__main__':
#     srcBucket = os.environ.get('BUCKET', False)
#     srcKey = os.environ.get('FILE', False)
#
#     if srcBucket == False or srcKey == False:
#         print "Must Provide ${BUCKET} and ${FILE}"
#         sys.exit()
#     recogniseCelebs(srcBucket, srcKey)

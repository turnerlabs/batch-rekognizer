import os
import boto3
import botocore
import io
import zipfile
import pandas as pd
import datetime
import shutil
import sys
import logging
import rds_config
import pymysql
import csv

def batchRekognizer(srcKey,srcBucket):
    rekognition = boto3.client('rekognition')
    s3 = boto3.resource('s3')
    localFilename = '/tmp/{}'.format(os.path.basename(srcKey))
    conn = RDSconnection()

    try:
        s3.Bucket(srcBucket).download_file(srcKey,localFilename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            print e
            raise e

    with zipfile.ZipFile(localFilename) as zip_file:
        for member in zip_file.namelist():
            filename = os.path.basename(member)
            # skip directories
            if not filename:
                continue

            # copy file (taken from zipfile's extract)
            source = zip_file.open(member)
            target = file(os.path.join('/tmp/', filename), "wb")
            with source, target:
                shutil.copyfileobj(source, target)

    try:
        videoName = str(srcKey.rsplit("/",2)[1])
        videoName = videoName.split(".")[0]
        for imgFile in os.listdir('/tmp/'):
            if imgFile.find("img") != -1:
                with open('/tmp/' + imgFile, "rb") as imageFile:
                    slicedImage = imageFile.read()
                    imgBytes = bytearray(slicedImage)
                    mili = '000'
                    ID = os.path.basename(imgFile)
                    imageName = format(os.path.basename(imgFile))
                    #Get the seconds information of the frame
                    time = float(ID.split("_",1)[0]) - 1
                    iso = str(datetime.timedelta(seconds=time))
                    strTime = iso.rsplit(".",1)[0]
                    try:
                        mili = iso.rsplit(".",1)[1]
                        mili = mili[:-3]
                    except:
                        pass
                    iso = strTime+":"+mili
                    celebFileName = recogniseCelebs(rekognition,imgBytes,videoName,imageName,iso,time,conn)
                    labelFileName = getLabels(rekognition,imgBytes,videoName,imageName,iso,time,conn)
    except Exception as e:
        print e

    conn.commit()
    conn.close()

    object = s3.Bucket(srcBucket).put_object(Body = open(celebFileName), Key="/videos/" + videoName + "_AWS_celebs.csv")
    print 'AWS_celebs_result.csv created and uploaded to s3'
    object = s3.Bucket(srcBucket).put_object(Body = open(labelFileName), Key="/videos/" + videoName + "_AWS_labels.csv")
    print 'AWS_labels_result.csv created and uploaded to s3'

def RDSconnection():
    rds_host  = rds_config.db_endpoint
    name = rds_config.db_username
    password = rds_config.db_password
    db_name = rds_config.db_name

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    except Exception as e:
        print e
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()

    logger.info("SUCCESS: Connection to RDS mysql instance succeeded")
    return conn

def recogniseCelebs(rekognition,imgBytes,videoName,imageName,iso,time,conn):
    colNames = ['VideoName','ImageName','ISO','TimeStamp','Celebrities','MatchConfidence','HeightBox','LeftBox','TopBox','WidthBox']
    df = pd.DataFrame(columns=colNames)
    curr = conn.cursor()
    fileName = '/tmp/AWS_' + videoName + '_Celebrity_result.csv'
    try:
        response = rekognition.recognize_celebrities(
            Image={
                'Bytes': imgBytes,
            }
        )
        for i in range(0,len(response['CelebrityFaces'])):
            celebName = response['CelebrityFaces'][i]['Name'].encode('utf-8')
            confidence = float(response['CelebrityFaces'][i]['MatchConfidence'])
            height = float(response['CelebrityFaces'][i]['Face']['BoundingBox']['Height'])
            left = float(response['CelebrityFaces'][i]['Face']['BoundingBox']['Left'])
            top= float(response['CelebrityFaces'][i]['Face']['BoundingBox']['Top'])
            width= float(response['CelebrityFaces'][i]['Face']['BoundingBox']['Width'])
            df_toAppend = pd.DataFrame([[videoName,imageName,iso,time,celebName, \
                                         confidence,height,left,top,width]],\
                                         columns=colNames)
            df = df.append(df_toAppend)

        df.reset_index(inplace=True,drop=True)
        if os.path.exists("%s"%fileName):
            with open("%s"%fileName,"a") as f:
                df.to_csv(f,header=False,index=False)
        else:
            with open("%s"%fileName,"w+") as f:
                df.to_csv(f,header=True,index=False)

        for i in range(0,len(df.index)):
            try:
                curr.execute("INSERT INTO AWSCelebResults VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",\
                             (df.iloc[i]["VideoName"],\
                             df.iloc[i]["ImageName"],\
                             df.iloc[i]["ISO"],\
                             float(df.iloc[i]["TimeStamp"]),\
                             df.iloc[i]["Celebrities"],\
                             float(df.iloc[i]["MatchConfidence"]),\
                             float(df.iloc[i]["HeightBox"]),\
                             float(df.iloc[i]["LeftBox"]),\
                             float(df.iloc[i]["TopBox"]),\
                             float(df.iloc[i]["WidthBox"])))
            except Exception as e:
                print e.args[1]
                if str(type(e).__name__)=='ProgrammingError' and str(e.args[0])=='1146':
                    curr.execute("create table AWSCelebResults ( VideoName VARCHAR(255),ImageName VARCHAR(255),ISO VARCHAR(255)," + \
                                 "TimeStamp FLOAT,Celebrities VARCHAR(255),MatchConfidence FLOAT,HeightBox FLOAT,LeftBox FLOAT," + \
                                  "TopBox FLOAT,WidthBox FLOAT, PRIMARY KEY (VideoName,TimeStamp,Celebrities))")
                    curr.execute("INSERT INTO AWSCelebResults VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",\
                                  (df.iloc[i]["VideoName"],df.iloc[i]["ImageName"],df.iloc[i]["ISO"],\
                                  float(df.iloc[i]["TimeStamp"]),df.iloc[i]["Celebrities"],\
                                  float(df.iloc[i]["MatchConfidence"]),\
                                  float(df.iloc[i]["HeightBox"]),\
                                  float(df.iloc[i]["LeftBox"]),\
                                  float(df.iloc[i]["TopBox"]),\
                                  float(df.iloc[i]["WidthBox"])))
    except Exception as e:
        print e

    return fileName

def getLabels(rekognition,imgBytes,videoName,imageName,iso,time,conn):
    colNames = ['VideoName','ImageName','ISO','TimeStamp','Labels','Confidence']
    df = pd.DataFrame(columns=colNames)
    curr = conn.cursor()
    fileName = '/tmp/AWS_' + videoName + '_Labels_result.csv'
    try:
        response = rekognition.detect_labels(
            Image={
                'Bytes': imgBytes,
            }
        )
        for i in range(0,len(response['Labels'])):
            label = response['Labels'][i]['Name'].encode('utf-8')
            confidence = float(response['Labels'][i]['Confidence'])
            df_toAppend = pd.DataFrame([[videoName,imageName,iso,time,label,confidence]],columns=colNames)
            df = df.append(df_toAppend)

        df.reset_index(inplace=True,drop=True)
        if os.path.exists("%s"%fileName):
            with open("%s"%fileName,"a") as f:
                df.to_csv(f,header=False,index=False)
        else:
            with open("%s"%fileName,"w+") as f:
                df.to_csv(f,header=True,index=False)


        for i in range(0,len(df.index)):
            try:
                curr.execute("INSERT INTO AWSLabelResults VALUES(%s, %s, %s, %s, %s, %s)",\
                             (df.iloc[i]["VideoName"],df.iloc[i]["ImageName"],\
                             df.iloc[i]["ISO"],float(df.iloc[i]["TimeStamp"]),\
                             df.iloc[i]["Labels"],float(df.iloc[i]["Confidence"])))
            except Exception as e:
                print e.args[1]
                if str(type(e).__name__)=='ProgrammingError' and str(e.args[0])=='1146':
                    curr.execute("create table AWSLabelResults ( VideoName VARCHAR(255),ImageName" + \
                                 "VARCHAR(255),ISO VARCHAR(255), TimeStamp FLOAT,Labels VARCHAR(255),Confidence FLOAT, " + \
                                 "PRIMARY KEY (VideoName,TimeStamp,Labels))")
                    curr.execute("INSERT INTO AWSLabelResults VALUES(%s, %s, %s, %s, %s, %s)", \
                                 (df.iloc[i]["VideoName"],df.iloc[i]["ImageName"],df.iloc[i]["ISO"], \
                                 float(df.iloc[i]["TimeStamp"]), \
                                 df.iloc[i]["Labels"], \
                                 float(df.iloc[i]["Confidence"])))
    except Exception as e:
        print e

    return fileName

if __name__ == '__main__':
    srcBucket = os.environ.get('BUCKET', False)
    srcKey = os.environ.get('FILE', False)

    if srcBucket == False or srcKey == False:
        print "Must Provide ${BUCKET} and ${FILE}"
        sys.exit()

    batchRekognizer(srcKey,srcBucket)

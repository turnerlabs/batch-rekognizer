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
    try:
        s3.Bucket(srcBucket).download_file(srcKey,localFilename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
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
                    recogniseCelebs(rekognition,imgBytes,videoName,imageName,iso,time)
                    getLabels(rekognition,imgBytes,videoName,imageName,iso,time)
    except Exception as e:
        print e

def recogniseCelebs(rekognition,imgBytes,videoName,imageName,iso,time):
    colNames = ['VideoName','ImageName','ISO','TimeStamp','Celebrities','MatchConfidence']
    df = pd.DataFrame(columns=colNames)
    response = rekognition.recognize_celebrities(
        Image={
            'Bytes': imgBytes,
        }
    )
    for i in range(0,len(response['CelebrityFaces'])):
        celebName = response['CelebrityFaces'][i]['Name']
        confidence = float(response['CelebrityFaces'][i]['MatchConfidence'])
        df_toAppend = pd.DataFrame([[videoName,imageName,iso,time,celebName.encode('utf-8'),confidence]],columns=colNames)
        df = df.append(df_toAppend)

    df.reset_index(inplace=True,drop=True)
    fileName = '/tmp/AWS_' + videoName + '_Celebrity_result.csv'
    print fileName
    if os.path.exists("%s"%fileName):
        with open("%s"%fileName,"a") as f:
            df.to_csv(f,header=False,index=False)
    else:
        with open("%s"%fileName,"w+") as f:
            df.to_csv(f,header=True,index=False)

def getLabels(rekognition,imgBytes,videoName,imageName,iso,time):
    colNames = ['VideoName','ImageName','ISO','TimeStamp','Labels','Confidence']
    df = pd.DataFrame(columns=colNames)
    response = rekognition.detect_labels(
        Image={
            'Bytes': imgBytes,
        }
    )
    for i in range(0,len(response['Labels'])):
        label = response['Labels'][i]['Name']
        confidence = float(response['Labels'][i]['Confidence'])
        df_toAppend = pd.DataFrame([[videoName,imageName,iso,time,label,confidence]],columns=colNames)
        df = df.append(df_toAppend)

    df.reset_index(inplace=True,drop=True)
    fileName = '/tmp/AWS_' + videoName + '_Labels_result.csv'
    print fileName
    if os.path.exists("%s"%fileName):
        with open("%s"%fileName,"a") as f:
            df.to_csv(f,header=False,index=False)
    else:
        with open("%s"%fileName,"w+") as f:
            df.to_csv(f,header=True,index=False)

if __name__ == '__main__':
    srcBucket = os.environ.get('BUCKET', False)
    srcKey = os.environ.get('FILE', False)

    if srcBucket == False or srcKey == False:
        print "Must Provide ${BUCKET} and ${FILE}"
        sys.exit()

    batchRekognizer(srcKey,srcBucket)

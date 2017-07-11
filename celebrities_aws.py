import os
import boto3
import botocore
import io
import zipfile
import pandas as pd
import datetime
import shutil

def recogniseCelebs(srcBucket,srcKey):
    client = boto3.client('rekognition')
    resource = boto3.resource('s3')
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

    #Create DataFrame
    colNames = ['VideoName','ImageName','ISO','TimeStamp','Celebrities','MatchConfidence']
    df = pd.DataFrame(columns=colNames)

    #Getting video name from the path of zip file with images
    videoName = str(srcKey.rsplit("/",2)[1])
    for imgFile in sorted(os.listdir('/tmp/')):
        if imgFile.find("img") != -1:
            with open('/tmp/' + imgFile, "rb") as imageFile:
                slicedImage = imageFile.read()
                imgBytes = bytearray(slicedImage)
                response = client.recognize_celebrities(
                    Image={
                        'Bytes': imgBytes,
                    }
                )
                #To add millisec to the timeformat
                mili = '000'
                ID = os.path.basename(imgFile)
                imageName = format(os.path.basename(imgFile))

                #Get the seconds information of the frame
                time = float(ID.split("_",1)[0])
                iso = str(datetime.timedelta(seconds=time))
                strTime = iso.rsplit(".",1)[0]
                try:
                    mili = iso.rsplit(".",1)[1]
                    mili = mili[:-3]
                except:
                    pass
                iso = strTime+":"+mili
                print imageName
                for i in range (0,len(response['CelebrityFaces'])):
                    celebName = response['CelebrityFaces'][i]['Name']
                    confidence = float(response['CelebrityFaces'][i]['MatchConfidence'])
                    df_toAppend = pd.DataFrame([[videoName,imageName,iso,time,celebName,confidence]],columns=colNames)
                    df = df.append(df_toAppend)
    df.reset_index(inplace=True,drop=True)
    resultFileName = '/tmp/AWS_' + videoName + '_result.csv'
    f = open("%s"%resultFileName,"w+")
    f.close()
    df.to_csv("%s"%resultFileName)
    name = str(srcKey.rsplit("/",1)[0]+"/"+"AWS_result.csv")
    object = s3.Bucket(srcBucket).put_object(Body = open(resultFileName), Key = name)
    print 'AWS_result.csv created and uploaded to s3'


if __name__ == '__main__':
    srcBucket = str(os.environ.get('BUCKET'))
    srcKey = str(os.environ.get('FILE'))
    recogniseCelebs(srcBucket,srcKey)

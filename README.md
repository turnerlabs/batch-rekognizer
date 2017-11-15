# AWS Celebrity and Object Detection
## To run locally on mac:
Use the 'rds' branch of the repo and not the 'master' branch --
1. Chop up the images and upload the zip file in s3 using the Video-Slicer code present [here](https://github.com/turnerlabs/video-slicer).

In the ```celebrity_aws.py``` code set the ```BUCKET``` environment variable as the bucket containing the zipped images and ```FILE``` as the name of the video.

Note: To set an environment variable use command: ```export ENVVARIABLENAME = 'value'```

2. Create a rds database instance in AWS 
Set environment variables:
Set the ```DB_USERNAME``` as the username of the DB.
Set the ```DB_PASSWORD``` as the password of the DB.
Set the ```DB_NAME``` as the name you gave to the DB.
Set the ```DB_ENDPOINT``` as the endpoint value you get from AWS after creating the RDS instance.

Run the code using ```python rds_config.py```
Then run ```python dbconnect.py``` to connect to the DB.

Finally run the code using ```python celebrity_aws.py```
 
 
## Batch-rekognizer
Use all of AWS rekognitions apis in one simple Batch job, which feeds data back into a SQL table.



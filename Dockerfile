from turnerlabs/celebs_aws_base:0.2.0

ADD *.py /opt/app/

# run the API
CMD [ "python", "/opt/app/rekognizer.py" ]

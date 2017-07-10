from turnerlabs/celebs_aws_base

ADD celebrities_aws.py /opt/app/

# run the API 
CMD [ "python", "/opt/app/celebrities_aws.py" ]


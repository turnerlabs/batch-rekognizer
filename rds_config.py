import os

db_username =str(os.environ.get('DB_USERNAME'))
db_password = str(os.environ.get('DB_PASSWORD'))
db_name = str(os.environ.get('DB_NAME'))
db_endpoint = str(os.environ.get('DB_ENDPOINT'))

if db_username == False or db_password == False or db_name == False or db_endpoint == False:
    print "Must Provide ${DB_USERNAME} and ${DB_PASSWORD} and ${DB_NAME} and ${DB_ENDPOINT}"
    sys.exit()

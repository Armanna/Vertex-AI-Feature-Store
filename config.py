import os
from configparser import ConfigParser

#####################################################################################################################################
######################################################### GATHER  ALL  DATA #########################################################
#####################################################################################################################################

# We connect to our configuration file and read source and target databases' parameters. 
config = ConfigParser()
file = os.path.dirname(__file__) + '/gcpData.ini'
config.read(file)

# After reading from config file, we gather all the information needed for further run. 
# The config file contains 1 section: [GCP]
PROJECT_ID = config['GCP']['PROJECT_ID']
REGION = config['GCP']['REGION']
SOURCE_BUCKET = config['GCP']['SOURCE_BUCKET']
DESTINATION_BUCKET = config['GCP']['DESTINATION_BUCKET']
FS = config['GCP']['FS']
ONLINE_STORE_FIXED_NODE_COUNT = config['GCP']['ONLINE_STORE_FIXED_NODE_COUNT']

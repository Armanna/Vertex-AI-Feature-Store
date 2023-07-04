import warnings
warnings.filterwarnings('ignore')

import os
import sys
import time
import base64
import string
import random
import pyarrow
import pandas as pd
from pathlib import Path
from google.cloud import storage
from google.cloud import aiplatform
from configparser import ConfigParser
from datetime import datetime, timezone 
from google.cloud.aiplatform import Feature, Featurestore

sys.path.insert(1, str(Path.cwd()))
import config
import gcpCredentials

# Generate a uuid of a specifed length(default=8)
def generate_uuid(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))

def readFromGCS(SOURCE_BUCKET: str):
# Read images from GCS bucket and convert them from jpeg to string
# Then define DataFrame in put all the information there

    image_id = []
    image_byte = []
    update_time = []

    client = storage.Client()
    bucket = client.get_bucket(SOURCE_BUCKET)
    blobs = list(bucket.list_blobs())
    i = 1
    for blob in blobs:
        image_id.append('image-' + str(i))
        
        img_byte = blob.download_as_bytes()
        image_byte.append(img_byte)
        
        feature_time_str = datetime.now().isoformat(sep=" ", timespec="milliseconds")
        feature_time = datetime.strptime(feature_time_str, "%Y-%m-%d %H:%M:%S.%f")
        update_time.append(feature_time)
        
        i += 1
        
    imageDF = pd.DataFrame(list(zip(image_id, image_byte, update_time)), columns =['image_id', 'image_byte', 'update_time'])  
    
    print('===================================================================')
    print(f'==========> 1. Image dataframe is created with {len(imageDF)} rows')
    print('===================================================================')
    return imageDF

def FeatureStore(PROJECT_ID: str, REGION: str, FEATURESTORE_ID: str, ONLINE_STORE_FIXED_NODE_COUNT: int, source: pd.DataFrame):
    # Create Featurestore
    fs = Featurestore.create(
        featurestore_id=FEATURESTORE_ID,
        online_store_fixed_node_count=ONLINE_STORE_FIXED_NODE_COUNT,
        project=PROJECT_ID,
        location=REGION,
        sync=True,
    )

    print('============================================================')
    print('==========> 2.', FEATURESTORE_ID, 'feature store is created.')
    print('============================================================')

    # Create Images Entity Type
    images_entity_type = fs.create_entity_type(
        entity_type_id="images",
        description="Images entity",
    )

    print('=============================================')
    print('==========> 3. images entity type is created.')
    print('=============================================')


    image_byte = images_entity_type.create_feature(
        feature_id="image_byte",
        value_type="BYTES",
        description="image converted from jpeg to byte",
    )

    print('=============================================')
    print('==========> 4. image_byte feature is created.')
    print('=============================================')

    # Import to Feature Store from dataframe
    IMAGES_FEATURES_IDS = [feature.name for feature in images_entity_type.list_features()]
    IMAGES_FEATURE_TIME = "update_time"
    IMAGES_ENTITY_ID_FIELD = "image_id"
            
    # DATAFRAME
    images_entity_type.ingest_from_df(
        feature_ids = IMAGES_FEATURES_IDS,
        feature_time = IMAGES_FEATURE_TIME,
        df_source = source,
        entity_id_field = IMAGES_ENTITY_ID_FIELD,
    )

    print('===============================================')
    print('==========> 5. Batch ingestion process is done.')
    print('===============================================')

def onlineServing(PROJECT_ID: str, REGION: str, FEATURESTORE_ID: str, entity_ids: str):

    aiplatform.init(project=PROJECT_ID, location=REGION)
    my_entity_type = aiplatform.featurestore.EntityType(
            entity_type_name='images', featurestore_id=FEATURESTORE_ID
        )
    my_dataframe = my_entity_type.read(entity_ids=entity_ids, feature_ids='image_byte')

    print('========================================================')
    print(f'==========> 6. Computer randomly selected {entity_ids}.')
    print('========================================================')

    return my_dataframe

def backToBucket(df: pd.DataFrame, DESTINATION_BUCKET: str):

    imgByte = df['image_byte'][0]

    imgName = 'img' + df['entity_id'][0][6:] + '.jpg'

    f = open(imgName, 'wb')
    f.write(imgByte)
    f.close()

    client = storage.Client()
    bucket = client.get_bucket(DESTINATION_BUCKET)
    blob = bucket.blob(imgName)
    blob.upload_from_filename(imgName)

    print('==============================================================================================================')
    print(f'==========> 7. Randomly selected image is converted from bytes to jpg and uploaded into {DESTINATION_BUCKET}.')
    print('==============================================================================================================')
    print('==============================')
    print('==========> Just go and check!')
    print('==============================')
    
def deletFS(PROJECT_ID: str, REGION: str, FEATURESTORE_ID: str):

    aiplatform.init(project=PROJECT_ID, location=REGION)
    fs = aiplatform.featurestore.Featurestore(featurestore_name=FEATURESTORE_ID)
    fs.delete(sync=True, force=True)

    print('===========================================================')
    print('==========> 8.', FEATURESTORE_ID, 'feature store is deleted')
    print('===========================================================')
 

if __name__ == "__main__":

    #####################################################################################################################################
    ######################################################### GATHER  ALL  DATA #########################################################
    #####################################################################################################################################

    PROJECT_ID = config.PROJECT_ID
    REGION = config.REGION
    SOURCE_BUCKET = config.SOURCE_BUCKET
    DESTINATION_BUCKET = config.DESTINATION_BUCKET
    FS = config.FS
    ONLINE_STORE_FIXED_NODE_COUNT = int(config.ONLINE_STORE_FIXED_NODE_COUNT)

    #Generate Universal Unique Identifier for Feature Store ID
    UUID = generate_uuid()
    FEATURESTORE_ID = FS + UUID

    #####################################################################################################################################
    ######################################################### GOOGLE  CREDENTIALS #######################################################
    #####################################################################################################################################

    gcpCredentials.specifyGoogleCredentials()

    #####################################################################################################################################
    ######################################## READ  IMAGES  FROM  GCS,  CREATE  DATAFRAME ################################################
    #####################################################################################################################################

    df = readFromGCS(SOURCE_BUCKET)

    #####################################################################################################################################
    ######################################### CREATE  FEATURE  STORE, MANAGE BATCH INGESTION ############################################
    #####################################################################################################################################

    FeatureStore(PROJECT_ID, REGION, FEATURESTORE_ID, ONLINE_STORE_FIXED_NODE_COUNT, df)

    #####################################################################################################################################
    ########################################## ONLINE SERVING, READ A RANDOMLY SELECTED IMAGE ###########################################
    #####################################################################################################################################

    entity_ids = 'image-' + str(random.randint(1, 100))

    testDF = onlineServing(PROJECT_ID, REGION, FEATURESTORE_ID, entity_ids)

    #####################################################################################################################################
    ################################ DECODE A RANDOMLY SELECTED IMAGE AND PUT IT BACK INTO BUCKET #######################################
    #####################################################################################################################################

    backToBucket(testDF, DESTINATION_BUCKET)

    #####################################################################################################################################
    #################################################### DELETE FEATURE STORE ###########################################################
    #####################################################################################################################################

    deletFS(PROJECT_ID, REGION, FEATURESTORE_ID)

    print('=========================================================================')
    print('==========> Congratulations! Pipelines worked excellent! Have a nice day!')
    print('=========================================================================')

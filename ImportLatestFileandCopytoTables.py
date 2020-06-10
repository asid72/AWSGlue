import boto3
import pandas as pd
import io
import sys
from awsglue.utils import getResolvedOptions

# MapperFile = 'Scripts/ImportLatestFile/Mapper.csv' 
args = getResolvedOptions(sys.argv,['DATANAME','BUCKETNAME','MAPPER'])
prmDataName = args['DATANAME']
prmBucketName = args['BUCKETNAME']
MapperFile = args['MAPPER'] #put Location of mapper file in S3

def get_latest_file_name(bucket_name,prefix):
    """
    Return the latest file name in an S3 bucket folder.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (folder  name).
    """
    s3_client = boto3.client('s3')
    objs = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
    shortlisted_files = dict()            
    for obj in objs:
        key = obj['Key']
        timestamp = obj['LastModified']
        # if key starts with folder name retrieve that key
        if key.startswith(prefix):              
            # Adding a new key value pair
            shortlisted_files.update( {key : timestamp} )   
    latest_filename = max(shortlisted_files, key=shortlisted_files.get)
    print('Lastest File Name: ' + latest_filename)
    return latest_filename

def copy_paste(bucket,LastLocation,NewLocation):
    s3 = boto3.resource('s3')
    copy_source = {
    'Bucket': bucket,
    'Key': LastLocation
    }
    s3.meta.client.copy(copy_source, bucket, NewLocation)
    print('Copy and Paste Complete')



s3c = boto3.client('s3')
csv_obj = s3c.get_object(Bucket=prmBucketName, Key=MapperFile)
body = csv_obj['Body']
iniMapperdf = pd.read_csv(io.BytesIO(csv_obj['Body'].read()), encoding='utf8')
Mapperdf = iniMapperdf[iniMapperdf['DataName']==prmDataName]

for index, row in Mapperdf.iterrows():
    # Script the copy and pastes the file#######
    TableLocation = row['TableLocation']
    NewFileName = row['NewFileName']
    Path = row['Path']
    S3Bucket = prmBucketName
    latest_filename = get_latest_file_name(bucket_name=S3Bucket,prefix = Path)
    copy_paste(bucket=S3Bucket,LastLocation=latest_filename,NewLocation=(TableLocation + NewFileName))


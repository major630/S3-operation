import boto3
from boto3.session import Session
from botocore.client import Config
import sys
import os
import logging
import logging.handlers

# set s3 log 
s3_handler = logging.handlers.RotatingFileHandler('/var/log/s3.log', maxBytes=1024*1024, backupCount=5)
s3_fmt = '[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s][%(funcName)s] %(message)s'
s3_logFormat = logging.Formatter(s3_fmt)
s3_handler.setFormatter(s3_logFormat)

logger = logging.getLogger('s3')
logger.addHandler(s3_handler)
logger.setLevel(logging.DEBUG)


class s3server(object):
    """
    Encapsulation for s3 application
    For EC2: using role instead of key and secret
    For other sever: config key/secret/region in ./aws
    """
	
    s3_resource = None
    s3_client = None
	
    def __init__(self, stype='s3'):
        if self.s3_resource == None:
            self.s3_resource = boto3.resource(stype, config=Config(signature_version='s3v4'))
        if self.s3_client == None:
            self.s3_client = boto3.client(stype,config=Config(signature_version='s3v4'))
		
    def __del__(self):
        pass

    def isBucketExist(self, name):
        for bucket in self.s3_resource.buckets.all():
            if name == bucket.name:
                return True
        return False

    def createBucket(self, name, region):
        kwargs = {'Bucket': name}
        kwargs['CreateBucketConfiguration'] = {'LocationConstraint': region}
        bucket = self.s3_resource.create_bucket(**kwargs)
        return bucket

    def deleteBucket(self, name):
        if self.isBucketExist(name):
            self.s3_resource.Bucket(name).delete()

    def getBucket(self, name):
        return self.s3_resource.Bucket(name)
			
    def upload(self, bucket_name, file_path, key):
        if os.path.exists(file_path):
            try:
                self.s3_resource.Bucket(bucket_name).upload_file(Filename=file_path, 
                                                                 Key=key, 
                                                                 # below can add more info as you would
                                                                 ExtraArgs={'ACL': 'public-read'}) 
                logger.info("bucket_name=%s,file_path=%s,key=%s",bucket_name, file_path, key)
                return True
            except Exception, ex:
                logger.error("Exception: %s", ex)
                return False
        else:
            logger.error("file_path=%s, not exists", file_path)
            return False

    def download(self, bucket_name, path, key):
        file_path = ''
        if os.path.exists(path):
            if os.path.isdir(path):
                file_path = os.path.join(path,key)
            elif os.path.isfile(path):
                file_path = path
            else:
                logger.error("file_path=%s, invalid", path)
                return False
        else:
            os.makedirs(path)
	
            logger.info("bucket_name=%s,path=%s,key=%s",bucket_name, file_path, key)
            try:
                self.s3_resource.Object(bucket_name, key).download_file(file_path)
                return True
            except Exception,ex:
                logger.error("Exception: %s", ex)
                return False

    def deleteFile(self, bucket_name, key):
        self.s3_client.delete_object(Bucket=bucket_name, Key=key)
        logger.info("bucket_name=%s,key=%s",bucket_name, key)

    def getURL(self, bucket_name, key):
        urls = self.s3_client.generate_presigned_post(Bucket=bucket_name, 
                                                      Key=key,
                                                      Fields=None, 
                                                      Conditions=[{"acl": "public-read"}], 
                                                      ExpiresIn=60*60*24)
        url = urls['url'] + '/' + key
        return url

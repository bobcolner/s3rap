import logging
import os
import gzip
import boto3

s3resource = boto3.resource('s3')
s3client = boto3.client('s3')
logger = logging.getLogger(__name__)

def get_object(bucket, key):
    "Download S3 object into memory"
    data = s3resource.Object(bucket, key).get()["Body"].read()
    logger.debug('downlated object: {0}'.format(key))
    return data

def download_object(bucket, key, file_name=None):
    "Download S3 object to local file"
    if file_name is None:
        file_name = key.split('/')[-1]
    s3resource.Bucket(bucket).download_file(Key=key, Filename=file_name)
    logger.debug('downloaded object: {0}'.format(key))

def get_gzip_object(bucket, key):
    "Download and un-gzip S3 object into memory"
    data_gzip = get_object(bucket, key)
    return gzip.decompress(data_gzip).decode("utf-8")

def download_gzip_object(bucket, key):
    "Download and un-gzip S3 object to local file"
    download_object(bucket, key)
    return un_gzip(file_path)

def un_gzip(file_path):
    "Un-gzip local file"
    if file_path.endswith('.gz'):
        out_file = file_path.replace('.gz', '')
    else:
        out_file = file_path + '.un-gz'
    with gzip.open(file_path, 'rb') as gzip_f, open(out_file, 'wb') as out_f:
        data_b = gzip_f.read()
        out_f.write(data_b)
    return out_file

def upload_file(bucket, key, local_file):
    "Upload & gzip local file to S3"
    gzip_file = local_file + '.gz'
    with open(local_file, 'rb') as file_in, gzip.open(gzip_file, 'wb') as gzip_out:
        gzip_out.writelines(file_in)
    try:
        s3resource.Bucket(bucket).upload_file(
            Filename = gzip_file, 
            Key = key, 
            ExtraArgs = {
                'ContentType': 'application/x-gzip',
                'ServerSideEncryption': 'AES256'
            }
        )
    finally:
        logger.debug('uploaded object: {0}'.format(key))
        os.remove(gzip_file)

def copy_object(src_bucket, src_key, dst_bucket, dst_key):
    "Copy an object to a new S3 location"
    src_path = os.path.join(src_bucket, src_key)
    s3resource.Object(dst_bucket, dst_key).copy_from(CopySource=src_path)
    logger.debug('copying object from: {0} to:{1}'.format(src_key, dst_key))

def exists_object(bucket, key):
    "Check if an key exists in S3"
    results = s3client.list_objects(Bucket=bucket, Prefix=key)
    return 'Contents' in results

def list_buckets():
    "List accessable buckets"
    buckets = s3client.list_buckets()
    return [ b['Name'] for b in buckets['Buckets'] ]

def list_objects(bucket, prefix=''):
    "List objects in S3 bucket"
    bucket = s3resource.Bucket(bucket)
    results = []
    for obj in bucket.objects.filter(Prefix=prefix):
        obj_meta = {
            'key': obj.key, 
            'size_mb': round(obj.size / 1000000, 1), 
            'last_modified': obj.last_modified.strftime('%Y-%m-%dT%H:%M:%S')
        }
        results.append(obj_meta)
    return results

def create_bucket(bucket):
    "Create bucket"
    s3resource.create_bucket(Bucket=bucket)
    logger.debug('created bucket: {0}'.format(bucket))

def delete_bucket(bucket):
    "Delete bucket and all objects within"
    for obj_meta in list_objects(bucket):
        delete_object(bucket, key=obj_meta['key'])
    s3client.delete_bucket(Bucket=bucket)
    logger.debug('deleted bucket: {0}'.format(bucket))

def delete_object(bucket, key):
    "Delete an object"
    s3resource.Object(bucket, key).delete()
    logger.debug('deleted object: {0}'.format(key))

def generate_presigned_url(bucket, key, exp_secs=100):
    "Genereate a tempory auth-signed URL to access an S3 object"
    temp_url = s3client.generate_presigned_url(
        ClientMethod = 'get_object',
        Params = {'Bucket': bucket, 'Key': key},
        ExpiresIn = exp_secs
    )
    logger.debug('generated tempory url: {0} expires in: {1} seconds'.format(key, exp_secs))
    return temp_url

import os
import gzip
import boto3
import logging

_resource = boto3.resource('s3')
_client = boto3.client('s3')
_logger = logging.getLogger(__name__)

def delete_object(bucket, key):
    "Delete an object"
    _resource.Object(bucket, key).delete()
    _logger.info('deleted object: {0}'.format(key))

def get_object(bucket, key):
    "Download an object into memory"
    data = _resource.Object(bucket, key).get()["Body"].read()
    _logger.info('downlated object: {0}'.format(key))
    return data

def copy_object(src_bucket, src_key, dst_bucket, dst_key):
    "Copy an object to a new S3 location"
    src_path = os.path.join(src_bucket, src_key)
    _resource.Object(dst_bucket, dst_key).copy_from(CopySource=src_path)
    _logger.info('copying object from: {0} to:{1}'.format(src_key, dst_key))

def exists_object(bucket, key):
    "Check if an object exists in S3"
    results = _client.list_objects(Bucket=bucket, Prefix=key)
    return 'Contents' in results

def download_object(bucket, key, file_name=None):
    "Download object to local file"
    if file_name is None:
        file_name = key.split('/')[-1]
    _resource.Bucket(bucket).download_file(Key=key, Filename=file_name)
    _logger.info('downloaded object: {0}'.format(key))

def get_gzip_object(bucket, key):
    "Download and un-gzip object into memory"
    datagzip = get_object(bucket, key)
    data_b = gzip.decompress(data_gzip)
    _logger.info('downloaded gziped object: {0}'.format(key))
    return data_b.decode("utf-8")

def download_gzip_object(bucket, key):
    "Download and un-gzip object to local file"
    file_path = key.split('/')[-1]
    download_object(bucket, key, file_path)
    out_file = un_gzip(file_path)
    _logger.info('downloaded gziped object: {0}'.format(key))
    return out_file

def un_gzip(file_path):
    "Un-gzip local file"
    if file_path.endswith('.gz'):
        out_file = file_path.replace('.gz', '')
    else:
        out_file = 'decompressed--' + file_path
    with gzip.open(file_path, 'rb') as gzip_f, open(out_file, 'wb') as out_f:
        data_b = gzip_f.read()
        out_f.write(data_b)
    return out_file

def upload_file(bucket, key, local_file):
    "Upload local file to S3"
    gzip_file = local_file + '.gz'
    with open(local_file, 'rb') as file_in, gzip.open(gzip_file, 'wb') as gzip_out:
        gzip_out.writelines(file_in)
    try:
        _resource.Bucket(bucket).upload_file(Filename=gzip_file, Key=key, 
            ExtraArgs={'ContentType':'application/x-gzip', 'ServerSideEncryption':'AES256'})
    finally:
        _logger.info('uploaded object: {0}'.format(key))
        os.remove(gzip_file)

def list_buckets():
    "List accessable buckets"
    buckets = _client.list_buckets()
    return [ b['Name'] for b in buckets['Buckets'] ]

def create_bucket(bucket):
    "Create bucket"
    _resource.create_bucket(Bucket=bucket)
    _logger.info('created bucket: {0}'.format(bucket))

def delete_bucket(bucket):
    "Delete bucket"
    _client.delete_bucket(Bucket=bucket)
    _logger.info('deleted bucket: {0}'.format(bucket))

def list_objects(bucket, prefix=''):
    "List object in S3 bucket"
    bucket = _resource.Bucket(bucket)
    return [{'key': b.key, 
            'size_mb': round(b.size / 1000000, 1), 
            'last_modified': b.last_modified.strftime('%Y-%m-%dT%H:%M:%S')} 
        for b in bucket.objects.filter(Prefix=prefix)]

def gen_tmp_get_url(bucket, key, exp_secs=100):
    "Genereate a tempory URL to access an S3 object"
    temp_url = _client.generate_presigned_url('get_object',
        Params = {'Bucket': bucket, 'Key': key}, ExpiresIn=exp_secs)
    _logger.info('generated tempory url: {0} expires in: {1} seconds'.format(key, exp_secs))
    return temp_url

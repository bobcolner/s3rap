import os
import gzip
import boto3

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

def delete_object(bucket, key):
    "Delete an object"
    return s3_resource.Object(bucket, key).delete()

def get_object(bucket, key):
    "Download an object into memory"
    return s3_resource.Object(bucket, key).get()["Body"].read()

def copy_object(src_bucket, src_key, dst_bucket, dst_key):
    "Copy an object to a new S3 location"
    return s3_resource.Object(dst_bucket, dst_key).copy_from(CopySource=src_bucket+'/'+src_key)

def exists_object(key, bucket):
    "Check if an object exists in S3"
    results = s3_client.list_objects(Bucket=bucket, Prefix=key)
    return 'Contents' in results

def download_object(bucket, key, file_name=None):
    "Download object to local file"
    if file_name is None:
        file_name = key.split('/')[-1]
    s3_resource.Bucket(bucket).download_file(Key=key, Filename=file_name)

def get_gzip_object(bucket, key):
    "Download and un-gzip object into memory"
    data_gzip = get_object(bucket, key)
    data_b = gzip.decompress(data_gzip)
    return data_b.decode("utf-8")

def download_gzip_object(bucket, key):
    "Download and un-gzip object to local file"
    file_path = key.split('/')[-1]
    download_object(bucket, key, file_path)
    out_file = un_gzip(file_path)
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
        s3_resource.Bucket(bucket).upload_file(Filename=gzip_file, Key=key, 
            ExtraArgs={'ContentType':'application/x-gzip', 'ServerSideEncryption':'AES256'})
    finally:
        os.remove(gzip_file)

def list_buckets():
    "List accessable buckets"
    buckets = s3_client.list_buckets()
    return [b['Name'] for b in buckets['Buckets']]

def create_bucket(bucket):
    "Create bucket"
    return s3_resource.create_bucket(Bucket=bucket)

def delete_bucket(bucket):
    "Delete bucket"
    return s3_client.delete_bucket(Bucket=bucket)

def list_objects(bucket, prefix=''):
    "List object in S3 bucket"
    bucket = s3_resource.Bucket(bucket)
    return [{'key': b.key, 
            'size_mb': round(b.size / 1000000, 1), 
            'last_modified': b.last_modified.strftime('%Y-%m-%dT%H:%M:%S')} 
        for b in bucket.objects.filter(Prefix=prefix)]

def gen_tmp_get_url(bucket, key, exp_secs=100):
    "Genereate a tempory URL to access an S3 object"
    return s3_client.generate_presigned_url('get_object',
        Params={'Bucket': bucket, 'Key': key}, ExpiresIn=exp_secs)

import os
import gzip
import boto3

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

def delete_key(bucket, key):
    return s3_resource.Object(bucket, key).delete()

def get_key(bucket, key):
    return s3_resource.Object(bucket, key).get()["Body"].read()

def exists_key(key, bucket):
    bucket = s3_resource.Bucket(bucket)
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        return key

def download_key(bucket, key, file_name=None):
    if file_name is None:
        file_name = key.split('/')[-1]
    s3_resource.Bucket(bucket).download_file(Key=key, Filename=file_name)

def get_gzip_key(bucket, key):
    data_gzip = get_key(bucket, key)
    data_b = gzip.decompress(data_gzip)
    return data_b.decode("utf-8")

def download_gzip_key(bucket, key):
    file_path = key.split('/')[-1]
    download_key(bucket, key, file_path)
    out_file = un_gzip(file_path)
    return out_file

def un_gzip(file_path):
    if file_path.endswith('.gz'):
        out_file = file_path.replace('.gz', '')
    else:
        out_file = 'decompressed--' + file_path
    with gzip.open(file_path, 'rb') as gzip_f, open(out_file, 'wb') as out_f:
        data_b = gzip_f.read()
        out_f.write(data_b)
    return out_file

def upload_file(bucket, key, local_file):
    gzip_file = local_file + '.gz'
    with open(local_file, 'rb') as file_in, gzip.open(gzip_file, 'wb') as gzip_out:
        gzip_out.writelines(file_in)
    try:
        s3_resource.Bucket(bucket).upload_file(Filename=gzip_file, Key=key, 
            ExtraArgs={'ContentType':'application/x-gzip', 'ServerSideEncryption':'AES256'})
    finally:
        os.remove(gzip_file)

def list_buckets():
    buckets = s3_client.list_buckets()
    return [b['Name'] for b in buckets['Buckets']]

def create_bucket(bucket):
    return s3_resource.create_bucket(Bucket=bucket)

def del_bucket(bucket):
    return s3_client.delete_bucket(Bucket=bucket)

def list_keys(bucket, prefix=''):
    bucket = s3_resource.Bucket(bucket)
    return [{'key': b.key, 
            'size_mb': round(b.size / 1000000, 1), 
            'last_modified': b.last_modified.strftime('%Y-%m-%dT%H:%M:%S')} 
        for b in bucket.objects.filter(Prefix=prefix)]

def gen_tmp_get_url(bucket, key, exp_secs=100):
    return s3_client.generate_presigned_url('get_object',
        Params={'Bucket': bucket, 'Key': key}, ExpiresIn=exp_secs)

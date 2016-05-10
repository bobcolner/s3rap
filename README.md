# S3rap
AWS S3 convenience functions based on boto3.

#### features
- Simple functions to access key S3 capabilities
- Transparently gzip uploads/downloads from S3

#### install
```sh
pip install s3rap
```

#### examples
```py
from s3rap import s3rap
```

###### List S3 resources
```py
# list accessable buckets
s3rap.list_buckets()
# list (filter) objects in a bucket
s3rap.list_objects(bucket, prefix='')
```

###### Access S3 resource
```py
# download object to memory
s3rap.get_object(bucket, key)
# download object to local file
s3rap.download_object(bucket, key, file_name=None)
# download gziped resources
s3rap.get_gzip_object(bucket, key)
s3rap.download_gzip_object(bucket, key)
```
###### Upload S3 resource
```py
# upload & gzip local file
s3rap.upload_file(bucket, key, local_file)
```

###### Native AWS S3 copy
```py
s3rap.copy_object(src_bucket, src_key, dst_bucket, dst_key)
```

###### Delete S3 reources
```py
# delete bucket
s3rap.delete_bucket(bucket)
# delete object
s3rap.delete_object(bucket, key)
```

###### Generate tempory S3 authorazation URL
```py
s3rap.gen_tmp_get_url(bucket, key, exp_secs=100)
```

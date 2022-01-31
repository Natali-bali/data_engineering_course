import logging
from pyarrow import parquet as pq
from google.cloud import storage
from pyarrow import csv as pv


def format_to_parquet(src_file):
    # if not src_file.endwith('.csv'):
    #     logging.error("Can only accept files in CSV format")
    #     return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))
    print('formated successfully')



def upload_to_gcs(bucket_name, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    bucket: GCS bucket name
    object_name: target path & file-name
    local_file: source path & file-name
    """
   # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
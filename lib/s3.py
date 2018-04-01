import os
import boto3
import botocore
import json
import utils
import tempfile
from lib.logset import myLog
logger = myLog()


def get_s3_client():
    return boto3.client(service_name="s3", region_name=os.environ["AWS_REGION"], aws_access_key_id=os.environ["AWS_ACCESS_KEY"], aws_secret_access_key=os.environ["AWS_ACCESS_SECRET_KEY"])


def get_s3_resource():
    return boto3.resource(service_name="s3", region_name=os.environ["AWS_REGION"], aws_access_key_id=os.environ["AWS_ACCESS_KEY"], aws_secret_access_key=os.environ["AWS_ACCESS_SECRET_KEY"])


def upload_to_s3(filename, key):
    """
    Uploads the given file to the AWS S3 bucket and key (S3 filename) specified.

    Returns boolean indicating success/failure of upload.

    http://stackabuse.com/example-upload-a-file-to-aws-s3/
    """

    with open(filename, "r") as file:
        try:
            size = os.fstat(file.fileno()).st_size
        except:
            # Not all file objects implement fileno(),
            # so we fall back on this
            file.seek(0, os.SEEK_END)
            size = file.tell()

    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.
    get_s3_client().upload_file(filename, os.environ["AWS_BUCKET"], key)

    # Check the size of what we sent matches the size of what got there
    # s3 = boto3.resource("s3")
    object_summary = get_s3_resource().ObjectSummary(os.environ["AWS_BUCKET"], key)

    if object_summary.size != size:
        raise Exception("Failed uploading {} to S3 - size mismatch.".format(filename))


def create_and_upload_latest_json_metadata(zip_filename, s3_key, lastRefreshTime):
    """
    Creates a JSON file with key info about the zip file
    we're passed.

    Key info:
      - filename: The name of the file on S3
      - md5_checksum: The MD5 checksum of the file that was uploaded
      - slip_last_updated_ticks: A timestamp of the last time the dataset was refreshed in SLIP (Represented as a Microsoft Timestamp Thingo)
    """

    dataset_name, filename = os.path.split(s3_key)

    data = {
        "filename": filename,
        "md5_checksum": utils.md5_hash_file(zip_filename),
        "slip_last_updated_ticks": lastRefreshTime,
    }

    with tempfile.NamedTemporaryFile() as file:
        json.dump(data, file)
        file.seek(0)

        s3_key = "{}/{}".format(dataset_name, "latest.json")
        if upload_to_s3(file.name, s3_key) == False:
            raise Exception("Failed uploading latest.json ({}) to S3.".format(dataset_name))


def fetch_latest_json_metadata(dataset_name):
    with tempfile.NamedTemporaryFile() as file:
        try:
            s3_file = "{}/latest.json".format(dataset_name)
            get_s3_resource().Bucket(os.environ["AWS_BUCKET"]).download_fileobj(s3_file, file)
            file.seek(0)
            return json.load(file)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise

from __future__ import print_function
import os
import json
from urlparse import urlparse
import requests
import boto
from boto.s3.key import Key
from urlparse import urlparse
import datetime
import sys


def eprint(*args, **kwargs):
    """ https://stackoverflow.com/a/14981125 """
    print(*args, file=sys.stderr, **kwargs)


def upload_to_s3(aws_access_key_id, aws_secret_access_key, file, bucket, key, callback=None, md5=None, reduced_redundancy=False, content_type=None):
    """
    Uploads the given file to the AWS S3
    bucket and key specified.

    callback is a function of the form:

    def callback(complete, total)

    The callback should accept two integer parameters,
    the first representing the number of bytes that
    have been successfully transmitted to S3 and the
    second representing the size of the to be transmitted
    object.

    Returns boolean indicating success/failure of upload.

    http://stackabuse.com/example-upload-a-file-to-aws-s3/
    """
    try:
        size = os.fstat(file.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        file.seek(0, os.SEEK_END)
        size = file.tell()

    conn = boto.s3.connect_to_region(
        "ap-southeast-2", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    bucket = conn.get_bucket(bucket, validate=True)
    k = Key(bucket)
    k.key = key
    if content_type:
        k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(
        file, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy, rewind=True)

    # Rewind for later use
    file.seek(0)

    if sent == size:
        return True
    return False


def fetchDownloadSnapshot(URL):
    """Fetch a given data snapshot from SLIP and take care of
    following redirects and passing our credentials.
    :param verbose: Be verbose (give additional messages).

    This is required to work around a design decision in the
    requests library that limits passing authentication credentials
    on requests.Session() objects to URLs that match the domain of
    the original URL. In SLIP's case we must traverse maps.slip.wa.gov.au
    and sso.slip.wa.gov.au.

    See https://github.com/requests/requests/issues/2949 for further discussion.

    Args:
        URL (str): A URL to a data snapshot file or resource.

    Returns:
        object: A response object from the requests library.

    Raises exception if authentication fails or a non-200 response
    is received for the given URL.
    """

    response = s.get(URL, allow_redirects=False)
    if response.status_code == 302:
        parsed_uri = urlparse(response.headers['Location'])
        domain = "{uri.scheme}://{uri.netloc}/".format(uri=parsed_uri)
        if parsed_uri.netloc.startswith("sso.slip.wa.gov.au") or parsed_uri.netloc.startswith("maps.slip.wa.gov.au"):
            response = fetchDownloadSnapshot(response.headers['Location'])
        else:
            raise Exception("Receieved a redirect to an unknown domain '%s' for %s" % (
                parsed_uri.netloc, response.headers['Location']))

    if response.status_code == 200:
        if response.headers["Content-Type"] == "application/zip":
            return response
        raise Exception("Received an invalid Content-Type response - should be 'application/zip', but was '{}'".format(response.headers["Content-Type"]))
    else:
        raise Exception("Received a '%s' response for the URL %s" %
                        (response.status_code, URL))


# Fetch datasets config
with open("datasets.json", "r") as f:
    datasets = json.load(f)

# Setup a Session object globally to be used by all calls to requests
# c.f. http://docs.python-requests.org/en/latest/user/advanced/
s = requests.Session()
# Pro tip: Travis requires environment variables with special characters
# (e.g. $) to be escaped
s.auth = (os.environ["SLIP_USER"], os.environ["SLIP_PASS"])
s.headers.update({"User-Agent": "QGIS"})

for dataset_url in datasets:
    print("Processing %s" % dataset_url)
    eprint("Foo!")
    print >> sys.stderr, "Foobar!"

    # Fetch from SLIP
    try:
        response = fetchDownloadSnapshot(dataset_url)
    except Exception as e:
        eprint(str(e))

    zip_filename = os.path.basename(urlparse(dataset_url).path)
    with open(zip_filename, mode="wb") as localfile:
        localfile.write(response.content)

    # Upload to S3
    with open(zip_filename, "r") as file:
        datetimestamp = datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
        filename, file_extension = os.path.splitext(file.name)

        key = "{}/{}_{}{}".format(filename, filename, datetimestamp, file_extension)
        bucket = "geogig"

        if upload_to_s3(os.environ["AWS_ACCESS_KEY"], os.environ["AWS_ACCESS_SECRET_KEY"], file, bucket, key):
            print("Upload Succeeded")
        else:
            eprint("Upload Failed")

    # Tidy up
    os.remove(os.path.join(os.getcwd(), zip_filename))

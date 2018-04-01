import os
from urlparse import urlparse
import datetime
import re
import requests
from lib.logset import myLog
logger = myLog()

import urllib3
urllib3.disable_warnings()

# Setup a Session object globally to be used by all calls to requests that
# need to authenticate against SLIP's data snapshot file server
# c.f. http://docs.python-requests.org/en/latest/user/advanced/
session = requests.Session()
# Pro tip: Travis requires environment variables with special characters
# (e.g. $) to be escaped
session.auth = (os.environ["SLIP_USER"], os.environ["SLIP_PASS"])
session.headers.update({"User-Agent": "QGIS"})


def get_dataset_name_from_dataset_url(url):
    zip_filename = os.path.basename(urlparse(url).path)
    filename, file_extension = os.path.splitext(zip_filename)
    return filename


def get_s3_key_name_from_dataset_url(url):
    zip_filename = os.path.basename(urlparse(url).path)
    filename, file_extension = os.path.splitext(zip_filename)
    datetimestamp = datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    return "{}/{}_{}{}".format(filename, filename, datetimestamp, file_extension)


def fetch_download_snapshot(url):
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
        url (str): A URL to a data snapshot file or resource.
        s: A requests.Session object

    Returns:
        object: A response object from the requests library.

    Raises exception if authentication fails or a non-200 response
    is received for the given URL.
    """

    global session

    response = session.get(url, allow_redirects=False, verify=False)
    if response.status_code == 302:
        parsed_uri = urlparse(response.headers['Location'])
        domain = "{uri.scheme}://{uri.netloc}/".format(uri=parsed_uri)
        if parsed_uri.netloc.startswith("sso.slip.wa.gov.au") or parsed_uri.netloc.startswith("maps.slip.wa.gov.au"):
            response = fetch_download_snapshot(response.headers['Location'])
        else:
            raise Exception("Receieved a redirect to an unknown domain '%s' for %s" % (
                parsed_uri.netloc, response.headers['Location']))

    if response.status_code == 200:
        if response.headers["Content-Type"] == "application/zip":
            return response
        raise Exception("Received an invalid Content-Type response - should be 'application/zip', but was '{}'".format(response.headers["Content-Type"]))
    else:
        raise Exception("Received a '%s' response for the URL %s" %
                        (response.status_code, url))


def fetch_last_refresh_time_for_dataset(slip_id):
    """
    Foobar.
    """

    url = "https://selfservice.slip.wa.gov.au/Api/Index?id={}".format(slip_id)
    response = requests.get(url, verify=False)

    if response.status_code == 200:
        return response.json()["LastSuccessfulUpload"]
    elif response.status_code == 404:
        return False
    else:
        raise Exception("Received a '%s' response for the URL %s" %
                        (response.status_code, url))


def get_slip_id_from_dataset_title(dataset_title):
    """
    Foobar.
    """

    m = re.search(r".+\(([A-z]{2,}-[0-9]{3,})\)", dataset_title)
    if m:
        if len(m.groups()) > 1:
            raise Exception("Found two matches for SLIP Id in the dataset title '{}' - {}.".format(dataset_title, "//".join(m.groups())))
        return m.groups()[0]
    raise Exception("Unable to extract SLIP Id from dataset title '{}'.".format(dataset_title))


def should_we_download(latest, lastRefreshTime):
    """
    Foobar.
    """

    # If latest.json doesn't exist we can assume this is the
    # first time ever that we're downloading this dataset.
    if latest is False:
        return True

    # The dataset isn't in DUT yet - so fallback to downloading and
    # comparing the md5 checksums
    if lastRefreshTime == False:
        return True

    # The dataset is in DUT, but it's never been refreshed succesfully -
    # so again, let's fallback to comparing the md5 checksum
    if lastRefreshTime == 0:
        return True

    # That dataset is in DUT, and it has been refreshed since the last
    # time we downloaded it
    if latest["slip_last_updated_ticks"] != lastRefreshTime:
        return True

    return False

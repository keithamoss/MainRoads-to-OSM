import requests
from lib.logset import myLog
logger = myLog()

import urllib3
urllib3.disable_warnings()


def get_public_slip_shapefiles():
    """
    Foobar.
    """
    def get_all_ckan_packages():
        limit = 1000
        offset = 0
        packagesRemain = True
        packages = []

        while packagesRemain is True:
            url = "https://catalogue.data.wa.gov.au/api/3/action/current_package_list_with_resources?limit={limit}&offset={offset}".format(limit=limit, offset=offset)
            response = requests.get(url, verify=False)

            if response.status_code == 200:
                result = response.json()["result"]
                packages += result

                if len(result) < limit:
                    packagesRemain = False
                else:
                    offset += limit
            else:
                raise Exception("Received a '%s' response for the URL %s" % (response.status_code, url))
        return packages

    datasets = []
    packages = get_all_ckan_packages()

    logger.info("Got {} packages from CKAN.".format(len(packages)))

    for package in packages:
        dataset = {
            "name": package["name"],
            "title": package["title"],
            "organisation_name": package["organization"]["title"],
            "resource_urls": [],
        }

        slipDataSnapshotResources = [r for r in package["resources"] if r["format"] == "ZIP" and "maps.slip.wa.gov.au/datadownloads/" in r["url"]]

        for resource in slipDataSnapshotResources:
            if resource["url"].endswith(".csv.zip") or resource["url"].endswith(".geojson.zip") or resource["url"].endswith(".gpkg.zip"):
                logger.critical("Found a CSV/GeoJSON/GeoPackage dataset with format 'ZIP'")
                continue

            if "SLIP_Public_Services" in resource["url"] or "_Public_Secure_" in resource["url"]:
                dataset["resource_urls"].append(resource["url"])

        if len(dataset["resource_urls"]) > 0:
            datasets.append(dataset)

    return datasets

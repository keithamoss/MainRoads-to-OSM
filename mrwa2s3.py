import os
import tempfile
from lib.logset import myLog
logger = myLog()

import lib.slip
import lib.s3
import lib.utils
import lib.ckan
import lib.stats

stats = lib.stats.get_stats_object()

for dataset in lib.ckan.get_public_slip_shapefiles():
    logger.info(dataset["name"])

    stats["datasetsWithPublicShapefiles"] += 1
    if len(dataset["resource_urls"]) > 1:
        stats["datasetWithMultipleShapefiles"] += 1

    for dataset_url in dataset["resource_urls"]:
        dataset_name = lib.slip.get_dataset_name_from_dataset_url(dataset_url)

        try:
            latest = lib.s3.fetch_latest_json_metadata(dataset_name)
            slip_id = lib.slip.get_slip_id_from_dataset_title(dataset["title"])
            logger.info(slip_id)
            lastRefreshTime = lib.slip.fetch_last_refresh_time_for_dataset(slip_id)
            logger.info(lastRefreshTime)

            if lastRefreshTime is False:
                stats["datasetsWithAFalseResponseFromDUT"] += 1
            elif lastRefreshTime == 0:
                stats["datasetsWithA0ResponseFromDUT"] += 1

            if lib.slip.should_we_download(latest, lastRefreshTime) == True:
                # Fetch from SLIP's data snapshots file server
                response = lib.slip.fetch_download_snapshot(dataset_url)

                if response.status_code == 200:
                    # Write to a temporary file we can work with
                    with tempfile.NamedTemporaryFile(suffix=".zip", dir=os.path.join(os.getcwd(), "tmp")) as file:
                        file.write(response.content)
                        file.seek(0)

                        # A dataset being updated in SLIP doesn't actually mean that
                        # it has changed, so let's compare MD5 checksums to see if we
                        # need to actually upload this file.
                        if latest is False or latest["md5_checksum"] != lib.utils.md5_hash_file(file.name):
                            logger.info("Uploading...")
                            stats["datasetsWithChangedData"] += 1

                            # Upload the zipped data to S3
                            s3_key = lib.slip.get_s3_key_name_from_dataset_url(dataset_url)
                            lib.s3.upload_to_s3(file.name, s3_key)

                            # Refresh and upload a new latest.json metadata file to S3
                            lib.s3.create_and_upload_latest_json_metadata(file.name, s3_key, lastRefreshTime)

                        else:
                            logger.info("Skip downloading (checksum match)")
                            stats["datasetsSkippedDueToChecksumMatch"] += 1

            else:
                logger.info("Skip downloading (no need to download due to SLIP ticks)")
                stats["datasetsSkippedDueToSLIPTicks"] += 1

        except Exception as e:
            logger.error(str(e))

        logger.info("")

# Stats
lib.stats.print_stats(stats)
lib.stats.update_stats(stats)

# So Travis-CI will notify us of issues
if logger.has_critical_or_errors():
    print "We've got a few errors:"
    print logger.status()
    exit(1)

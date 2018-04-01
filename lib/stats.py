import os
import csv
import tempfile
import botocore
import lib.s3
import datetime


def get_stats_header():
    return ["date", "datasetsWithPublicShapefiles", "datasetWithMultipleShapefiles", "datasetsWithAFalseResponseFromDUT", "datasetsWithA0ResponseFromDUT", "datasetsWithChangedData", "datasetsSkippedDueToSLIPTicks", "datasetsSkippedDueToChecksumMatch"]


def download_file():
    try:
        file = tempfile.NamedTemporaryFile(suffix=".csv", dir=os.path.join(os.getcwd(), "tmp"))
        lib.s3.get_s3_resource().Bucket(os.environ["AWS_BUCKET"]).download_fileobj("stats.csv", file)
        return file
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise


def upload_file(file):
    if lib.s3.upload_to_s3(file.name, "stats.csv") == False:
        raise Exception("Failed uploading stats.csv to S3.")


def init_stats_csv():
    file = tempfile.NamedTemporaryFile(suffix=".csv", dir=os.path.join(os.getcwd(), "tmp"))
    writer = csv.writer(file)
    writer.writerow(get_stats_header())
    return file


def write_stats(file, stats):
    row = []
    for key in get_stats_header():
        row.append(stats[key] if key in stats else "NA")

    writer = csv.writer(file)
    writer.writerow(row)
    file.seek(0)


def get_stats_object():
    stats = {}
    for key in get_stats_header():
        stats[key] = 0
    stats["date"] = datetime.datetime.today().strftime('%Y-%m-%d')
    return stats


def update_stats(stats):
    file = download_file()
    if file is False:
        file = init_stats_csv()

    write_stats(file, stats)
    upload_file(file)

    file.close()


def stats_summary(stats):
    summary = ["Stats"]
    for key, value in stats.iteritems():
        summary.append("{}: {}".format(key, value))
    return "\n".join(summary)

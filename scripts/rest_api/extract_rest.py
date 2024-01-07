import configparser
import csv
import json

import boto3
import requests

lat = 42.36
lon = 71.05

lat_log_params = {"lat": lat, "lon": lon}


def fetch_data():
    api_reponse = requests.get(
        "http://api.open-notify.org/astros.json", params=lat_log_params
    )

    if api_reponse.status_code == 200:
        for item in json.loads(api_reponse.content)["people"]:
            yield [item.get("name"), item.get("craft")]


# for response in json_respose['']

export_file = "export_file.csv"
with open(export_file, "w", newline="") as f:
    csvw = csv.writer(f, delimiter=",")
    csvw.writerow(["name", "craft"])
    for d in fetch_data():
        csvw.writerow(d)


# to s3
parser = configparser.ConfigParser()

parser.read("pipeline.conf")

access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")


s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

s3_file = export_file

s3.upload_file(export_file, bucket_name, s3_file)

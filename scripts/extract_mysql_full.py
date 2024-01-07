import configparser
import csv

import boto3
import pymysql

if __name__ == "__main__":
    parser = configparser.ConfigParser()
    parser.read("../pipeline.conf")

    # mysql
    hostname = parser.get("mysql_config", "hostname")
    port = parser.get("mysql_config", "port")
    username = parser.get("mysql_config", "username")
    dbname = parser.get("mysql_config", "database")
    password = parser.get("mysql_config", "password")

    conn = pymysql.connect(
        host=hostname, user=username, password=password, db=dbname, port=int(port)
    )

    if conn is None:
        print("Error connecting to mysql")
    else:
        print("Connection established!")

    m_query = "SELECT * FROM ORDERS"
    local_filename = "order_extract.csv"

    m_cursor = conn.cursor()
    m_cursor.execute(m_query)

    results = m_cursor.fetchall()

    with open(local_filename, "w", newline="") as f:
        csv_w = csv.writer(f, delimiter=";")
        csv_w.writerow(["id", "status", "date"])
        csv_w.writerows(results)

    m_cursor.close()
    conn.close()

    # aws s3
    access_key = parser.get("aws_boto_credentials", "access_key")
    secret_key = parser.get("aws_boto_credentials", "secret_key")
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")

    s3 = boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )

    s3_file = local_filename

    s3.upload_file(local_filename, bucket_name, s3_file)

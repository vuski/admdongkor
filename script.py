import os
import sys
import csv
import json
import glob
import boto3
from datetime import datetime
from io import StringIO
import argparse

def tsv_to_json(tsv_file):
    with open(tsv_file, 'r', encoding='utf-8') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        rows = list(reader)

    json_data = json.dumps(rows, ensure_ascii=False)
    json_data = json.loads(json_data)
    return json_data

def upload_to_s3(json_data, bucket_name, key_name):
    s3 = boto3.client('s3')
    if type(json_data) == 'list':
        json_str = json.dumps(json_data, ensure_ascii=False)
    else:
        json_str = json_data
    return s3.put_object(Body=json_str, Bucket=bucket_name, Key=key_name, ContentType="application/json; charset=utf-8")

def get_adm_code_file():
    directory = "./통계청MDIS인구용_행정경계중심점"
    tsv_files = glob.glob(os.path.join(directory, '*.tsv'))
    if not tsv_files:
        return None
    tsv_files = sorted(tsv_files, reverse=True)
    return tsv_files[0]

def upload_adm_code(version, bucket):
    file = get_adm_code_file()
    print("adm 코드 파일: ", file, version)
    json_data = tsv_to_json(file)
    json_data = list(map(lambda d: {'code': d['ADMCD'], 'name': d['ADMNM']}, json_data))
    key_name = f"adm/adm_codes_{version}.json"
    # res = {'version': version, 'bucket': bucket, 'key_name': key_name}
    res = upload_to_s3(json_data, bucket_name=bucket, key_name=key_name)
    print("result: ", res)


def get_latest_geojson_file():
    geojson_files = glob.glob(os.path.join('.', '**/*.geojson'), recursive=True)
    if not geojson_files:
        return None
    geojson_files = sorted(geojson_files, reverse=True)
    return geojson_files[0]

def upload_adm_geojson(version, bucket):
    file_name = get_latest_geojson_file()
    print("geojson 파일: ", file_name)
    key_name = f"adm/adm_geojson_{version}.geojson"
    with open(file_name, 'r', encoding='utf-8') as f:
        json_data = f.read()
        upload_to_s3(json_data, bucket_name=bucket, key_name=key_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', default=datetime.today().strftime("%Y.%m.%d.%H%M%S"))
    parser.add_argument('-b', '--bucket', default="dev-geo-data.everybike.io")
    args = vars(parser.parse_args())
    print(args)
    # upload_adm_code(version=args['version'], bucket=args['bucket'])
    upload_adm_geojson(version=args['version'], bucket=args['bucket'])
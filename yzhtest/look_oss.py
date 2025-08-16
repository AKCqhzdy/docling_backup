import boto3
import os
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import requests
load_dotenv() 

BUCKET_TO_EXPLORE = 'graxy-dev'
# PREFIX_TO_EXPLORE = 'ofnil/tmp/FSD/Fire_Safety_in_industrial_building/test2'
PREFIX_TO_EXPLORE = 'ofnil/tmp/user/2e9946d0-eb85-4508-b499-feda899d0314'
LOCAL_DOWNLOAD_DIR = 'test/downloaded_files'

def get_oss_client():
    endpoint = os.getenv('OSS_ENDPOINT')
    access_key_id = os.getenv('OSS_ACCESS_KEY_ID')
    secret_access_key = os.getenv('OSS_ACCESS_KEY_SECRET')

    if not all([endpoint, access_key_id, secret_access_key]):
        raise ValueError("请确保设置了环境变量: OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET")

    return boto3.client(
        's3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        endpoint_url=endpoint,
        config=Config(s3={"addressing_style": "virtual"},
                      signature_version='v4')
    )


def list_and_download_from_prefix(bucket_name, prefix):
    s3_oss_client = get_oss_client()

    print(f"正在查找 Bucket '{bucket_name}' 中前缀为 '{prefix}' 的文件...")

    response = s3_oss_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' not in response:
        print("在该目录下没有找到任何文件。")
        return

    files_to_download = response['Contents']
    print(f"找到了 {len(files_to_download)} 个文件")
    print(files_to_download)
    EXCLUDE_PREFIX = f'{prefix}test'

    valid_files = [
        f for f in files_to_download
        if not f['Key'].endswith('/') and not f['Key'].startswith(EXCLUDE_PREFIX)
    ]

    print(f"排除后剩余 {len(valid_files)} 个文件，准备下载...")
    # return ;

    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
    print(f"文件将被下载到本地目录: '{LOCAL_DOWNLOAD_DIR}/'")

    for file_obj in valid_files:
        object_key = file_obj['Key']
        if object_key != 'ofnil/tmp/user/2e9946d0-eb85-4508-b499-feda899d0314/FSIC_list_sc.pdf':
            continue
        
        if object_key.endswith('/'):
            continue

        local_file_name = os.path.basename(object_key)
        local_file_path = os.path.join(LOCAL_DOWNLOAD_DIR, local_file_name)

        print(f"  -> 正在下载 '{object_key}' 到 '{local_file_path}' ...")
        try:
            s3_oss_client.download_file(bucket_name, object_key, local_file_path)
            print(f"  ✓ 下载成功: {local_file_path}")
        except ClientError as e:
            print(f"  ✗ 下载失败: {object_key}, 错误: {e}")

if __name__ == '__main__':
    list_and_download_from_prefix(BUCKET_TO_EXPLORE, PREFIX_TO_EXPLORE)
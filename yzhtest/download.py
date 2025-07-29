import boto3

# 配置
BUCKET_NAME = 'monkeyocr'
OBJECT_KEY = 'test/input/test_pdf/small.pdf'
FILE_NAME = 'test.pdf'

def download_file(bucket_name, object_key, file_name):
    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket_name, object_key, file_name)
        print(f"文件 '{object_key}' 已成功从 '{bucket_name}' 下载到 '{file_name}'")
    except Exception as e:
        print(f"下载文件时发生错误：{e}")


if __name__ == '__main__':
    download_file(BUCKET_NAME, OBJECT_KEY, FILE_NAME)
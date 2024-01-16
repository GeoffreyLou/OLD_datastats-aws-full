from botocore.exceptions import ClientError
import json
import boto3
import os

def get_secret():

    secret_name_1 = os.environ['SECRET_NAME_1']
    secret_name_2 = os.environ['SECRET_NAME_2']
    region_name = os.environ['SECRET_REGION_NAME']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response_1 = client.get_secret_value(
            SecretId=secret_name_1
        )
    except ClientError as e:
        raise e
    
    try:
        get_secret_value_response_2 = client.get_secret_value(
            SecretId=secret_name_2
        )
    except ClientError as e:
        raise e

    secret_1 = get_secret_value_response_1['SecretString']
    secret_dict_1 = json.loads(secret_1)

    # Retrieve specific variables (e.g., username and password)
    database = secret_dict_1.get('dbInstanceIdentifier')
    user = secret_dict_1.get('username')
    password = secret_dict_1.get('password')
    host = secret_dict_1.get('host')
    port = secret_dict_1.get('port')

    secret_2 = get_secret_value_response_2['SecretString']
    secret_dict_2 = json.loads(secret_2)

    django_secretkey = secret_dict_2.get('DJANGO_SECRETKEY')
    email_host_user = secret_dict_2.get('EMAIL_HOST_USER')
    email_host_password = secret_dict_2.get('EMAIL_HOST_PASSWORD')
    email_send_user = secret_dict_2.get('EMAIL_SEND_USER')
    aws_storage_bucket_name = secret_dict_2.get('AWS_STORAGE_BUCKET_NAME')
    aws_storage_csv_folder_name = secret_dict_2.get('AWS_STORAGE_CSV_FOLDER_NAME')
    aws_id = secret_dict_2.get('AWS_K_ID')
    aws_secret_id = secret_dict_2.get('AWS_S_K_ID')
    aws_bucket_data_files = secret_dict_2.get('AWS_S3_BUCKET_DATA_FILES')
    aws_bucket_static_files = secret_dict_2.get('AWS_S3_BUCKET_STATIC_FILES')
    aws_cloudfront_distribution = secret_dict_2.get('AWS_CLOUDFRONT_DISTRIBUTION')
    aws_cloudfront_key_id = secret_dict_2.get('AWS_CLOUDFRONT_KEY_ID')
    aws_cloudfront_key = secret_dict_2.get('AWS_CLOUDFRONT_KEY')
    users_db = secret_dict_2.get('USERS_DB')


    return database, user, password, \
            host, port, django_secretkey, \
            email_host_user, email_host_password, \
            email_send_user, aws_storage_bucket_name, \
            aws_storage_csv_folder_name, aws_id, \
            aws_secret_id, aws_bucket_data_files, \
            aws_bucket_static_files, aws_cloudfront_distribution, \
            aws_cloudfront_key_id, aws_cloudfront_key, users_db

database, user, password, host, port, django_secretkey, \
    email_host_user, email_host_password, email_send_user, \
    aws_storage_bucket_name, aws_storage_csv_folder_name, aws_id, \
    aws_secret_id, aws_bucket_data_files, aws_bucket_static_files, \
    aws_cloudfront_distribution, aws_cloudfront_key_id, aws_cloudfront_key, \
    users_db = get_secret()

aws_cloudfront_key = aws_cloudfront_key.replace('\\n', '\n')
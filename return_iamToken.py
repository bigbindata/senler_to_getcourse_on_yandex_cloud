import datetime
import os
import json
import requests
import boto3

def return_iamToken()-> str:

  bucket_name = os.getenv("bucket_name") # Имя корзины в s3
  name_file = os.getenv("name_file") #Файл с токеном в s3 корзине
  oauth_token = os.getenv("OAuth_token")

  session = boto3.session.Session()
  s3 = session.client(
  service_name='s3',
  endpoint_url='https://storage.yandexcloud.net',
  aws_access_key_id = os.getenv("aws_access_key_id"),
  aws_secret_access_key = os.getenv("aws_secret_access_key") )

  get_object_response = s3.get_object(Bucket=bucket_name,Key=name_file)
  dict_token=json.loads(get_object_response['Body'].read())
  if dict_token.get('expiresAt') or \
    datetime.datetime.now()>=datetime.datetime.strptime(dict_token['expiresAt'].split(".")[0],
                                                        '%Y-%m-%dT%H:%M:%S'):
      response=requests.post("https://iam.api.cloud.yandex.net/iam/v1/tokens", params={'yandexPassportOauthToken': oauth_token})
      s3.put_object(Bucket=bucket_name, Key=name_file, Body=response.text)
      return response.json()['iamToken']
  else:
    return dict_token['iamToken']

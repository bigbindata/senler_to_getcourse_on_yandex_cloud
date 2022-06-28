import base64
import requests
import json
import os
from return_iamToken import return_iamToken
import ydb
import ydb.iam

def add_to_getcource_order_from_vk(email: str, deal_number: str ,product_title: str , object_dict_with_utm: dict)-> dict:
  "Добавляет заказ по апи в геткурс"
  account_name=os.getenv("getcource_account_name")
  secret_key=os.getenv("api_secret_key_from_getcource")
  list_utm=["source","medium","campaign","content"]
  url_api=f"https://{account_name}.getcourse.ru/pl/api/deals?action=add&key={secret_key}"


  message=base64.b64encode(json.dumps(

        {
        "user":{
              "email":email,

          },
        "system":{
            "refresh_if_exists":1, # // обновлять ли существующего пользователя 1/0 да/нет

            "multiple_offers":0, # добавлять несколько предложений в заказ 1/0
            "return_payment_link":0, #// возвращать ссылку на оплату 1/0
            "return_deal_number":0 #// возвращать номер заказа 1/0
        },
        "session":     {"utm_"+i: object_dict_with_utm[i].decode("utf-8") for i in list_utm}
        ,
        "deal":{
            "deal_number":deal_number,
            "product_title":product_title,
            "quantity":1, #// количество
            "deal_cost":0,
            "addfields":{"scan_utm_"+i: object_dict_with_utm[i].decode("utf-8") for i in list_utm}, #// для добавления дополнительных полей заказа
        }
    } ).encode("utf-8")
  )
  response=requests.post(url_api,data={"action":"add",
                                      "key": {secret_key},
                                      "params":{message}
  })

  return response



# Задаём значение переменной DEBUG
os.environ.setdefault("YDB_ACCESS_TOKEN_CREDENTIALS", return_iamToken())
os.environ.setdefault("YDB_ENDPOINT", 'your_YDB_ENDPOINT')
os.environ.setdefault("YDB_DATABASE", "your_YDB_DATABASE")



def execute_query(session, query):

    return session.transaction().execute(
        query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2),
    )


def main(query):
    
  driver = ydb.Driver(
      endpoint=os.getenv("YDB_ENDPOINT"),
      database=os.getenv("YDB_DATABASE"),
      credentials=ydb.AccessTokenCredentials(
          os.getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
      ),
  )

  with driver:

    driver.wait(fail_fast=True, timeout=15)

    with ydb.SessionPool(driver) as pool:
      # Execute the query with the `retry_operation_helper` the.
      # The `retry_operation_sync` helper used to help developers
      # to retry YDB specific errors like locks invalidation.
      # The first argument of the `retry_operation_sync` is a function to retry.
      # This function must have session as the first argument.
      result = pool.retry_operation_sync(execute_query,query=query)
      # print(result)
      return result
      
def return_dict_utm(vk_id):
  query=f"""SELECT `id`, `campaign`, `content`, `medium`, `source`
  FROM `deryabin/senler_vk`
  where `id`=={vk_id} ;"""
  
  result=main(query)
  try:
    return result[0].rows[0]
  except:
    return False

def handler(event, context):
    params=event['queryStringParameters']
    product_title="your_product_title_in_getcourse"
    try:
      dict_utm=return_dict_utm(params['vk_id'])
      if dict_utm:
        add_to_getcource_order_from_vk(email=params['email'], deal_number=params['deal_number'], product_title=product_title, object_dict_with_utm=dict_utm)
    except Exception as e:
      print("Ошибка",e,params['vk_id'],params['email'],params['deal_number'])


    return {
        'statusCode': 200,
        'body': 'OK',
    }
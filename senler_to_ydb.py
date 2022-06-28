import os
import ydb
import json
from return_iamToken import return_iamToken
from dotenv import load_dotenv

load_dotenv()

os.environ.setdefault("YDB_ACCESS_TOKEN_CREDENTIALS", return_iamToken() )
os.environ.setdefault("YDB_ENDPOINT", 'your_YDB_ENDPOINT')
os.environ.setdefault("YDB_DATABASE", "your_YDB_DATABASE")


def handler(event, context):
    # Execute query with the retry_operation helper.
    data=json.loads(event['body'])
    if data.get('type')=='check':
        return {
        'statusCode': 200,
        'body': "OK",
        }

    # print(json.loads(event['body']))

    def execute_query(session):

      utm_string=str([object_dict.get("utm_"+i,"none") for i in ['campaign', 'content', 'medium', 'source']])[1:-1]
      values=f"{int(object_dict['vk_user_id'])},{utm_string}"
      query=f"""UPSERT INTO `deryabin/senler_vk`
          ( `id`, `campaign`, `content`, `medium`, `source` )
      VALUES ({values} );
      """
      # create the transaction and execute query.
      return session.transaction().execute(
          query,
          commit_tx=True,
          settings=ydb.BaseRequestSettings().with_timeout(10).with_operation_timeout(10)
      )


    try:
        object_dict=data['object']
    except Exception as e:
        print(e)
        return {
        'statusCode': 200,
        'body': "OK",
        }

    if object_dict['source'].startswith("subscr"):

        driver = ydb.Driver(
            endpoint=os.getenv("YDB_ENDPOINT"),
            database=os.getenv("YDB_DATABASE"),
            credentials=ydb.AccessTokenCredentials(
                os.getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
            ),
        )

        # Wait for the driver to become active for requests.
        driver.wait(fail_fast=False, timeout=10)
        # Create the session pool instance to manage YDB sessions.
        pool = ydb.SessionPool(driver)
        result = pool.retry_operation_sync(execute_query)
    return {
        'statusCode': 200,
        'body': "OK",
    }

# handler({"body":json.dumps({
#     "vk_group_id": 161605158,
#     "date": "2022-06-27 15:55:26",
#     "unixtime": 1656334526,
#     "subscription_id": 1676234,
#     "vk_user_id": 79034317,
#     "source": "subscription"
#   })
#   }, context=None)
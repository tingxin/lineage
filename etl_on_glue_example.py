import boto3
import time
# Athena
athena = boto3.client('athena')

context = {'Database': 'default'}
config = {'OutputLocation': 's3://aws-glue-assets-027040934161-cn-northwest-1/'}

sql = """
INSERT INTO user_click
SELECT user_info.user_mail, city, COUNT(*) as click
FROM user_event
LEFT JOIN user_info
ON user_info.user_mail=user_info.user_mail
GROUP BY user_info.user_mail,city
"""

execution = athena.start_query_execution(
    QueryString=sql,
    QueryExecutionContext=context,
    ResultConfiguration=config,
    WorkGroup='primary'
)

# 轮询查询是否完成
while True:
    response = athena.get_query_execution(
        QueryExecutionId=execution['QueryExecutionId'])
    status = response['QueryExecution']['Status']['State']
    if status == 'SUCCEEDED':
        # 打印本次查询数据扫描量
        print(float(response['QueryExecution']['Statistics']
              ['DataScannedInBytes'])/1024/1024/1024)
        break
    time.sleep(1)

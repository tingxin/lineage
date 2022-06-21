import json
import time
import urllib.parse
import boto3
from datetime import datetime, timedelta

region_name = "cn-northwest-1"
max_parallel_count = 5
time_diff = 10  # 十分钟内不容许上传相同的文档

# ===========================需要修改或确认的配置参数================================
workflow_name = "lineage-data-workflow"
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
# ===========================需要修改或确认的配置参数================================


def lambda_handler(event, context):
    print("Received event: " + str(event))
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    condition = (now - timedelta(minutes=time_diff)
                 ).strftime("%Y-%m-%d %H:%M:%S")

    keys = [record['s3']['bucket']['name'] + "/" + record['s3']['object']['key']
            for record in event['Records']]
    task_count = 0
    try:

        sqs = boto3.client('sqs', region_name=region_name)
        for key in keys:
            ukey = urllib.parse.unquote_plus(key, encoding='utf-8')
            file_key = "s3://" + ukey
            task_count += 1
            k_info = {
                "file_key": file_key,
                "event_time": current_time,
                "status": "begin"
            }

            str_info = json.dumps(k_info)

            sqs.send_message(
                QueueUrl=sqs_url,
                MessageBody=str_info,
                DelaySeconds=0
            )

        parallel_count = min(max_parallel_count, task_count)

        glue = boto3.client('glue', region_name=region_name)
        for i in range(0, parallel_count):
            p = glue.start_workflow_run(
                Name=workflow_name,
            )
            print(f"got glue workflow {p}")
    except Exception as e:
        print(e)
        return {
            "status": str(e)
        }

    return {
        "status": "ok"
    }

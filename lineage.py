import boto3
import requests
import io
import json
import time

region_name = "cn-northwest-1"
job_name = "lineage_demo"
sqs_endpoint_url = "https://vpce-00d7a33aec3f90717-5lz7g217.sqs.cn-northwest-1.vpce.amazonaws.com.cn"
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
endpoint = 'database-demo-instance-1.c6lwjjfhbm6a.neptune.cn-northwest-1.amazonaws.com.cn'
port = 8182


def update_neptune(sources: list, target: str):
    glue = boto3.client('glue', region_name=region_name)

    api = f'HTTPS://{endpoint}:{port}/openCypher'

    nodes = [item for item in sources]
    nodes.append(target)
    for node in nodes:
        cmd = "query=MERGE (a:Table{name:'"+node+"'})"
        r = requests.post(api, data=cmd)
        print(r.text)

    for source in sources:
        cmd = "query=MATCH (a:Table{name:'" + source + \
            "'}),(c:Table{name:'"+target+"'}) MERGE (a)-[r1:Parent]->(c)"
        r = requests.post(api, data=cmd)
        print(r.text)

    for node in nodes:
        resp = glue.get_table(
            CatalogId='027040934161',
            DatabaseName='default',
            Name=node
        )

        if 'Table' in resp and 'StorageDescriptor' in resp['Table']:
            columns = resp['Table']['StorageDescriptor']['Columns']
            for column in columns:
                comumn_name = column['Name']

                cmd1 = "query=MERGE (a:Column{name:'"+comumn_name+"'})"
                r = requests.post(api, data=cmd1)
                print(r.text)

                cmd2 = "query=MATCH (a:Column{name:'" + comumn_name + \
                    "'}),(c:Table{name:'"+node+"'}) MERGE (a)-[r1:Belong]->(c)"
                print(cmd2)
                r = requests.post(api, data=cmd2)
                print(r.text)


def get_glue_job_code(s3_code_path):
    s3 = boto3.client('s3', region_name=region_name)

    s3_parts = s3_code_path.split('/')
    bucket = s3_parts[2]
    key = "/".join(s3_parts[3:])
    code_info = s3.get_object(Bucket=bucket, Key=key)

    data = code_info['Body'].read()
    bytes = io.BytesIO(data)
    sql_list = list()
    begin_add = False
    for line in bytes:
        line_code = line.decode('utf-8')

        if not begin_add:
            index = line_code.find("sql =")
            if index >= 0:
                begin_add = True
        else:
            if line_code.find('"""') >= 0:
                break
            sql_list.append(line_code)

    return " ".join(sql_list)


def parse_sql(sql: str):
    return {
        "target": "user_click",
        "source": ["user_event", "user_info"]
    }


def get_tasks():
    sqs = boto3.client('sqs', region_name=region_name,
                       endpoint_url=sqs_endpoint_url)

    index = 0
    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            VisibilityTimeout=600
        )
        print(response)
        if "Messages" not in response:
            print(f"===> got {index}")
            break

        index += 1
        messages = response["Messages"]
        task_info = list()
        for msg in messages:
            body_str = msg["Body"]
            body = json.loads(body_str)
            receipt_handle = msg["ReceiptHandle"]
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=receipt_handle
            )
            task_info.append(body)

        for body in task_info:
            info = yield body

    yield None


def main():
    gen = get_tasks()
    task = next(gen)
    while task:
        s3_code_path = task['file_key']
        print(f"=========> got task {s3_code_path}")

        try:
            sql = get_glue_job_code(s3_code_path)
            dependencies = parse_sql(sql)

            target = dependencies['target']
            source = dependencies['source']

            update_neptune(source, target)
            task = gen.send("successful==>successful")
        except Exception as e:
            print(f"ERROR ===========> {e}")
            task = gen.send(f"failed==>{e}")
        time.sleep(1)


main()

import sys
import time
import boto3

# 클러스터의 경우, 클러스터에 아래 태그 수정 및 추가 필요
# 인스턴스의 경우, 인스턴스에 아래 태그 수정 및 추가 필요
tag_key = 'autostop'
tag_value = 'true'


# DB 클러스터/인스턴스별 파라미터 변수(status, db_type, db_id, db_engine)에 저장
def get_params(source_type):
    if source_type == "CLUSTER":
        return "Status", "DBClusters", "DBClusterIdentifier", "Engine"
    elif source_type == "DB_INSTANCE":
        return "DBInstanceStatus", "DBInstances", "DBInstanceIdentifier", "Engine"


# DB 클러스터/인스턴스 중지
def response_stop_db(client, source_type, event_db_id, db_engine):
    if source_type == "CLUSTER":
        if db_engine in ['aurora-mysql','aurora-postgresql']:
            return client.stop_db_cluster(DBClusterIdentifier=event_db_id)
    elif source_type == "DB_INSTANCE":
        return client.stop_db_instance(DBInstanceIdentifier=event_db_id)


# DB 클러스터/인스턴스별 정보 확인
def response_describe_db(client, source_type):
    if source_type == "CLUSTER":
        return client.describe_db_clusters()
    elif source_type == "DB_INSTANCE":
        return client.describe_db_instances()


# DB 인스턴스가 클러스터에 속하는지 여부 확인
def is_instance_in_cluster(i):
    # DB 인스턴스가 클러스터 내에 존재할 경우 Exit
    try:
        event_db_id = i["DBClusterIdentifier"] # 클러스터에 속한 DB 인스턴스가 아닌 경우, 해당 필드가 존재하지 않음.
        print(f"Exit. This instance is a member of a DB cluster: {event_db_id}")
        sys.exit()
    except KeyError:
        return


# 중지할 DB 클러스터/인스턴스 확인[이벤트 발생한 DB ID(event_db_id)와 존재하는 DB 클러스터/인스턴스만 종료]
def stop_db(client, response, event_db_id, source_type):
    status, db_type, db_id, db_engine = get_params(source_type)
    
    for i in response[db_type]:
        
        if i[db_id] != event_db_id:  # 이벤트 발생한 DB ID(event_db_id)와 존재하는 DB 클러스터/인스턴스 비교
            continue
              
        if source_type == "DB_INSTANCE": # DB 인스턴스가 클러스터에 속하는지 여부 확인
            is_instance_in_cluster(i)

        if len(i['TagList']) == 0: # TagList('autostop'/'true') 존재하는지 여부 확인
            continue
        else:
            for item in i['TagList']:
                if item.get('Key') == tag_key and item.get('Value') == tag_value:
                    break
            else:
                continue
        
        if i[status] == "available":
            print(f"{event_db_id} Status is available, stop this.")
            try:
                response = response_stop_db(client, source_type, event_db_id, i[db_engine])
                status_code = response["ResponseMetadata"]["HTTPStatusCode"]
                print("HTTPStatusCode: ", status_code)
                sys.exit()
            except Exception as e:
                print(e)
                print("Wait 60 sec. Status is actually not available.")
                time.sleep(60)
                return
        elif (i[status] == "stopping") or (i[status] == "stopped"):
            print(f"Exit. {event_db_id} Status: {i[status]}")
            sys.exit()
        else:
            print(f"{event_db_id} Status is: {i[status]}. Wait 30 sec for 'available'. ")
            time.sleep(30)
            return


def lambda_handler(event, context):
    client = boto3.client("rds")
    
    print(f"event: {event}")
    
    event_db_id = event["detail"]["SourceIdentifier"] # EventBridge 이벤트에서 DB 식별자 파싱
    # event_db_id = "mkkim-aurora-postgre-test"
    source_type = event["detail"]["SourceType"] # EventBridge 이벤트에서 DB 클러스터/인스턴스 식별
    # source_type = "CLUSTER"

    while True:
        response = response_describe_db(client, source_type) # DB 클러스터/인스턴스별 정보 저장
        stop_db(client, response, event_db_id, source_type)

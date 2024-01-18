import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# load .env
load_dotenv()

# CSV 파일 읽기
csv_file = 'relationaccount.csv'  # CSV 파일 경로
df = pd.read_csv(csv_file)

# Neo4j 연결 설정
uri = os.environ.get('uri')
username = os.environ.get('username')
password = os.environ.get('password')
driver = GraphDatabase.driver(uri, auth=(username, password))

# 노드 생성 쿼리 실행 함수
def create_node(tx, label, properties):
    query = (
        f"CREATE (n:{label} {{ " +
        ", ".join([f"{key}: ${key}" for key in properties.keys()]) +
        " })"
    )
    tx.run(query, **properties)

# 관계 생성 쿼리 실행 함수 (옵션)
def create_relationship(tx, label1, id1, label2, id2, rel_type):
    query = (
        f"MATCH (a:{label1}), (b:{label2}) "
        "WHERE a.id = $id1 AND b.id = $id2 "
        f"CREATE (a)-[r:{rel_type}]->(b)"
    )
    tx.run(query, id1=id1, id2=id2)

# Neo4j 데이터베이스에 쿼리 전송
with driver.session() as session:
    # 노드 생성
    for index, row in df.iterrows():
        print("@@@@")
        print(index,row)
        print("@@@@")

        #session.write_transaction(create_node, 'PayerAccount', row.to_dict())
        session.write_transaction(create_relationship, 'PayerAccount', "id", 'PayerAccount', "id", 'RELATES_TO')

    # 노드 생성 (옵션)
    # 예: session.write_transaction(create_node, 'NodeLabel', row.to_dict())

    # 관계 생성 (옵션)
    # 예: session.write_transaction(create_relationship, 'Label1', id1, 'Label2', id2, 'RELATES_TO')

driver.close()
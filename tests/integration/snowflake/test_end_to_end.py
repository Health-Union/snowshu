from datetime import datetime
import docker
import pytest
import os
import time
from sqlalchemy import create_engine
from snowshu.configs import PACKAGE_ROOT
from click.testing import CliRunner
from snowshu.core.main import cli

""" End-To-End all inclusive test session"""
#1. builds a new replica based on the template configs
#2. launches the replica
#3. Queries the replica 
#4. Spins down and cleans up

BASE_CONN='postgresql://snowshu:snowshu@integration-test:9999/{}'
SNOWSHU_META_STRING=BASE_CONN.format('snowshu')
SNOWSHU_DEVELOPMENT_STRING=BASE_CONN.format('SNOWSHU_DEVELOPMENT')

@pytest.fixture(scope="session", autouse=True)
def end_to_end(docker_flush_session):
    runner = CliRunner()
    configuration_path = os.path.join(
        PACKAGE_ROOT, 'snowshu', 'templates', 'replica.yml')
    create_output=runner.invoke(cli, ('create', '--replica-file', configuration_path)).output.split('\n')
    client=docker.from_env()
    client.containers.run(replica.id,
                          ports={'9999/tcp':9999},
                          name='snowshu_replica_integration-test',
                          network='snowshu',
                          detach=True)
    time.sleep(5) # the replica needs a second to initialize
    return create_output

def any_appearance_of(string,strings):
    return any([string in line for line in strings])

def test_reports_full_catalog_start(end_to_end):
    print('test_reports_catalog')
    result_lines = end_to_end
    assert any_appearance_of('Assessing full catalog...',result_lines)

def test_finds_9_relations(end_to_end):
    print('test_reports_9_relations')
    result_lines= end_to_end
    assert any_appearance_of('Identified a total of 9 relations to sample based on the specified configurations.',result_lines)

def test_replicates_order_items(end_to_end):
    print('test_replicates_order_items')
    result_lines = end_to_end
    assert any_appearance_of('Done replication of relation SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS',result_lines)

@pytest.mark.skip
def test_snowshu_explain(end_to_end):
    runner = CliRunner()
    response = json.loads(runner.invoke(
        cli, ('explain', 'integration-test', '--json')))

    assert response['name'] == 'integration-test'
    assert response['image'] == 'postgres:12'
    assert response['target_adapter'] == 'postgres'
    assert response['source_adapter'] == 'snowflake'
    assert datetime(response['created_at']) < datetime.now()

def test_bidirectional(end_to_end):
    print('test_bidirectional')
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = """
SELECT 
    COUNT(*) 
FROM 
    "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."USER_COOKIES" uc
FULL OUTER JOIN
     "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."USERS" u
ON 
    uc.user_id=u.id
WHERE 
    uc.user_id IS NULL
OR
    u.id IS NULL
"""
    q = conn.execute(query)
    count = q.fetchall()[0][0]
    assert count == 0


def test_directional(end_to_end):
    print('test_directional')
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = """
WITH
joined_roots AS (
SELECT 
    oi.id AS oi_id
    ,oi.order_id AS oi_order_id
    ,o.id AS o_id
FROM 
    "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."ORDER_ITEMS" oi
FULL OUTER JOIN
     "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."ORDERS" o
ON 
    oi.order_id = o.id
)
SELECT 
    (SELECT COUNT(*) FROM joined_roots WHERE oi_id is null) AS upstream_missing
    ,(SELECT COUNT(*) FROM joined_roots WHERE o_id is null) AS downstream_missing
"""
    q = conn.execute(query)
    upstream_missing, downstream_missing = q.fetchall()[0]
    assert upstream_missing > 0
    # it is statistically very unlikely that NONE of the upstreams without a downstream will be included.
    assert downstream_missing == 0

def test_view(end_to_end):
    print('test_view')
    conn = create_engine(SNOWSHU_DEVELOPMENT_STRING)
    query = """
SELECT 
    (SELECT COUNT(*) FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."ORDER_ITEMS_VIEW") /
    (SELECT COUNT(*) FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."ORDER_ITEMS") AS delta
"""
    q = conn.execute(query)
    assert len(set(q.fetchall()[0])) == 1

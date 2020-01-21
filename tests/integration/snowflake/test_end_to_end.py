from datetime import datetime
import docker
import pytest
import os
import time
from sqlalchemy import create_engine
from snowshu.configs import PACKAGE_ROOT
from click.testing import CliRunner
from snowshu.core.main import cli

# Build the initial test replica from snowshu creds

CONN_STRING = 'postgresql://snowshu:snowshu@integration-test:9999/snowshu'
REPLICA_STRING='/'.join(CONN_STRING.split('/')
                                  [:-1]+['SNOWSHU_DEVELOPMENT'])

@pytest.fixture(scope="session", autouse=True)
def run_snowshu_create():
    runner = CliRunner()
    configuration_path = os.path.join(
        PACKAGE_ROOT, 'snowshu', 'templates', 'replica.yml')
    return runner.invoke(cli, ('run', '--replica-file', configuration_path)).output.split('\n')


@pytest.fixture(scope="session", autouse=True)
def run_snowshu_launch():
    runner = CliRunner()
    runner.invoke(cli, 'launch', 'integration-test')
    yield
    docker.from_env().containers.get('integration-test').kill()


def test_reports_full_catalog_start(run_snowshu_create):
    result_lines = run_snowshu_create
    assert any(['Assessing full catalog...' in line for line in result_lines])


def test_finds_9_relations(run_snowshu_create):
    result_lines = run_snowshu_create
    assert any(['Identified a total of 9 relations to sample based on the specified configurations.' in line for line in result_lines])


def test_replicates_order_items(run_snowshu_create):
    result_lines = run_snowshu_create
    assert any(
        ['Done replication of relation SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS' in line for line in result_lines])


@pytest.mark.skip
def test_snowshu_explain(run_snowshu_create):
    runner = CliRunner()
    response = json.loads(runner.invoke(
        cli, ('explain', 'integration-test', '--json')))

    assert response['name'] == 'integration-test'
    assert response['image'] == 'postgres:12'
    assert response['target_adapter'] == 'postgres'
    assert response['source_adapter'] == 'snowflake'
    assert datetime(response['created_at']) < datetime.now()


def test_launches(run_snowshu_create):
    runner = CliRunner()
    time.sleep(3)
    response = runner.invoke(cli, ('launch', 'integration-test'))
    EXPECTED_STRING = """
Replica integration-test has been launched and started.
To stop your replica temporarily, use command `snowshu stop integration-test`.
To spin down your replica, use command `snowshu down integration-test`.

You can connect directly from your host computer using the connection string

snowshu:snowshu@localhost:9999/snowshu

You can connect to the sample database from within docker containers running on the `snowshu` docker network.
use the connection string

snowshu:snowshu@integration-test:9999/snowshu

to connect.
"""
    assert EXPECTED_STRING in response.output
    conn_string = (REPLICA_STRING)
    conn = create_engine(conn_string)
    time.sleep(5)
    q = conn.execute(
        'SELECT COUNT(*) FROM "SNOWSHU_DEVELOPMENT"."EXTERNAL_DATA"."ADDRESS_REGION_ATTRIBUTES"')
    count = q.fetchall()[0][0]
    assert count > 100


@pytest.mark.skip
def test_starts(run_snowshu_create):
    runner = CliRunner()
    response = runner.invoke(cli, 'start', 'integration-test')
    assert 'ReplicaFactory integration-test restarted.' in response.output
    assert 'You can connect to this replica with connection string: postgresql://snowshu:snowshu@snowshu:9999/snowshu' in response.output
    assert 'To stop your replica temporarily, use command `snowshu stop integration-test`' in response.output
    assert 'To spin down your replica, use command `snowshu down integration-test`' in response.output
    # cleanup
    runner.invoke(cli, 'down', 'integration-test')


@pytest.mark.skip
def test_stops(run_snowshu_create):
    runner = CliRunner()
    response = runner.invoke(cli, 'stop', 'integration-test')
    assert 'ReplicaFactory integration-test stopped.' in response.output
    assert 'You can connect to this replica with connection string: postgresql://snowshu:snowshu@snowshu:9999/snowshu' not in response.output
    assert 'To start your replica again use command `snowshu start integration-test`' in response.output


def test_bidirectional(run_snowshu_create, run_snowshu_launch):
    conn = create_engine(REPLICA_STRING)
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

    time.sleep(5)
    q = conn.execute(query)
    count = q.fetchall()[0][0]
    assert count == 0


def test_directional(run_snowshu_create, run_snowshu_launch):
    conn = create_engine(REPLICA_STRING)
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


def test_view(run_snowshu_create, run_snowshu_launch):

    conn = create_engine(REPLICA_STRING)
    query = """
SELECT 
    (SELECT COUNT(*) FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."ORDER_ITEMS_VIEW") /
    (SELECT COUNT(*) FROM "SNOWSHU_DEVELOPMENT"."SOURCE_SYSTEM"."ORDER_ITEMS") AS delta
"""
    time.sleep(5)
    q = conn.execute(query)
    assert len(set(q.fetchall()[0])) == 1

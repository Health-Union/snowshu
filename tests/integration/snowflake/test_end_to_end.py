from datetime import datetime
import pytest
import os
from snowshu.configs import PACKAGE_ROOT
from click.testing import CliRunner
from snowshu.core.main import cli

#Build the initial test replica from snowshu creds


@pytest.fixture(scope="session")
def run_snowshu_create():
    runner=CliRunner()
    configuration_path=os.path.join(PACKAGE_ROOT,'snowshu','templates','replica.yml')
    return runner.invoke(cli,('run','--replica-file',configuration_path)).output.split('\n')

def test_reports_full_catalog_start(run_snowshu_create):
    result_lines=run_snowshu_create
    assert 'Assessing full catalog...' in result_lines[2]

def test_finds_7_relations(run_snowshu_create):
    result_lines=run_snowshu_create
    assert 'Identified a total of 7 relations to sample based on the specified configurations.' in result_lines[4]


def test_replicates_order_items(run_snowshu_create):
    result_lines=run_snowshu_create
    assert 'Done replication of relation SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS' in result_lines[-3]   

@pytest.mark.skip 
def test_snowshu_explain(run_snowshu_create):
    runner= CliRunner()
    response=json.loads(runner.invoke(cli,('explain','integration-test','--json')))

    assert response['name'] == 'integration-test'
    assert response['image'] == 'postgres:12'
    assert response['target_adapter'] == 'postgres'
    assert response['source_adapter'] == 'snowflake'
    assert datetime(response['created_at']) < datetime.now()

@pytest.mark.skip
def test_starts(run_snowshu_create):
    runner=CliRunner()
    response=runner.invoke(cli,'start','integration-test')
    assert 'Replica integration-test started.' in response.output
    assert 'You can connect to this replica with connection string: postgresql://snowshu:snowshu@snowshu:9999/snowshu' in response.output
    assert 'To spin down your replica, use command `snowshu stop integration-test`' in response.output

    conn=create_engine('postgresql://snowshu:snowshu@snowshu:9999/snowshu')
    q=conn.execute('SELECT COUNT(*) FROM "SNOWSHU_DEVELOPMENT"."EXTERNAL_DATA"."ADDRESS_REGION_ATTRIBUTES"')
    count=q.fetchall()[0][0]
    assert count > 100

@pytest.mark.skip
def test_stops(run_snowshu_create):
    pass    

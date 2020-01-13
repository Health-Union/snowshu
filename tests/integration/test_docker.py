import py.test
from snowshu.core.docker import SnoShuDocker
from snowshu.adapters.target_adapters import PostgresAdapter

def test_creates_replica():
    # build image
    # load it up with some data
    # convert it to a replica
    # spin it all down
    # start the replica
    # query it and confirm that the data is in there

    shdocker=SnowShuDocker()
    target_adapter=PostgresAdapter()
    target_container=shdocker.startup(
                        target_adapter.DOCKER_IMAGE,
                        target_adapter.DOCKER_START_COMMAND,
                        9999,
                        target.name,
                        
                        ['POSTGRES_USER=snowshu',
                         'POSTGRES_PASSWORD=snowshu',
                         'POSTGRES_DB=snowshu',])
    test_name, test_table =[rand_string(10) for _ in range(2)]


    ## load test data
    engine=create_engine('postgresql://snowshu:snowshu@snowshu_target:9999/snowshu')
    engine.execute('CREATE TABLE {test_table} (column_one VARCHAR, column_two INT)')
    engine.execute("INSERT INTO {test_table} VALUES ('a',1), ('b',2), ('c',3)")
    
    replica=shdocker.convert_container_to_replica(target_container, 
                                                  test_name,
                                                  target)
    
                                                                        

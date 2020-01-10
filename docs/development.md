## Developing SnowShu

Contributing to SnowShu is designed to be easy. 

1. To get started, create a fork of official repo [here](url_needed).
2. Clone your fork to your local machine. We suggest naming your own fork `origin` and the official repo `upstream` (and we will use these names in the docs).
3. Start your dev environment with 
```
docker-compose up -d
```
4. Shell into your dev environment with 
```
docker-compose exec snowshu /bin/bash
```
*A Note On Preventing Docker-Ception:* Snowshu uses docker to create and run replicas, and as such developing _in_ docker while using docker creates potential challenges. There is a great debate around the use of docker-in-docker (docker-ception). For the purposes of developing SnowShu we prevent docker-ception by mounting the host docker.sock into the container, creating a "sideways" relationship between the development container and new spawns. This means you can `docker ps` on your host machine and see containers spawned from _inside_ the dev container. It also creates some tricky bits around resolving hostnames, but beats the potential mess of nesting-doll development.

### Branching and PRing
We use a forking model. So when working on a new feature:
1. Check out a new branch with the GitHub issue # and the name (ie `ISSUE-444-fix-banana-stand`)
2. Do work and push to _your_ origin. 
3. When ready create a PR from `origin` against the `upstream` development branch (master).  

### Testing

Since SnowShu is a tool for enabling TDD in data development, you can expect that SnowShu development leans heavily toward TDD. `pytest-cov` is included in dev requirements, so your dev cycle can easily be:
1. Edit or author new test files in `tests/unit` and `tests/integration` to cover the feature you are working on.
2. Run `pytest --cov=snowshu tests/` to see them fail.
3. Edit the application code.
4. Run tests again and edit application code again until all tests pass.
5. Commit. Repeat.  

#### Integration Testing
Integration tests require live test sources (ie Redshift, Snowflake, Bigquery instances). These tests will look for a trail-path.yml and credentials.yml in the folder `/tests/assets/integration` (by default these files are included in the `.gitignore`). Name each of the profiles after the source to be tested.

**TEST DATA** 
integration test data is included in the `/tests/assets/data` folder. The folder structure of the test data reflects how the integration tests are expecting the source db to be structured (ie `database=SNOWSHU_DEVELOPMENT` means the database name should be... you guessed it, `SNOWSHU_DEVELOPMENT`). Data loading will vary by source data system, but you can start with the `integration_test_setup.py` script located in `/tests/assets/` folder. 

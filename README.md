# SnowShu
_by Health Union_

![Coverage](https://bitbucket.org/healthunion/snowshu/downloads/coverage.svg)
![Pipeline](https://bitbucket.org/healthunion/snowshu/downloads/pipeline.svg)

SnowShu enables data developers to author transforms and models in a highly performant, fully local environment. It is designed to make true red-green TDD not only possible for data development, but painless and blazingly fast.

![SnowShu Logo](docs/assets/snowshu_logo.png)

**Note:** SnowShu requires Docker, if you have not done so first [install Docker](https://docs.docker.com/install/).

To get started install with 

```
pip install snowshu

```
Then create new `replica.yml` and `credentials.yml` files with 

```

snowshu init

```

Configure your `replica.yml` and `credentials.yml` files, then execute with 

```

snowshu create

```

and your replica is ready for use! 


Check out the full documentation [here](https://snowshu.readthedocs.org)

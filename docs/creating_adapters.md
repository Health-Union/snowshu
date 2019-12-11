## Creating a SnowShu Adapter

SnowShu is designed to be easy to extend via creation of new adapters. Adapter modules live in their respective parent [source](./sources), [target](./targets) or [storage](./storage) modules. 
You can get most of what your adapter needs by extending the `BaseAdapter` class found in each of these modules. 

*A note on naming*: your adapter name will be the snake-case representation of the model and class you create. for example: 

* `sources.snowflake_adapter` class `SnowflakeAdapter` will have an adapter name in trail-path.yml of `snowflake`
* `targets.sql_server_adapter` class `SqlServerAdapter` will have an adapter name in trail-path.yml of `sql_server`

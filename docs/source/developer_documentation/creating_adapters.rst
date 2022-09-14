=============================
Creating New SnowShu Adapters
=============================

SnowShu is designed to be easy to extend via creation of new adapters. Adapter modules live in their respective parent `source <'https://bitbucket.org/healthunion/snowshu/src/master/snowshu/adapters/source_adapters/'>`__, `target <'https://bitbucket.org/healthunion/snowshu/src/master/snowshu/adapters/target_adapters/'>`__  and `sampling <'https://bitbucket.org/healthunion/snowshu/src/master/snowshu/samplings/'>`__  modules.


*A note on naming*: your adapter name will be the snake-case representation of the model and class you create. for example: 

  Class ``SnowflakeAdapter`` will have an adapter name in ``replica.yml`` of ``snowflake``.
  Class ``SqlServerAdapter`` will have an adapter name in ``replica.yml`` of ``sql_server``.



Adapter-Type Specific Documentation 
===================================
 
.. toctree::
   
   creating_a_sampling_adapter
   creating_a_source_adapter
   creating_a_target_adapter



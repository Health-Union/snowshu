.. _replica.yml:

========================
The replica.yml File
========================

Building a SnowShu replica is essentially configured in a single file. 

.. note:: The replica.yml file can be named any valid ``.yml`` file, by default SnowShu will look for a ``replica.yml`` file in the execution directory. To specify the file name use the ``--replica-file`` flag. 
 

Sample replica.yml File
=====================================

.. code-block:: yaml
   
   version: "1"
   credpath: "/snowshu/credentials.yml" 
   name: development-replica
   short_description: Used for end-to-end testing
   long_description: This replica includes our full test suite with simple sampling.
   threads: 15
   target:
     adapter: postgres
   source:
     profile: default
     sampling: default
     include_outliers: True
     general_relations:
       databases:
       - pattern: SNOWSHU_DEVELOPMENT
         schemas:
         - pattern: '.*'
           relations:
           - '^(?!.+_VIEW).+$'
     specified_relations: 
     - database: SNOWSHU_DEVELOPMENT
       schema: SOURCE_SYSTEM
       relation: ORDERS
       unsampled: True
     - database: SNOWSHU_DEVELOPMENT
       schema: SOURCE_SYSTEM
       relation: '^ORDER_ITEMS$'
       relationships:
         bidirectional: 
         - local_attribute: PRODUCT_ID 
           database: '' 
           schema: ''
           relation: PRODUCTS
           remote_attribute: ID
         directional: 
         - local_attribute: ORDER_ID
           database: SNOWSHU_DEVELOPMENT
           schema: SOURCE_SYSTEM
           relation: ORDERS
           remote_attribute: ID
     - database: SNOWSHU_DEVELOPMENT
       schema: EXTERNAL_DATA
       relation: SOCIAL_USERS_IMPORT
       sampling:
         default:
           margin_of_error: 0.05
           confidence: 0.95
          min_sample_size: 300


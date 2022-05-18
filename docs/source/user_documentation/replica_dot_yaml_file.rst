.. _replica.yml:

========================
The replica.yml File
========================

Building a SnowShu replica is essentially configured in a single file. 

.. note::
  The replica.yml file can be named any valid ``.yml`` file, by default SnowShu will look for a ``replica.yml`` file
  in the execution directory. To specify the file name use the ``--replica-file`` flag.
 

Sample replica.yml File
=====================================

Your initial replica file will look something like `this
<https://github.com/Health-Union/snowshu/blob/master/snowshu/templates/replica.yml>`_
(*hint*: you can run ``snowshu init`` to get a generated sample file):

.. code-block:: yaml
   
   version: "1"
   credpath: "/snowshu/credentials.yml" ## can be a relative or expandable path
   name: development-replica
   short_description: Used for end-to-end testing
   long_description: This replica includes our full test suite with simple sampling.
   threads: 15
   target:
     adapter: postgres
     adapter_args:
      pg_extensions:
        - citext
   source:
     profile: default
     sampling: default
     include_outliers: True
     general_relations:
       databases:
       - pattern: SNOWSHU_DEVELOPMENT
         schemas:
         - pattern: "(?i)(EXTERNAL_DATA|SOURCE_SYSTEM|TESTS_DATA)"
           relations:
           - '(?i)^.*(?<!_view)$' # matches all relations that do not end with '_VIEW'
     # these are exceptions to the 'default' sampling above
     specified_relations: 
     - database: SNOWSHU_DEVELOPMENT
       schema: SOURCE_SYSTEM
       relation: ORDERS
       unsampled: True
     - database: SNOWSHU_DEVELOPMENT
       schema: SOURCE_SYSTEM
       relation: ORDER_ITEMS_VIEW
       unsampled: True
     - database: SNOWSHU_DEVELOPMENT
       schema: TESTS_DATA
       relation: DATA_TYPES
       unsampled: True
        
     - database: SNOWSHU_DEVELOPMENT
       schema: SOURCE_SYSTEM
       relation: ORDER_ITEMS
       relationships:
         bidirectional: 
         - local_attribute: PRODUCT_ID 
           database: '' ## empty strings inherit from the parent relation
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
       schema: SOURCE_SYSTEM
       relation: USER_COOKIES
       relationships:
         bidirectional: 
         - local_attribute: USER_ID 
           database: '' ## empty strings inherit from the parent relation
           schema: ''
           relation: USERS
           remote_attribute: ID
     - database: SNOWSHU_DEVELOPMENT
       schema: EXTERNAL_DATA
       relation: SOCIAL_USERS_IMPORT
       sampling:
         default:
           margin_of_error: 0.05
           confidence: 0.95
           min_sample_size: 300
           max_allowed_rows: 1500000
     - database: SNOWSHU_DEVELOPMENT
       schema: POLYMORPHIC_DATA
       relation: PARENT_TABLE
       relationships:
         polymorphic:
           - local_attribute: CHILD_ID
             local_type_attribute: CHILD_TYPE
             database: ''
             schema: ''
             relation: '(?i)^CHILD_TYPE_[0-9]_ITEMS$'
             remote_attribute: ID
             local_type_overrides:
               - database: SNOWSHU_DEVELOPMENT
                 schema: POLYMORPHIC_DATA
                 relation: CHILD_TYPE_2_ITEMS
                 override_value: type_2
     - database: SNOWSHU_DEVELOPMENT
       schema: POLYMORPHIC_DATA
       relation: PARENT_TABLE_2
       relationships:
         polymorphic:
           - local_attribute: ID
             database: SNOWSHU_DEVELOPMENT
             schema: POLYMORPHIC_DATA
             relation: '(?i)^CHILD_TYPE_[0-9]_ITEMS$'
             remote_attribute: PARENT_2_ID

This file tells SnowShu all kinds of things, including: 
- which relations (tables, views etc) to sample
- where relationships exist between relations
- what type of target replica to use
- how to go about sampling


Anatomy of replica.yml
======================

The file consists of 2 primary sections, the header and the source. 

Header
------

The header section of the ``replica.yml`` file is basically everything that is *not* part of the ``source`` directive. 
In our example, this would be the header:

.. code-block:: yaml
   
   version: "1"
   credpath: "/snowshu/credentials.yml" 
   name: development-replica
   short_description: Used for end-to-end testing
   long_description: This replica includes our full test suite with simple sampling.
   threads: 15
   target:
     adapter: postgres
     adapter_args:
      pg_extensions:
        - uuid-ossp

Let's disect each of the components:

- **version** (*Required*) is the replica file version, and tells SnowShu how to consume this file. Currently it should always be set to ``1``.
- **credpath** (*Required*) is the path to a valid ``credentials.yml`` file (where the source database secrets are kept). Can be relative or absolute.
- **name** (*Required*) will translate to the final name of the replica to be generated. The name should be short and distinctive. 
- **short_description** (*Optional*) tells users a little bit about the replica you are creating.
- **long_description** (*Optional*) provides users with a detailed explanation of the replica you are creating.
- **threads** (*Optional*) tells SnowShu the max number of threads that can be used when multiprocessing. When not set SnowShu may run much slower :(. 
- **target** (*Required*) Specifies the adapter to use when creating a replica.

  - **adapter** (*Required*) For Snowflake, BigQuery and Redshift this should be ``postgres``.
  - **adapter_args** (*Optional*) Some targets may require additional configuration, especially when emulating a different source type. These keys and values are specific to the target type. Currently, only `pg_extensions` is supported.

Source
------

The source section of the ``replica.yml`` file is "where the magic happens". This section is comprised of 3 parts:
- the overall source settings
- the general sampling configuration
- the specified sampling configurations

Overall Source Settings
^^^^^^^^^^^^^^^^^^^^^^^

In our example, this portion of the source directive would be the overall source settings:
 
.. code-block:: yaml
   
   ... 
   source:
     profile: default
     sampling: default
     include_outliers: True

The components of the overall source settings, dissected:

- **profile** (*Required*) is the name of the profile found in ``credentials.yml`` to execute with. In this example we are using a profile named "default".
- **sampling** (*Required*) is the name of the sampling method to be used. Samplings combine both the number of records sampled and the way in which they are selected. Current sampling options are ``default`` (uses Bernoulli sampling and Cochran's sizing), or ``brute_force`` (Uses a fixed % and Bernoulli).
- **include_outliers** (*Optional*) determines if SnowShu should look for records that do not respect specified relationships, and ensure they are included in the sample. Defaults to False. 
- **max_number_of_outliers** (*Optional*) specifies the maximum number of outliers to include when they are found. This helps keep a bad relationship (such as an incorrect assumption on a trillion row table) from exploding the replica. Default is 100. 


.. relations in _replica.yml:

================================
Relations in replica.yml
================================

General Sampling Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With your overall source settings configured, you can set your *general* sampling configuration. The general sampling is the most broad (and least configurable) data sampling hierarchy. For example: 

.. code-block:: yaml

   ...
   general_relations:
     databases:
     - pattern: SNOWSHU_DEVELOPMENT
       schemas:
       - pattern: "(?i)(EXTERNAL_DATA|SOURCE_SYSTEM|TESTS_DATA)"
         relations:
         - '^(?i).*(?<!_view)$'

General relations accepts a nested structure of **database->schema(s)->relation(s)**.
The configuration accepts both plain text relation names and regex strings (python re syntax).
For example, the regex pattern above matches all relations (tables and views) in the database ``SNOWSHU_DEVELOPMENT``
in specific schemas, where the name does not end in "VIEW" (or "view", "vIew" etc).

.. note::
  Error in specifying the correct regex is a common mistake here.

  * Python regular expression operation `fullmatch <https://docs.python.org/3/library/re.html#re.fullmatch>`_
    is used to filter out the relations.
  * Please take note of the case of the database object or handle case inside the regex using ``(?i)``.
  * In yaml string input can be given with single, double or no quotes.

This nested pattern of relations follows all the specs outlined in the `Overall Source Settings`_.

Specified Sampling Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The specified sampling configurations are the most... specific. If a relation appears in both the general
sampling configuration and a specified sampling configuration, the specified sampling will win out.
They are also evaluated top-down, so a relation appearing in more than one specified configuration will
have either the cumulative value (for relationships) or the last value (for flags).

Specified relations look like this: 

.. code-block:: yaml

   ...
   specified_relations: 
     - database: SNOWSHU_DEVELOPMENT
       schema: SOURCE_SYSTEM
       relation: ORDERS
       unsampled: True
     - database: SNOWSHU_DEVELOPMENT
       schema: SOURCE_SYSTEM
       relation: ORDER_ITEMS_VIEW
       unsampled: True
     - database: SNOWSHU_DEVELOPMENT
       schema: TESTS_DATA
       relation: DATA_TYPES
       unsampled: True
        
     - database: SNOWSHU_DEVELOPMENT
       schema: SOURCE_SYSTEM
       relation: ORDER_ITEMS
       relationships:
         bidirectional: 
         - local_attribute: PRODUCT_ID 
           database: '' ## empty strings inherit from the parent relation
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
       schema: SOURCE_SYSTEM
       relation: USER_COOKIES
       relationships:
         bidirectional: 
         - local_attribute: USER_ID 
           database: '' ## empty strings inherit from the parent relation
           schema: ''
           relation: USERS
           remote_attribute: ID
     - database: SNOWSHU_DEVELOPMENT
       schema: EXTERNAL_DATA
       relation: SOCIAL_USERS_IMPORT
       sampling:
         default:
           margin_of_error: 0.05
           confidence: 0.95
          min_sample_size: 300
     - database: SNOWSHU_DEVELOPMENT
       schema: POLYMORPHIC_DATA
       relation: PARENT_TABLE
       relationships:
         polymorphic:
           - local_attribute: CHILD_ID
             local_type_attribute: CHILD_TYPE
             database: ''
             schema: ''
             relation: '(?i)^CHILD_TYPE_[0-9]_ITEMS$'
             remote_attribute: ID
             local_type_overrides:
               - database: SNOWSHU_DEVELOPMENT
                 schema: POLYMORPHIC_DATA
                 relation: CHILD_TYPE_2_ITEMS
                 override_value: type_2
     - database: SNOWSHU_DEVELOPMENT
       schema: POLYMORPHIC_DATA
       relation: PARENT_TABLE_2
       relationships:
         polymorphic:
           - local_attribute: ID
             database: SNOWSHU_DEVELOPMENT
             schema: POLYMORPHIC_DATA
             relation: '(?i)^CHILD_TYPE_[0-9]_ITEMS$'
             remote_attribute: PARENT_2_ID

Each specified relation must have the following: 

- **database** (*Required*) is the name or valid regex for the specified relation database.
- **schema** (*Required*) is the name or valid regex for the specified relation schema.
- **relation** (*Required*) is the name or valid regex for the specified relation.

.. note:: specified relations can represent one or many many relations, based on the pattern provided. 

They can then contain one or more of these options:
- **unsampled** (*Optional*) tells SnowShu to pull the entire relation. Good for tiny reference tables, very bad for big stores of data.
- **sampling** (*Optional*) allows you to override the higher-level configuration and set specifics for that sampling.

The primary use of specified relations is to create relationships. This is accomplished through the ``relationships`` directive of a specified relation.

A Relationships Primer
""""""""""""""""""""""

One of the more gnarly parts of generating sample data for testing is the issue of `referential integrity.
<https://en.wikipedia.org/wiki/Referential_integrity>`__. Say you have a table,
say ``USERS``, and another table ``ORDERS`` with a column ``user_id`` in it.
In the full data set, every row of ``ORDERS`` will have a valid ``user_id`` from the ``USERS``
table - and you can test your software by checking to make sure your final output of ``ORDERS`` has a valid
``user_id`` that can be found in ``USERS``. However, when we sample this is no longer the case.
Not all the rows selected by the sample from one table can be referenced by the other - and this breaks our tests.

SnowShu handles this complexity by enforcing relationships. 

- A **directional** relationship is where the records for one table (``ORDERS`` in the example above) must have referential integrity to another (``USERS``). 
- A **bidirectional** relationship is where both tables must have referential integrity to each other (ie ``USER_ADDRESSES`` and ``USERS`` must only have references that exist in each other). 
- A **polymorphic** relationship is where a record for one table has referential integrity to one of multiple tables (ie ``CHILD_TYPE_2_ITEMS`` and ``PARENT_TABLE`` must only have references that exist in each other). 

Specified relations can have more than one of each type of relationship. For each relationship the following must be defined:

- **database** (*Required*) is the name or valid regex for the database that the specified relation will have a relationship with, or a blank string (more on that below).
- **schema** (*Required*) is the name or valid regex for the schema that the specified relation will have a relationship with, or a blank string (more on that below).
- **relation** (*Required*) is the name or valid regex for the relation that the specified relation will have a relationship with, or a blank string (more on that below).
- **local_attribute** (*Required*) is the name of the column in the specified relation that has an fkey relationship. Cannot be regex, needs to be the actual column name.
- **remote_attribute** (*Required*) is the name of the column in the relation that the specified relation has an fkey relationship with. Cannot be regex, needs to be the actual column name.
- **local_type_attribute** (*Optional*) is the name of the column in the matched specified relations that has an fkey relationship. It specifies the table that the other attribute is supposed to match to.
- **local_type_overrides** (*Optional*) provides a value to override the `local_type_attribute` of a specific relation match.

So in this example: 


.. code-block:: yaml

   ...
    - database: SNOWSHU_DEVELOPMENT
      schema: SOURCE_SYSTEM
      relation: ORDER_ITEMS
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


The *specified relation* is ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS``. When SnowShu builds this replica:

- All the records in ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS`` will be records with a ``product_id`` found in ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.PRODUCTS``.
- All the records in ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.PRODUCTS`` will be records with an ``id`` found in ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS``.
- All the records in ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS`` will be records with an ``order_id`` found in ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDERS``.
- The records in ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDERS`` *may* be records with an ``id`` not found in ``SNOWSHU_DEVELOPMENT.SOURCE_SYSTEM.ORDER_ITEMS``. 

The example below shows that of polymorphic relationships: 


.. code-block:: yaml

   ...
    - database: SNOWSHU_DEVELOPMENT
      schema: POLYMORPHIC_DATA
      relation: PARENT_TABLE_2
      relationships:
        polymorphic:
          - local_attribute: ID
            database: SNOWSHU_DEVELOPMENT
            schema: POLYMORPHIC_DATA
            relation: '(?i)^CHILD_TYPE_[0-9]_ITEMS$'
            remote_attribute: PARENT_2_ID


The *specified relation* is ``SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.PARENT_TABLE_2`` which relates with a child relations ``SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.(?i)^CHILD_TYPE_[0-9]_ITEMS$``. When SnowShu builds this replica:

- All the records in ``SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.PARENT_TABLE_2`` will be records with a ``ID`` found in the ``PARENT_2_ID`` of any tables that match ``SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.(?i)^CHILD_TYPE_[0-9]_ITEMS$`` (example: ``PARENT_2_ID`` in ``SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.CHILD_TYPE_2_ITEMS``).

.. code-block:: yaml

   ...
    - database: SNOWSHU_DEVELOPMENT
      schema: POLYMORPHIC_DATA
      relation: PARENT_TABLE
      relationships:
        polymorphic:
          - local_attribute: CHILD_ID
            local_type_attribute: CHILD_TYPE
            database: ''
            schema: ''
            relation: '(?i)^CHILD_TYPE_[0-9]_ITEMS$'
            remote_attribute: ID
            local_type_overrides:
              - database: SNOWSHU_DEVELOPMENT
                schema: POLYMORPHIC_DATA
                relation: CHILD_TYPE_2_ITEMS
                override_value: type_2


The *specified relation* is ``SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.PARENT_TABLE`` which relates with a child relations ``SNOWSHU_DEVELOPMENT.POLYMORPHIC_DATA.(?i)^CHILD_TYPE_[0-9]_ITEMS$``. When SnowShu builds this replica:

- All the records in the parent table will be records that have a ``CHILD_TYPE`` (``local_type_attribute`` column) value that matches the child table name (or the ``local_type_overrides`` for the child table if used) and a ``CHILD_ID`` value that matches ``ID`` value in the matching table.


*A note on empty strings in relationships:* When specifying a relationship, SnowShu will interpret empty strings in the database or schema to inherit from the specified relation under test. For example:

.. code-block:: yaml

   ...
    - database: '[hamburger|hotdog]'
      schema: '[socks|shoes]'
      relation: giraffes
      relationships:
        bidirectional: 
        - local_attribute: id
          database: '' 
          schema: ''
          relation: condiments
          remote_attribute: giraffe_id

This will evaluate to:

- ``hamburger.socks.giraffes`` will be related to ``hamburger.socks.condiments``
- ``hotdog.socks.giraffes`` will be related to ``hotdog.socks.condiments``
- ``hamburger.shoes.giraffes`` will be related to ``hamburger.shoes.condiments``

etc etc. 

Case (In)Sensitivity In Relations
=================================

.. Important:: **TLDR;** In SnowShu replica files, identifiers are case insensitive unless:

   - they are mixed case (ie ``CamelCase``)
   - they contain a space (ie ``Space Case``)
   - they are specified by a regex string
   - the global option ``preserve_case:True`` is set.

SQL casing is simple, until it is complex. A general interpretation of the spec is that identifiers (such as table names, schema names and column names) should behave in a case-insensitive way; that is to say that ``USER_TABLE`` and ``user_table`` should both query the same table when written in SQL. 

Most databases accomplish this case insensitivity by "folding", or selecting a case and casting all identifiers to that case. The challenge is that not all databases fold in the same direction. The debate of which way databases *should* fold is not one we will have here (the spec calls for uppercase, but that is not universally adopted). 

SnowShu does the best it can to interpret your "intentions". If you specify ``USERS`` or ``users``, (all one case) it will read that as case insensitive and grab either version in the source database. It will use the native default casing in the target database, so you can continue to write either form in your code without using double quotes. 

In situtations where you specified a mixed casing like ``Users``, SnowShu interprets this as intentional and will preserve the case. This means you will need to wrap the identifier in double quotes when querying for it. This is also true for situations where a space is included in the identifier.

Regex strings are interpreted exactly as-is. So if you want a case-insensitive regex string, you need to set that in the regex (ie ``(?i)``).

You can also force the native source case to persist all the way to the target. This is great if your entire source is full of mixed cases and spaces, but is otherwise a generally bad idea. 
Set this flag in the `Overall Source Settings`_ with ``preserve_case: True``. 

.. Warning:: It is usually a very bad idea to preserve case. SQL architectures generally depend heavily on the case-insensitive nature of the language, and breaking this means every single indentifier will likely need to be quoted in code *and* queries.
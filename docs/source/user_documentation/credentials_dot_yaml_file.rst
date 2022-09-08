The credentials.yml File
========================

.. Caution:: Your ``credentials.yml`` file will contain sensitive information. Always store it in a safe place and *never* in source control.

The ``credentials.yml`` file should be configured and moved to a safe place (like a `~/.snowshu` directory).

With your ``credentials.yml`` file relocated you can `configure your replica <replica_dot_yaml_file.html#configure-your-replica>`__.

Sample credentials.yml File
---------------------------

Your initial credentials file will look something like `this
<https://github.com/Health-Union/snowshu/blob/master/snowshu/templates/credentials.yml>`_
(*hint*: you can run ``snowshu init`` to get a generated sample file):

.. code-block:: yaml
   
   version: '1'
   sources:
   - name: default
     adapter: snowflake
     account: kic1992.us-east-1
     user: frosty
     password: "such_secure_password"
     database: "snowshu"


Configure your credentials
--------------------------

The file consists of version and sources components.

Let's disect each of the components:

- **version** (*Required*) is the replica file version, and tells SnowShu how to consume this file. Currently it should always be set to ``1``.

The components of the overall sources settings, dissected:

- **name** (*Required*). It can be set to ``default``. 
- **adapter** (*Required*). It should always be set to ``snowflake``.
- **account** (*Required*). It's an account identifier that uniquely identifies a Snowflake account within your organization. For example, the URL for an account uses the following format: ``<account_identifier>.snowflakecomputing.com``
- **user** (*Required*) is a user login name used to connect or log into the Snowflake web interface. 
- **password** (*Required*) is a user password used to connect or log into the Snowflake web interface.
- **database** (*Required*) specifies the DataBase name to use.
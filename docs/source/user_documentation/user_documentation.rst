==================
User Documentation
==================
.. toctree::
   :titlesonly:

   faq
   philosophy_statement
   it_only_takes_one
   replica.yml file <replica_dot_yaml_file>
   function_emulations
 
Getting Started
===============
Installing SnowShu
------------------
Snowshu can be installed via pip

>>> pip3 install snowshu

or built `from source <'https://bitbucket.org/healthunion/snowshu/src/master/'>`__ via setup install. 

Note that SnowShu uses Docker build replicas, so **if you don't already have Docker installed you will need to do that first**.
You can download and install the latest version of Docker Desktop `here <'https://docs.docker.com/install/'>`__.

Setting Up SnowShu
------------------
Once you have installed SnowShu you will want to create a `replica.yml <replica_dot_yaml_file.html>`__ for your project. Creating yaml files from scratch is no fun, so 
SnowShu comes with a built-in helper command to get you started.

>>> snowshu init

This will create 2 files, ``replica.yml`` and ``credentials.yml``. 

.. Caution:: Your ``credentials.yml`` file will contain sensitive information. Always store it in a safe place and *never* in source control.

Move your ``credentials.yml`` file to a safe place (like a `~/.snowshu` directory) and `configure your credentials <credentials_dot_yaml_file.html#configure-your-credentials>`__.

With your ``credentials.yml`` file relocated you will then want to `configure your replica <replica_dot_yaml_file.html#configure-your-replica>`__.

Now you probably want to see how well the replica settings will work in practice. You can do this with the ``analyze`` command, like this:

>>> snowshu analyze 

This will output the proposed relations and sampling sizes. You can tweak your ``replica.yml`` file until you are satisifed with your analyze output.

Creating A Replica
------------------
When you are ready, you can create your replica with 

>>> snowshu create

SnowShu will report details of the created replica once completed. 

.. image:: /../assets/completed_replica.png 

Using Your Replica
------------------

Now that you have a replica you will likely want to start it. You can use normal ``docker run`` commands with a replica image, no special context required. 
Note that **all replicas use port 9999 by default**.
To make docker startup easier snowshu comes with ``launch-docker-cmd`` which takes the replica name as an argument. So for a replica named *hamburger-sandwich*: 

>>> snowshu launch-docker-cmd  hamburger-sandwich
docker run -d -p 9999:9999 --rm --name hamburger-sandwich snowshu_replica_hamburger-sandwich

When running in bash you can easily wrap this command to execute, ie

>>> $(snowshu launch-docker-cmd hamburger-sandwich)

Now you can connect to the replica using a standard connection string. 

.. note:: ``snowshu`` is the default username, password and database for all replicas. 9999 is the port. These cannot be changed, `for good reason <faq.html#why-cant>`__
 









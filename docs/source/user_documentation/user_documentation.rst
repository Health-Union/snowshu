==================
User Documentation
==================

.. Tip:: You can now join the conversation on `Slack <'https://join.slack.com/t/snowshu/shared_invite/enQtOTcwNTA4MDk1Mzc2LTE5YzhkZTZjNDFkYmIzY2RkNDE4MDFiMzBhYTQxZWJhNzA5ZDgyZjY4ODZkM2RhZmY5Njc0OGQ2MjEyNTEzZTU'>`__!



.. toctree::
   :titlesonly:

   faq
   replica.yml file <replica_dot_yaml_file>
   philosophy_statement
   it_only_takes_one
   writing_cross_database_sql
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

Using SnowShu in Docker
-----------------------
Running SnowShu inside a Docker container is easy and solves a _lot_ of environmental problems. To get started, change directories to the directory where you will keep your ``replica.yml`` file.

*Hint*: this can be a distinct project just for making SnowShu replicas, but is probably easier to maintain inside the project repository you will use SnowShu with. For example, if you are using SnowShu to speed testing of a DBT project, you will want to run these commands in the root folder of that project. 

Once you are in the correct directory, run this docker command to generate your ``replica.yml`` and ``credentials.yml`` templates:

>>> docker run --network snowshu --rm -v /var/run/docker.sock:/var/run/docker.sock -v ${PWD}:/workspace hutech/snowshu init 

The ``docker.sock`` mount is so your container can use the running docker daemon on the metal of your machine. 

You should now have template files: 

>>> ls
replica.yml
credentials.yml
... # other files already in the folder

Configure your `replica.yml <replica_dot_yaml_file.html>`__ and ``credentials.yml`` files. 

.. warning:: If you are keeing the ``credentials.yml`` file in your project repository, don't forget to add it to your ``.gitignore`` file before you commit. Otherwise you could share passwords with the world by accident, which would be bad. 

You can now create replicas from these files with 

>>> docker run --network snowshu --rm -v /var/run/docker.sock:/var/run/docker.sock -v ${PWD}/replica.yml:/workspace/replica.yml -v ${PWD}/credentials.yml:/workspace/credentials.yml hutech/snowshu create 

This will create the replica. To confirm, check your images:

>>> docker image ls -a 
snowshu_replica_whatever_you_named_your_image

You can now start the replica with:

>>> $(docker run --network snowshu --rm -v /var/run/docker.sock:/var/run/docker.sock hutech/snowshu launch-docker-cmd <whatever_you_named_your_replica>)

Using docker-compose
--------------------
The above commands can get a little laborious. To do your work inside a configured container instead, you can use this docker-compose.yml file. 

.. code:: yaml
   
   ## docker-compose.yml
   version: "3.5"
   services:
     snowshu:
       image: hutech/snowshu
       volumes:
         - .:/workspace
         - /var/run/docker.sock:/var/run/docker.sock
       command: tail -f /dev/null
       networks:
         - snowshu
   networks:
     snowshu:
       name: snowshu
       driver: bridge

Then jump in with 

>>> docker-compose up -d && docker-compose exec snowshu /bin/bash

and you can run all your SnowShu commands from inside. 


Setting Up SnowShu On The Metal
-------------------------------
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

Creating An Incremental Replica
------------------

There's a simple way to rebuild an existing replica in case the ``replica.yml`` file has been changed. Instead of building a brand new replica, you can apply changes to the existing one.
Incremental replica creates relations and loads data only for new entries found in ``replica.yml`` file, which are not already present in target replica image.
The target for the incremental replica is actually a docker image name, which you can find by typing: 

>>> docker images

It usually starts with ``snowshu_replica_``. So for a replica named *hamburger-sandwich*:

>>> snowshu create -i snowshu_replica_hamburger-sandwich

or

>>> snowshu create --incremental snowshu_replica_hamburger-sandwich

Once completed you'll get a report with details of the replica with updated relations. 

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
 










Getting Started
===============
Installing SnowShu
------------------
Snowshu can be installed via pip

>>> pip3 install snowshu

or built `from source <https://bitbucket.org/healthunion/snowshu/src/master/>`__ via setup install. 

Note that SnowShu uses Docker build replicas, so **if you don't already have Docker installed you will need to do that first**.
You can download and install the latest version of Docker Desktop `here <https://docs.docker.com/install/>`__.

Using SnowShu in Docker
-----------------------
Running SnowShu inside a Docker container is easy and solves a _lot_ of environmental problems. To get started, change directories to the directory where you will keep your ``replica.yml`` file.

*Hint*: this can be a distinct project just for making SnowShu replicas, but is probably easier to maintain inside the project repository you will use SnowShu with. For example, if you are using SnowShu to speed testing of a DBT project, you will want to run these commands in the root folder of that project. 

Once you are in the correct directory, run this docker command to generate your ``replica.yml`` and ``credentials.yml`` templates:

>>> docker run --network snowshu --rm -v /var/run/docker.sock:/var/run/docker.sock -v ${PWD}:/workspace healthunion/snowshu init 

The ``docker.sock`` mount is so your container can use the running docker daemon on the metal of your machine. 

You should now have template files: 

>>> ls
replica.yml
credentials.yml
... # other files already in the folder

Configure your `replica.yml <replica_dot_yaml_file.html>`__ and `credentials.yml <credentials_dot_yaml_file.html>`__ files. 

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

.. note::
  Using the ``--retry-count`` or ``-r`` flag the value of DEFAULT_RETRY_COUNT parameter can be set during the build process. By default the number of times to retry failed query is set to ``1``.
  For example:
  
  >>> snowshu create -r 3


Creating An Multiple-Architecture Replica
-----------------------------------------

There's a simple way of creating replicas for both main architectures (amd64 and arm64) at the same time.
Just add a flag ``-m`` or ``--multiarch`` to your create command like this:

>>> snowshu create -m

or

>>> snowshu create --multiarch --replica-file path/to/replica.yml

Once completed you'll get a set of 3 replicas with same data but different tags: ``latest``, which is always your native architecture, ``amd64`` and ``arm64``, which are self descriptive.

Creating An Incremental Replica
-------------------------------

There's a simple way to rebuild an existing replica in case the ``replica.yml`` file has been changed. Instead of building a brand new replica, you can apply changes to the existing one.
Incremental replica creates relations and loads data only for new entries found in ``replica.yml`` file, which are not already present in target replica image.
The target for the incremental replica is actually a docker image name, which you can find by typing: 

>>> docker images

It usually starts with ``snowshu_replica_``. So for a replica named *hamburger-sandwich*:

>>> snowshu create -i snowshu_replica_hamburger-sandwich

or

>>> snowshu create --incremental snowshu_replica_hamburger-sandwich

Once completed you'll get the updated replica image with updated relations and the report with details of it.

The ``latest`` tag is applied by default to reference an image, if no version is present. But in case you need to explicitly specify the version of the image, you can include the tag.
For example, in order to use ``1.0.0`` version of the image:

>>> snowshu create -i snowshu_replica_hamburger-sandwich:1.0.0

Incremental replicas now also support ``-m`` flag. By default if you pass a base replica without tag, SnowShu will use the one tagged as ``latest``, but you can force it to use your non-native architecture by supplying specific tag, like so:

>>> snowshu create -i snowshu_replica_hamburger-sandwich:arm64 --multiarch

SnowShu will pull fresh target image of opposite architecture, and clone replica data to it, producing a set of 3 images like in case of standard multiarch build.

Using Special Flags For Verbosity Debug
---------------------------------------

There are special verbosity flags that can be used to determine a verbosity level of debugging.

- ``-v`` or ``--verbosity`` flag set a debug level in core and info level in adapters
- ``-vv`` flag set a debug level in core and adapters
- ``--debug-core`` flag set log level to debug only in core
- ``--debug-adapters`` flag set log level to debug only in adapters
- ``-d`` or ``--debug`` flag set log level to debug everywhere

For example:

>>> snowshu -v create

or:

>>> snowshu -vv create

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
 


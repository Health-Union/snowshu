.. image:: ../assets/snowshu_logo.png
   :alt: SnowShu

=======================
Introduction to SnowShu
=======================

*SnowShu is a sampling engine designed to support testing in data development.* 


Why Sample? 
===========

Statisticians have long understood that it isn’t always practical (or even possible!) to study an entire population. If we wanted to learn what percentage of homeowners in the USA claim to enjoy mowing their lawn, it would be impossible to ask all 136+ million of them! So instead we use a carefully crafted sample, or subset of homeowners that offers a reasonable representation.

.. image:: ../assets/huge_data.png

In data development populations can also be impractical to work with. Say you wrote a few lines of code to add a ``user_type`` value to your web traffic events, and you want to test it out. Running this code against the full population (billions of events!) could take hours. If the code doesn’t behave as expected you would have to start over, each time waiting for it to complete. 

Why Not Just Take Samples?
==========================

Sampling is actually pretty hard. Data developers will sometimes use sample-ish hacks (like testing with the last 3 days’ worth of data, or using a ``sample`` function built into the database) but these can often come back to bite you. Samples need to take into account referential integrity and unevenly distributed data if they are to be any use when testing. 


.. image:: ../assets/sampling.png

Why Share?
==========

When data teams want to collaborate and discuss code, it is helpful to compare apples-to-apples outputs. The only way to really do that is with matching input data, which the shared immutable samples from SnowShu provide.

Why Localize? 
=============
.. image:: ../assets/bottleneck.png

Most modern data stores exist in the cloud. This has plenty of advantages, but the downside is that data must travel great distances between the server and your laptop. This trip is one of the slowest parts of data development. By bringing your sampled data into your local workspace you can eliminate the bottleneck completely.


What Does SnowShu Do? 
=====================

SnowShu makes it easy to create a catalog of sampled data from your larger production data store (usually a cloud data warehouse, data lake or MRFS). SnowShu manages selecting the smallest possible sample that will accurately represent the data store population, while taking into account considerations such as  referential integrity and margin of error. This dataset becomes a SnowShu **replica** - a local emulator that can be queried just like you would the population data store. This replica is an immutable image, so spinning up new replicas becomes trivial. 


How Do I Use SnowShu?
=====================

You install snowshu like this:

>>> pip3 install snowshu


and set up your SnowShu config files like this:

>>> snowshu init

With SnowShu you simply select the source databases, schemas and relations (tables and views) you need for development. SnowShu creates a localized emulator of that data, called a replica, that you can query directly during development. You create your replica like this:

>>> snowshu create

This replica can be instantly available to everyone on your team with a shared image registry (all configured in your initial SnowShu setup), so working from the same initial dataset is a breeze. You can see all your available replicas like this:

>>> snowshu list
our-first-replica
five-percent
ten-percent-no-users

You can start and launch one of the replicas like this:

>>> snowshu launch five-percent

and then connect using your normal sql workflow. By default your replica will be on ``localhost`` port 9999. The default database, schema, user and password are all ``snowshu``. 

User Resources
==============

.. toctree::
   :numbered:

   philosophy_statement
   snowflake_function_emulation

Developer Resources
===================

* :ref:`genindex`
* :ref:`modindex`

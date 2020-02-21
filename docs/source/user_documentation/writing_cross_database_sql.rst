==========================
Writing Cross-Database SQL
==========================

The first question that tends to come up when a data team explores SnowShu is the question of Domain Specific Language (DSL) differences between source and target databases. Put simply: 

    **How do we keep from writing different SQL between source and target databases?**

This is a legit concern; if ``RLIKE(value, expression)`` works on your source database but won't run in the emulation, you could easily waste enough time sorting out differences that the speed gains from using SnowShu will be lost. 

Adding to this is the greater toll that supporting more than one vendor-specific DSL can take on developers. Many data developers have worked within the same vendor ecosystem for their entire career and never have to think if ``RLIKE()`` or ``REGEXP()`` or even ``~`` is the correct syntax for a regex comparison (because they only need to know one). 

The SnowShu theory is:
- writing cross-database SQL is worth it.
- writing cross-database SQL should be almost as easy as writing vendor-specific SQL, if we do it right.

Why Is Cross-Database SQL Worth It?
===================================

It is dangerously easy to become comfortable with technology vendor relationships, and forget that the tools we rely on are subject to change just like the rest of the world. You may love your data warehouse and they may love you back, but tomorrow they may suddenly drop support for semistructured parsing, or sunset the engine you rely on, or triple your credit costs. New offerings will appear (maybe even from the same vendor) that you will want to take advantage of. The whole point of using distributed/cloud/microservice architecture is to avoid "locking in" with technologies that will likely become obsolite. 

Cross-Database SQL removes the vendor commitment from your code, making your business logic portable and resiliant to change. 

How Do We Make Cross-Database SQL Easy?
=======================================

If you are writing in-database transform logic using ELT, a tool like `DBT <https://www.getdbt.com/>`__ with the `xdb package <https://github.com/Health-Union/dbt-xdb>`__ helps solve this problem. xdb allows DBT to precompile SQL based on the target database, replacing generic macros with vendor-specific dialects. So SQL code like this:

.. code-block:: SQL

   SELECT 
        {{xdb.regexp('user_name','\w','i')}} 

will compile to this is postgres

.. code-block:: SQL

   SELECT 
        'user_name' ~* '\w'

and this in snowflake

.. code-block:: SQL

   SELECT 
        RLIKE('user_name','\w','i')

This is actually the *preferred* method for mananging DSL differences between source and target. While contributions to the SnowShu emulation function libraries are awesome, `xdb macros <https://github.com/Health-Union/dbt-xdb/tree/master/xdb/macros>`__ better support our `philosophical goal <philosophy_statement.html>`__ of encouraging portable transform software. 

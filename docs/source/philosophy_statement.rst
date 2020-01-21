.. raw:: html

   <style type="text/css">
     .bolditalic{
       font-weight:bold;
       font-style:italic;
     }
   </style>

.. role:: bolditalic
  :class: bolditalic

.. image:: ../assets/snowshu_logo.png 

Philosophy Statement
====================

:bolditalic:`What this philosophy statement is (and is not):` 
With any undertaking it helps to establish guidelines outlining what we hope to accomplish. In addition, it can be of great use to enumerate the underlying beliefs and motivations that continue to direct our decisions along the way. This philosophy statement represents our attempt to cement those aforementioned reasons, motivations and beliefs. 

This philosophy statement is meant to serve as a bellwether for the development of SnowShu. It is a living document; as our understanding of the project and our beliefs about data development evolve, so will this statement. This is intended not as a manifesto demanding compliance, but a map to help aid in the journey. 

:bolditalic:`A note on terminology:` We use the phrase “source” in this philosophy statement to represent the data warehouse or data lake SnowShu is extracting a sample from. This is not to be confused with the originating source of the data, such as an application database or data provider.

Our Beliefs
============

We believe that data development is a subcategory of software engineering.
--------------------------------------------------------------------------
Data Engineering, Data Science, & Data Analytics are all disciplines that practice data development. Models, transforms and visualizations are all types of data software and are the product of data development. While data development has nuances that differ from sibling disciplines such as application development or firmware development, the universal best practices of software engineering very much apply to data. 

We believe in agile test driven development.
--------------------------------------------
SnowShu exists to make rapid TDD possible when practicing data development. Regardless of what programming language, framework or dialect you utilize, we believe the path to rapid development of reliable & performant software is through agile TDD. 

We believe data software should be deterministic in design.
-----------------------------------------------------------
Reports, dashboards, ML models and data warehouse tables should be viewed as disposable assets - similar to a web page rendered in a browser. SnowShu is focused on replicating “raw” source data as an input for data software. It is important that SnowShu encourages idempotent, deterministic architecture to foster highly resilient, self-healing software. 

We believe data software testing != data quality auditing.
----------------------------------------------------------
Data software tests exist to make rapid data development possible - their purpose is to tell us if the software is behaving as we expect it to. Data software tests must never be compromised in an attempt to leverage them for data quality auditing (aka detecting issues with source data). 

We believe it is better to get 95% of the way done very quickly. 
----------------------------------------------------------------
SnowShu is not intended to completely replace population testing. It is designed to help you get your data software to a point where you are pretty darn confident it is behaving as desired, in a fraction of the time it would have taken developing against the population dataset. That last 5% testing against the population should then be a relatively simple journey from pretty darn sure to completely certain, and can often happen as part of CI.

We believe platform-agnostic behavior should be encouraged. 
-----------------------------------------------------------
No data warehouse is completely ANSII SQL compliant. Writing data software that depends on vendor-specific features, functions and interfaces is often the right solution from a delivery standpoint, but subjects the codebase to vendor lock in. *We will strive in all ways to create as close an emulation for every supported source adapter as possible, but will never feel bad when a non-ANSII feature falls short.* This hardship will, in the end, encourage data developers to write more portable (and in our opinion better) code. 


Our Goals
==========

SnowShu makes data development much, much faster.
-------------------------------------------------
The most important guiding objective for SnowShu is how it impacts data development. First and foremost, we want SnowShu to enable data teams to deliver features at speeds that seem ludicrous with traditional development workflows. Every decision we make should be guided by the question “how much faster does this make data development?” Remember that overall speed includes the cost of refactoring, readability, and complexity, so our true measure of “fastness” is how SnowShu helps us get bug-free, easy-to-modify code into place. 

SnowShu replicates source dialect as closely as possible.
---------------------------------------------------------
We want SnowShu replicas to behave as closely to their respective source system as possible. So if you are developing in a SnowShu replica based on a Snowflake data warehouse, the same query should return the same result when run in either the replica or the same data set in Snowflake. This parity is not always 100% possible but is always our goal. 

SnowShu makes development collaboration easier.
-----------------------------------------------
By enabling teams to share manageable, immutable datasets, SnowShu drastically improves how data developers can work together. Data software applied to identical source data applied to software with the same behavior will always yield identical output. This removes source volatility and allows developers to speak the same language across disciplines. 

SnowShu remarkably simplifies data devops.
------------------------------------------
Data developers should spend as little time as possible negotiating with environments. SnowShu aims to make developer spin-up and environment maintenance totally scriptable and automated, adjustable with a handful of simple commands, and flexible enough to pivot as needed. 

SnowShu is for everyone.
------------------------
The power of SQL lies in the universal nature of the language. Regardless of which underlying SQL engine your population source operates within, SnowShu will strive to support your data development efforts. SnowShu aims to be accessible, favoring intuitive design over specialized technical configuration. We want SnowShu to be powerful enough to support very complex workflows but simple enough to bootstrap data development with no learning curve. 


What SnowShu is not
====================

SnowShu is not an ETL / ELT tool.
---------------------------------
For any new entry to the data software landscape It seems to be only a matter of time before someone attempts to bastardize it into jury-rigged extract / transform / load operations. To help avoid this, we will explicitly state here that *SnowShu is not intended to perform any part of the ETL/ELT process.* Amazing open-source tools already exist, such as Singer for extract/load and DBT for transform management, and we encourage you to check them out. What SnowShu does do is enable rapid testing within these tools! 

SnowShu does not replicate code.
--------------------------------
Data software belongs in a central codebase. Any code that influences, transforms or processes data is inherently part of the data software. To this end SnowShu does not replicate database functions, stored procedures etc as they persist “hidden business logic” that should be avoided at all costs.



.. _it_only_takes_one.yml:

=================
It Only Takes One
=================
*The blessing, and curse, of dataset development*

One of the base challenges in programming is that computers think in absolutes.
When we define a transforming condition to be applied to a dataset, that transforming condition *must be successful*
for every last memeber of the dataset or the program will fail. For example:

.. code-block:: SQL

   SELECT user_identifier::int FROM ONE_TRILLION_ROW_TABLE;

If ``1e12 - 1`` (all but one) rows in this table are numeric, and a *single row* has an alpha character jammed in it, the entire query will fail. 
When we use sampling to develop we both take advantage by and are hindered by this reality. 

We Only Need One To Be Right! 
=============================
When we sample a billion rows and 500 million have a specific condition we need to model for, *we only need one of those rows to make our code correct for that condition*. 
This means in a highly homogenious dataset we can reasonably do most of our development with a tiny fraction of the population. 
 
But We Also Only Need One To Be Wrong. 
======================================
When we sample a billion rows and only 1 has a specific condition we need to model for, *we are highly likely to miss the code needed for that condition*. 
This means in a highly heterogenious dataset we can reasonably expect to need more than one sampled pass to get to "done".

What Does This Mean For Data Development?
=========================================
Maturing your data development stack means tweaking your sampling strategy over time.
At first it is reasonable to have several iterations of code that pass tests locally but fail against the population;
the key is to learn from these passes and refine how you select records for development. Even if your SnowShu ⟳ CI cycle has more than a few 
iterations, imagine how long it would take developing directly against the population. 
